"""The public telemetry server: mTLS termination and per-frame dispatch.

:class:`TelemetryServer` is the top of the stack the rest of the library builds
toward. It runs an :mod:`aiohttp` WebSocket server that terminates mutual TLS
in-process, derives each connection's VIN from the verified client certificate,
and for every frame decodes the transport envelope, acks immediately, parses the
record, and hands it to an internal
:class:`~fleet_telemetry.dispatch.Dispatcher`. It also tracks live connections
and synthesizes CONNECTIVITY records as vehicles connect and disconnect.

This module only composes the already-built layers (``_envelope``, ``records``,
``identity``, ``dispatch``); it adds the network transport and connection
lifecycle around them.
"""

from __future__ import annotations

import logging
import ssl
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from aiohttp import WSMsgType, web
from cryptography import x509
from google.protobuf.message import DecodeError

from fleet_telemetry import _envelope
from fleet_telemetry.dispatch import Dispatcher
from fleet_telemetry.identity import UnauthorizedCertificate, identity_from_cert
from fleet_telemetry.proto import vehicle_connectivity_pb2 as vc
from fleet_telemetry.records import Record, Topic, parse_record

if TYPE_CHECKING:
    from fleet_telemetry.dispatch import Listener
    from fleet_telemetry.dispatch import (
        _RecordStream,  # pyright: ignore[reportPrivateUsage]
    )

__all__ = ["Connection", "TelemetryServer"]

_LOGGER = logging.getLogger(__name__)

#: Reject frames larger than this many bytes before decoding them (Go parity).
SIZE_LIMIT = 1_000_000


class _AckSink(Protocol):
    """The one thing :meth:`TelemetryServer._process_frame` needs of a socket."""

    async def send_bytes(self, data: bytes) -> None: ...


@dataclass(frozen=True, slots=True)
class Connection:
    """A live vehicle connection tracked by the server registry."""

    vin: str
    connected_at: datetime
    peer: str
    client_version: str | None


class TelemetryServer:
    """An mTLS WebSocket telemetry server with a live connection registry.

    :param ssl_context: A server-side context configured for mutual TLS
        (``verify_mode = ssl.CERT_REQUIRED`` and a CA that signs the client
        certificates). The server reads the verified peer certificate off each
        connection to derive its VIN.
    :param host: Interface to bind.
    :param port: Port to bind.
    :param queue_maxsize: Backlog bound for each :meth:`records` iterator.

    Register listeners with :meth:`add_listener` / :meth:`on_data` / … or pull
    records with :meth:`records`. Run it with ``async with server:`` (or
    :meth:`start` / :meth:`stop`).
    """

    def __init__(
        self,
        *,
        ssl_context: ssl.SSLContext,
        host: str = "0.0.0.0",
        port: int = 443,
        queue_maxsize: int = 1000,
    ) -> None:
        self._ssl_context = ssl_context
        self._host = host
        self._port = port
        self._dispatcher = Dispatcher(queue_maxsize=queue_maxsize)
        self._connections: dict[str, Connection] = {}
        self._runner: web.AppRunner | None = None
        self._closed = False

        self._app = web.Application()
        self._app.router.add_get("/", self._handle)

    # ---- listener delegation ------------------------------------------- #

    def add_listener(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        topic: Topic | Iterable[Topic] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for records matching the given filters."""
        return self._dispatcher.add_listener(
            callback, vin=vin, topic=topic, field=field
        )

    def on_data(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for DATA records."""
        return self._dispatcher.on_data(callback, vin=vin, field=field)

    def on_alert(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for ALERTS records."""
        return self._dispatcher.on_alert(callback, vin=vin, field=field)

    def on_error(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for ERRORS records."""
        return self._dispatcher.on_error(callback, vin=vin, field=field)

    def on_connectivity(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for CONNECTIVITY records."""
        return self._dispatcher.on_connectivity(callback, vin=vin, field=field)

    def records(self) -> _RecordStream:
        """Return an async iterator that yields records as they are dispatched."""
        return self._dispatcher.records()

    # ---- connection state ---------------------------------------------- #

    @property
    def connections(self) -> dict[str, Connection]:
        """A snapshot copy of the live connection registry, keyed by VIN."""
        return dict(self._connections)

    def is_connected(self, vin: str) -> bool:
        """Return whether a vehicle with ``vin`` is currently connected."""
        return vin in self._connections

    # ---- lifecycle ----------------------------------------------------- #

    async def start(self) -> None:
        """Bind the listening socket and begin serving."""
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(
            self._runner, self._host, self._port, ssl_context=self._ssl_context
        )
        await site.start()

    async def stop(self) -> None:
        """Stop serving, end all record iterators, and release the socket."""
        self._closed = True
        self._dispatcher.close()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def __aenter__(self) -> TelemetryServer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # ---- request handling ---------------------------------------------- #

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        """Terminate one vehicle's WebSocket session end to end."""
        vin = self._authorize(request)
        if isinstance(vin, web.Response):
            return vin

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        peer = request.remote or ""
        client_version = request.headers.get("Version")
        await self._connect(vin, peer=peer, client_version=client_version)
        try:
            async for msg in ws:
                if msg.type is not WSMsgType.BINARY:
                    continue
                data: bytes = msg.data
                if len(data) > SIZE_LIMIT:
                    _LOGGER.warning(
                        "dropping oversized frame (%d bytes) from %s", len(data), vin
                    )
                    continue
                await self._process_frame(ws, vin, data)
        finally:
            await self._disconnect(vin)
        return ws

    def _authorize(self, request: web.Request) -> str | web.Response:
        """Derive the VIN from the verified peer cert, or a 496 rejection.

        Returns the authorized VIN string, or a ``web.Response`` to return
        instead of upgrading to a WebSocket.
        """
        transport = request.transport
        if transport is None:
            return web.Response(status=496, text="no transport")
        ssl_object: Any = transport.get_extra_info("ssl_object")
        if ssl_object is None:
            return web.Response(status=496, text="no TLS session")
        der: bytes | None = ssl_object.getpeercert(binary_form=True)
        if not der:
            return web.Response(status=496, text="no client certificate")

        cert = x509.load_der_x509_certificate(der)
        try:
            identity = identity_from_cert(cert)
        except UnauthorizedCertificate as exc:
            return web.Response(status=496, text=str(exc))
        if identity.vin == "":
            return web.Response(status=496, text="client certificate has no VIN")
        return identity.vin

    async def _process_frame(self, ws: _AckSink, vin: str, data: bytes) -> None:
        """Decode, ack, parse, and dispatch a single binary frame.

        The ack is sent immediately after a successful decode and *before*
        parsing or dispatch (reliable_ack=false semantics): the vehicle is told
        the frame was received as soon as the envelope is understood, decoupling
        acknowledgement from downstream processing.
        """
        try:
            frame = _envelope.decode(data)
        except _envelope.EnvelopeError:
            _LOGGER.warning("dropping undecodable frame from %s", vin)
            return

        ack = _envelope.encode_ack(
            txid=frame.txid, topic=frame.topic, message_id=frame.message_id
        )
        try:
            await ws.send_bytes(ack)
        except (ConnectionError, ConnectionResetError):
            return

        try:
            record = parse_record(
                vin=vin,
                topic=frame.topic,
                txid=frame.txid,
                created_at=frame.created_at,
                payload=frame.payload,
            )
        except (ValueError, DecodeError, OSError, OverflowError):
            _LOGGER.exception("failed to parse record from %s", vin)
            return

        await self._dispatcher.dispatch(record)

    # ---- connectivity synthesis ---------------------------------------- #

    async def _connect(
        self, vin: str, *, peer: str, client_version: str | None
    ) -> None:
        """Register a connection and dispatch a synthetic CONNECTED record."""
        self._connections[vin] = Connection(
            vin=vin,
            connected_at=datetime.now(timezone.utc),
            peer=peer,
            client_version=client_version,
        )
        await self._dispatcher.dispatch(
            self._connectivity_record(vin, vc.ConnectivityEvent.CONNECTED)
        )

    async def _disconnect(self, vin: str) -> None:
        """Deregister a connection and dispatch a synthetic DISCONNECTED record."""
        self._connections.pop(vin, None)
        await self._dispatcher.dispatch(
            self._connectivity_record(vin, vc.ConnectivityEvent.DISCONNECTED)
        )

    @staticmethod
    def _connectivity_record(
        vin: str, status: vc.ConnectivityEvent
    ) -> Record:
        """Build a synthetic CONNECTIVITY record for ``vin`` with ``status``."""
        message = vc.VehicleConnectivity(vin=vin, status=status)
        return Record(
            vin=vin,
            topic=Topic.CONNECTIVITY,
            created_at=datetime.now(timezone.utc),
            txid="",
            message=message,
            raw=b"",
        )
