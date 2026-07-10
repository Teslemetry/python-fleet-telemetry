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


class _Message(Protocol):
    """The subset of an :mod:`aiohttp` ``WSMessage`` the loop reads per frame."""

    @property
    def type(self) -> WSMsgType: ...
    @property
    def data(self) -> Any: ...


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
        shutdown_timeout: float = 5.0,
    ) -> None:
        if ssl_context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError(
                "ssl_context must set verify_mode = ssl.CERT_REQUIRED for mTLS"
            )
        self._ssl_context = ssl_context
        self._host = host
        self._port = port
        self._shutdown_timeout = shutdown_timeout
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
        self._runner = web.AppRunner(self._app, shutdown_timeout=self._shutdown_timeout)
        await self._runner.setup()
        site = web.TCPSite(
            self._runner, self._host, self._port, ssl_context=self._ssl_context
        )
        await site.start()

    async def stop(self) -> None:
        """Stop serving, end all record iterators, and release the socket.

        The runner is cleaned up *first* so that each session's ``finally``
        runs its disconnect dispatch while the dispatcher is still open —
        reaching both listeners and ``records()`` iterators consistently. The
        dispatcher is closed only afterward, to unblock any remaining parked
        ``records()`` consumers. ``_closed`` is set up front so a connection
        arriving during the cleanup window is rejected rather than registered.
        """
        self._closed = True
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        self._dispatcher.close()

    async def __aenter__(self) -> TelemetryServer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # ---- request handling ---------------------------------------------- #

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        """Terminate one vehicle's WebSocket session end to end."""
        if self._closed:
            return web.Response(status=503, text="server is shutting down")

        vin = self._authorize(request)
        if isinstance(vin, web.Response):
            return vin

        # max_msg_size makes SIZE_LIMIT authoritative: aiohttp otherwise closes
        # frames above its 4MB default before our own check would run.
        ws = web.WebSocketResponse(max_msg_size=SIZE_LIMIT)
        await ws.prepare(request)

        peer = request.remote or ""
        client_version = request.headers.get("Version")
        conn = await self._connect(vin, peer=peer, client_version=client_version)
        try:
            async for msg in ws:
                await self._handle_message(ws, vin, msg)
        finally:
            await self._disconnect(vin, conn)
        return ws

    async def _handle_message(
        self, ws: _AckSink, vin: str, msg: _Message
    ) -> None:
        """Process one inbound WebSocket message, skipping unusable ones.

        Non-binary frames are ignored, and a binary frame larger than
        :data:`SIZE_LIMIT` is dropped without tearing down the connection (a
        belt-and-braces guard alongside the socket's ``max_msg_size``). Any
        usable binary frame is handed to :meth:`_process_frame`.
        """
        if msg.type is not WSMsgType.BINARY:
            return
        data: bytes = msg.data
        if len(data) > SIZE_LIMIT:
            _LOGGER.warning(
                "dropping oversized frame (%d bytes) from %s", len(data), vin
            )
            return
        await self._process_frame(ws, vin, data)

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
        except ConnectionError:
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
    ) -> Connection:
        """Register a connection and dispatch a synthetic CONNECTED record.

        Returns the :class:`Connection` instance stored in the registry; the
        caller holds it as a per-session token to pass to :meth:`_disconnect`,
        so a later session for the same VIN is never torn down by an earlier
        session's cleanup.
        """
        conn = Connection(
            vin=vin,
            connected_at=datetime.now(timezone.utc),
            peer=peer,
            client_version=client_version,
        )
        self._connections[vin] = conn
        await self._dispatcher.dispatch(
            self._connectivity_record(vin, vc.ConnectivityEvent.CONNECTED)
        )
        return conn

    async def _disconnect(self, vin: str, conn: Connection) -> None:
        """Deregister ``conn`` and dispatch a synthetic DISCONNECTED record.

        Only acts if ``conn`` is still the live registry entry for ``vin``. If a
        newer session has already replaced it (a same-VIN reconnect), this is a
        no-op: neither the newer connection is removed nor a spurious
        DISCONNECTED emitted.
        """
        if self._connections.get(vin) is not conn:
            return
        del self._connections[vin]
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
