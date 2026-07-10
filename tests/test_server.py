"""Tests for the telemetry server (:mod:`fleet_telemetry.server`).

Split into fast, TLS-free unit tests that drive ``_process_frame`` and the
connectivity-synthesis helpers directly with a fake WebSocket, plus one
end-to-end integration test that performs a real mTLS handshake through an
``aiohttp`` client so the whole cert -> identity -> dispatch path is exercised.

These are white-box tests that deliberately reach into the server's internal
frame/connection helpers, so protected-member access is expected here.
"""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import socket
import ssl
from pathlib import Path
from typing import cast

import aiohttp
import pytest
from aiohttp import WSMsgType, web
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from cryptography.x509.oid import NameOID

from fleet_telemetry import _envelope
from fleet_telemetry.proto import vehicle_connectivity_pb2 as vc
from fleet_telemetry.records import Record, Topic
from fleet_telemetry.server import SIZE_LIMIT, Connection, TelemetryServer
from tests.fixtures.golden import GOLDEN_STREAM

VIN = "5YJ3E1EA7JF000001"


def _mtls_context() -> ssl.SSLContext:
    """A server-side context that satisfies the mTLS construction contract."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def _dummy_server() -> TelemetryServer:
    """A server built on a never-started server-side SSL context."""
    return TelemetryServer(ssl_context=_mtls_context(), host="127.0.0.1", port=0)


class FakeWS:
    """Minimal stand-in for a WebSocket response that records sent frames."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)


class FakeMsg:
    """Minimal stand-in for an aiohttp ``WSMessage``."""

    def __init__(self, msg_type: WSMsgType, data: object) -> None:
        self.type = msg_type
        self.data = data


class _FakeSSLObject:
    """Stub SSL object exposing a fixed peer certificate in DER form."""

    def __init__(self, der: bytes | None) -> None:
        self._der = der

    def getpeercert(self, binary_form: bool = False) -> bytes | None:
        return self._der


class _FakeTransport:
    """Stub transport returning a fixed ssl_object from get_extra_info."""

    def __init__(self, ssl_object: object) -> None:
        self._ssl_object = ssl_object

    def get_extra_info(self, name: str, default: object = None) -> object:
        return self._ssl_object if name == "ssl_object" else default


class _FakeRequest:
    """Stub request exposing only the transport ``_authorize`` reads."""

    def __init__(self, transport: object) -> None:
        self.transport = transport


def _fake_request(der: bytes | None, *, with_transport: bool = True) -> web.Request:
    ssl_object = _FakeSSLObject(der) if der is not None else None
    transport = _FakeTransport(ssl_object) if with_transport else None
    return cast(web.Request, _FakeRequest(transport))


def _self_signed_der(subject_cn: str, issuer_cn: str) -> bytes:
    """A self-signed cert (DER) with the given subject/issuer CNs, for _authorize."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = (
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject_cn)])
        if subject_cn
        else x509.Name([])
    )
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_cn)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc))
        .not_valid_after(datetime.datetime(2034, 1, 1, tzinfo=datetime.timezone.utc))
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(Encoding.DER)


# --------------------------------------------------------------------------- #
# Unit tests: construction contract
# --------------------------------------------------------------------------- #


def test_construction_requires_cert_required_context() -> None:
    # A default PROTOCOL_TLS_SERVER context is CERT_NONE: the mTLS auth model
    # would be silently defeated, so construction must fail loudly.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    assert ctx.verify_mode == ssl.CERT_NONE
    with pytest.raises(ValueError, match="CERT_REQUIRED"):
        TelemetryServer(ssl_context=ctx)


# --------------------------------------------------------------------------- #
# Unit tests: _process_frame
# --------------------------------------------------------------------------- #


async def test_process_frame_acks_and_dispatches_data() -> None:
    server = _dummy_server()
    got: list[Record] = []
    server.on_data(got.append)

    ws = FakeWS()
    await server._process_frame(ws, VIN, GOLDEN_STREAM)

    # Exactly one ack was sent and it decodes as a StreamAck for our txid.
    assert len(ws.sent) == 1
    assert ws.sent[0]
    ack = _envelope.decode_ack(ws.sent[0])
    assert ack.txid == b"txid-0001"

    # A DATA record was dispatched to the listener with the vin we passed.
    assert len(got) == 1
    assert got[0].vin == VIN
    assert got[0].topic is Topic.DATA


async def test_process_frame_malformed_no_ack_no_dispatch() -> None:
    server = _dummy_server()
    got: list[Record] = []
    server.add_listener(got.append)

    ws = FakeWS()
    # Must neither raise, ack, nor dispatch on an undecodable frame.
    await server._process_frame(ws, VIN, b"\xff\xff\xff\xff")

    assert ws.sent == []
    assert got == []


async def test_process_frame_send_failure_skips_dispatch() -> None:
    server = _dummy_server()
    got: list[Record] = []
    server.on_data(got.append)

    class BrokenWS:
        async def send_bytes(self, data: bytes) -> None:
            raise ConnectionResetError("peer gone")

    # A dropped connection during ack must be swallowed and stop processing.
    await server._process_frame(BrokenWS(), VIN, GOLDEN_STREAM)
    assert got == []


# --------------------------------------------------------------------------- #
# Unit tests: connectivity synthesis + registry
# --------------------------------------------------------------------------- #


async def test_connect_registers_and_synthesizes_connected() -> None:
    server = _dummy_server()
    events: list[Record] = []
    server.on_connectivity(events.append)

    assert not server.is_connected(VIN)

    await server._connect(VIN, peer="10.0.0.1", client_version="2025.1")

    assert server.is_connected(VIN)
    conn = server.connections[VIN]
    assert isinstance(conn, Connection)
    assert conn.vin == VIN
    assert conn.peer == "10.0.0.1"
    assert conn.client_version == "2025.1"

    assert len(events) == 1
    assert events[0].topic is Topic.CONNECTIVITY
    assert events[0].vin == VIN
    message = events[0].message
    assert isinstance(message, vc.VehicleConnectivity)
    assert message.status == vc.ConnectivityEvent.CONNECTED


async def test_disconnect_clears_registry_and_synthesizes_disconnected() -> None:
    server = _dummy_server()
    events: list[Record] = []
    server.on_connectivity(events.append)

    conn = await server._connect(VIN, peer="10.0.0.1", client_version=None)
    events.clear()

    await server._disconnect(VIN, conn)

    assert not server.is_connected(VIN)
    assert VIN not in server.connections
    assert len(events) == 1
    message = events[0].message
    assert isinstance(message, vc.VehicleConnectivity)
    assert message.status == vc.ConnectivityEvent.DISCONNECTED


async def test_connections_property_returns_a_copy() -> None:
    server = _dummy_server()
    await server._connect(VIN, peer="10.0.0.1", client_version=None)

    snapshot = server.connections
    snapshot.clear()

    # Mutating the returned dict must not affect the live registry.
    assert server.is_connected(VIN)


async def test_stop_closes_dispatcher_and_sets_closed() -> None:
    server = _dummy_server()
    # stop() without start() should still close the dispatcher cleanly.
    await server.stop()
    stream = server.records()
    # A records() iterator obtained after close is already exhausted.
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()


# --------------------------------------------------------------------------- #
# Unit tests: same-VIN reconnect race
# --------------------------------------------------------------------------- #


async def test_reconnect_race_keeps_live_session() -> None:
    server = _dummy_server()
    events: list[Record] = []
    server.on_connectivity(events.append)

    # A connects, then B connects for the same VIN (network-change reconnect),
    # overwriting A's registry entry.
    conn_a = await server._connect(VIN, peer="a", client_version=None)
    conn_b = await server._connect(VIN, peer="b", client_version=None)
    assert server.is_connected(VIN)
    assert server.connections[VIN] is conn_b
    events.clear()

    # A's socket now closes; its finally runs _disconnect with A's stale token.
    await server._disconnect(VIN, conn_a)
    # B is still live and no spurious DISCONNECTED was emitted.
    assert server.is_connected(VIN)
    assert server.connections[VIN] is conn_b
    assert events == []

    # Only when the live session (B) closes do we deregister and emit once.
    await server._disconnect(VIN, conn_b)
    assert not server.is_connected(VIN)
    assert len(events) == 1
    message = events[0].message
    assert isinstance(message, vc.VehicleConnectivity)
    assert message.status == vc.ConnectivityEvent.DISCONNECTED


# --------------------------------------------------------------------------- #
# Unit tests: message loop (skip rules)
# --------------------------------------------------------------------------- #


async def test_handle_message_skips_non_binary() -> None:
    server = _dummy_server()
    got: list[Record] = []
    server.add_listener(got.append)

    ws = FakeWS()
    await server._handle_message(ws, VIN, FakeMsg(WSMsgType.TEXT, "hello"))
    assert ws.sent == []
    assert got == []


async def test_handle_message_skips_oversized_frame() -> None:
    server = _dummy_server()
    got: list[Record] = []
    server.add_listener(got.append)

    ws = FakeWS()
    oversized = FakeMsg(WSMsgType.BINARY, b"x" * (SIZE_LIMIT + 1))
    # Skipped without dispatch; the connection is not torn down (no raise).
    await server._handle_message(ws, VIN, oversized)
    assert ws.sent == []
    assert got == []


async def test_handle_message_processes_binary() -> None:
    server = _dummy_server()
    got: list[Record] = []
    server.on_data(got.append)

    ws = FakeWS()
    await server._handle_message(ws, VIN, FakeMsg(WSMsgType.BINARY, GOLDEN_STREAM))
    assert len(ws.sent) == 1
    assert len(got) == 1


# --------------------------------------------------------------------------- #
# Unit tests: authorization / rejection gate
# --------------------------------------------------------------------------- #


def _reject_status(server: TelemetryServer, request: web.Request) -> int:
    result = server._authorize(request)
    assert isinstance(result, web.Response)
    return result.status


def test_authorize_accepts_known_issuer() -> None:
    server = _dummy_server()
    der = _self_signed_der(VIN, "Tesla Issuing CA")
    assert server._authorize(_fake_request(der)) == VIN


def test_authorize_rejects_unauthorized_issuer() -> None:
    server = _dummy_server()
    der = _self_signed_der(VIN, "Some Random CA")
    assert _reject_status(server, _fake_request(der)) == 496


def test_authorize_rejects_blank_vin() -> None:
    server = _dummy_server()
    der = _self_signed_der("", "Tesla Issuing CA")
    assert _reject_status(server, _fake_request(der)) == 496


def test_authorize_rejects_missing_transport() -> None:
    server = _dummy_server()
    request = _fake_request(None, with_transport=False)
    assert _reject_status(server, request) == 496


def test_authorize_rejects_missing_ssl_object() -> None:
    server = _dummy_server()
    request = _fake_request(None)
    assert _reject_status(server, request) == 496


def test_authorize_rejects_empty_der() -> None:
    server = _dummy_server()
    request = _fake_request(b"")
    assert _reject_status(server, request) == 496


# --------------------------------------------------------------------------- #
# Integration test: real mTLS handshake
# --------------------------------------------------------------------------- #

_NOT_BEFORE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
_NOT_AFTER = _NOT_BEFORE + datetime.timedelta(days=3650)


def _make_ca() -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Tesla Issuing CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOT_BEFORE)
        .not_valid_after(_NOT_AFTER)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _make_signed(
    subject_cn: str,
    ca_key: ec.EllipticCurvePrivateKey,
    ca_cert: x509.Certificate,
    sans: list[x509.GeneralName] | None = None,
) -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    builder = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject_cn)])
        )
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOT_BEFORE)
        .not_valid_after(_NOT_AFTER)
    )
    if sans is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(sans), critical=False
        )
    cert = builder.sign(ca_key, hashes.SHA256())
    return key, cert


def _write_cert(path: Path, cert: x509.Certificate) -> str:
    path.write_bytes(cert.public_bytes(Encoding.PEM))
    return str(path)


def _write_key(path: Path, key: ec.EllipticCurvePrivateKey) -> str:
    path.write_bytes(
        key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    )
    return str(path)


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


async def test_real_mtls_roundtrip(tmp_path: Path) -> None:
    ca_key, ca_cert = _make_ca()
    server_key, server_cert = _make_signed(
        "localhost",
        ca_key,
        ca_cert,
        sans=[
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
        ],
    )
    client_key, client_cert = _make_signed(VIN, ca_key, ca_cert)

    ca_path = _write_cert(tmp_path / "ca.pem", ca_cert)
    server_cert_path = _write_cert(tmp_path / "server.pem", server_cert)
    server_key_path = _write_key(tmp_path / "server.key", server_key)
    client_cert_path = _write_cert(tmp_path / "client.pem", client_cert)
    client_key_path = _write_key(tmp_path / "client.key", client_key)

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(server_cert_path, server_key_path)
    server_ctx.load_verify_locations(ca_path)
    server_ctx.verify_mode = ssl.CERT_REQUIRED

    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.load_verify_locations(ca_path)
    client_ctx.load_cert_chain(client_cert_path, client_key_path)
    client_ctx.check_hostname = True

    port = _free_port()
    server = TelemetryServer(ssl_context=server_ctx, host="127.0.0.1", port=port)

    got_data = asyncio.Event()
    received: list[Record] = []

    def on_data(record: Record) -> None:
        received.append(record)
        got_data.set()

    server.on_data(on_data)

    connected_during_session = False
    async with server:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                f"https://localhost:{port}/", ssl=client_ctx
            ) as ws:
                await asyncio.wait_for(ws.send_bytes(GOLDEN_STREAM), timeout=5)
                msg = await asyncio.wait_for(ws.receive(), timeout=5)
                assert msg.type is aiohttp.WSMsgType.BINARY
                ack = _envelope.decode_ack(msg.data)
                assert ack.txid == b"txid-0001"

                await asyncio.wait_for(got_data.wait(), timeout=5)
                connected_during_session = server.is_connected(VIN)

    assert connected_during_session is True
    assert len(received) == 1
    assert received[0].vin == VIN
    assert received[0].topic is Topic.DATA
    # After teardown the connection is deregistered.
    assert not server.is_connected(VIN)
