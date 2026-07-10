"""Tests for the server certificate tooling (:mod:`fleet_telemetry.certs`).

Fast unit tests cover get-or-create/idempotency, the structural properties of
the generated root CA and server leaf, CA -> leaf chain verification, IP-literal
SAN handling, rotation, and the server ``SSLContext`` shape. One end-to-end test
stands up a real ``TelemetryServer`` presenting the *generated* leaf and drives a
real mTLS handshake from an ``aiohttp`` client that trusts the generated root,
proving the CA -> leaf chain is accepted by a real TLS peer.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import socket
import ssl
import stat
from pathlib import Path

import aiohttp
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from fleet_telemetry import ServerCredentials, tesla_ca_bundle_path
from fleet_telemetry import _envelope
from fleet_telemetry.records import Record, Topic
from fleet_telemetry.server import TelemetryServer
from tests.fixtures.golden import GOLDEN_STREAM

VIN = "5YJ3E1EA7JF000001"
FQDN = "telemetry.example.com"


def _load_cert(path: Path) -> x509.Certificate:
    return x509.load_pem_x509_certificate(path.read_bytes())


# --------------------------------------------------------------------------- #
# ensure(): creation, permissions, idempotency
# --------------------------------------------------------------------------- #


def test_ensure_creates_all_four_files_with_0600_keys(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)

    for name in ("ca.pem", "ca.key", "server.pem", "server.key"):
        assert (tmp_path / name).exists(), name

    for key_name in ("ca.key", "server.key"):
        mode = stat.S_IMODE((tmp_path / key_name).stat().st_mode)
        assert mode == 0o600, f"{key_name} mode was {oct(mode)}"


def test_ensure_is_idempotent(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)

    ca_key = (tmp_path / "ca.key").read_bytes()
    server_key = (tmp_path / "server.key").read_bytes()
    ca_pem = (tmp_path / "ca.pem").read_bytes()
    server_pem = (tmp_path / "server.pem").read_bytes()

    creds.ensure(FQDN)

    assert (tmp_path / "ca.key").read_bytes() == ca_key
    assert (tmp_path / "server.key").read_bytes() == server_key
    assert (tmp_path / "ca.pem").read_bytes() == ca_pem
    assert (tmp_path / "server.pem").read_bytes() == server_pem


def test_ensure_reissues_leaf_when_fqdn_not_covered(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    ca_key = (tmp_path / "ca.key").read_bytes()
    server_pem = (tmp_path / "server.pem").read_bytes()

    # A different FQDN not covered by the existing leaf's SAN forces a re-issue
    # of the leaf, but must leave the CA untouched.
    creds.ensure("other.example.com")

    assert (tmp_path / "ca.key").read_bytes() == ca_key
    assert (tmp_path / "server.pem").read_bytes() != server_pem
    leaf = _load_cert(tmp_path / "server.pem")
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert "other.example.com" in san.get_values_for_type(x509.DNSName)


def test_ensure_creates_directory_mode_0700(tmp_path: Path) -> None:
    target = tmp_path / "creds"  # does not exist yet
    creds = ServerCredentials(target)
    creds.ensure(FQDN)
    mode = stat.S_IMODE(target.stat().st_mode)
    assert mode == 0o700, f"dir mode was {oct(mode)}"


def test_ensure_tightens_preexisting_loose_directory(tmp_path: Path) -> None:
    target = tmp_path / "creds"
    target.mkdir(mode=0o755)
    ServerCredentials(target).ensure(FQDN)
    assert stat.S_IMODE(target.stat().st_mode) == 0o700


# --------------------------------------------------------------------------- #
# Partial-CA-state guard + CA-regeneration leaf coherence
# --------------------------------------------------------------------------- #


def test_ensure_raises_on_partial_ca_state_missing_key(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    (tmp_path / "ca.key").unlink()

    # Regenerating the trust anchor would silently invalidate every vehicle's
    # registered `ca`; a half-present CA must raise rather than self-heal.
    with pytest.raises(FileNotFoundError, match="partial CA state"):
        creds.ensure(FQDN)


def test_ensure_raises_on_partial_ca_state_missing_cert(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    (tmp_path / "ca.pem").unlink()

    with pytest.raises(FileNotFoundError, match="partial CA state"):
        creds.ensure(FQDN)


def test_fresh_dir_ensure_leaf_chains_to_ca(tmp_path: Path) -> None:
    # A completely fresh directory (both CA files absent) auto-creates a CA and
    # a leaf that cryptographically chains to it.
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    ca = _load_cert(tmp_path / "ca.pem")
    leaf = _load_cert(tmp_path / "server.pem")
    ca_pubkey = ca.public_key()
    assert isinstance(ca_pubkey, ec.EllipticCurvePublicKey)
    assert leaf.signature_hash_algorithm is not None
    ca_pubkey.verify(
        leaf.signature,
        leaf.tbs_certificate_bytes,
        ec.ECDSA(leaf.signature_hash_algorithm),
    )


def test_full_dir_wipe_reissues_coherent_chain(tmp_path: Path) -> None:
    # The sanctioned way to regenerate: delete BOTH CA files (a fresh dir). The
    # leaf must then be reissued so server.pem still chains to the new ca.pem,
    # even though the old leaf's SAN already covered the fqdn.
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    (tmp_path / "ca.pem").unlink()
    (tmp_path / "ca.key").unlink()

    creds.ensure(FQDN)  # same fqdn — old leaf SAN would still "cover" it

    ca = _load_cert(tmp_path / "ca.pem")
    leaf = _load_cert(tmp_path / "server.pem")
    assert leaf.issuer == ca.subject
    ca_pubkey = ca.public_key()
    assert isinstance(ca_pubkey, ec.EllipticCurvePublicKey)
    assert leaf.signature_hash_algorithm is not None
    ca_pubkey.verify(
        leaf.signature,
        leaf.tbs_certificate_bytes,
        ec.ECDSA(leaf.signature_hash_algorithm),
    )


# --------------------------------------------------------------------------- #
# FQDN length / emptiness handling
# --------------------------------------------------------------------------- #


def test_long_fqdn_issues_leaf_via_san_without_cn(tmp_path: Path) -> None:
    long_fqdn = "a" * 60 + ".example.com"  # 72 chars, > 64 CN cap
    assert len(long_fqdn) == 72
    creds = ServerCredentials(tmp_path)
    creds.ensure(long_fqdn)  # must not raise

    leaf = _load_cert(tmp_path / "server.pem")
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert long_fqdn in san.get_values_for_type(x509.DNSName)
    # CN omitted because it exceeds 64 chars.
    assert leaf.subject.get_attributes_for_oid(NameOID.COMMON_NAME) == []


def test_short_fqdn_still_sets_cn(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    leaf = _load_cert(tmp_path / "server.pem")
    cn = leaf.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    assert cn and cn[0].value == FQDN


def test_empty_fqdn_raises_clear_error(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    with pytest.raises(ValueError, match="non-empty"):
        creds.ensure("")


# --------------------------------------------------------------------------- #
# Structural properties + chain verification
# --------------------------------------------------------------------------- #


def test_ca_is_a_ca(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    ca = _load_cert(tmp_path / "ca.pem")

    bc = ca.extensions.get_extension_for_class(x509.BasicConstraints).value
    assert bc.ca is True
    ku = ca.extensions.get_extension_for_class(x509.KeyUsage).value
    assert ku.key_cert_sign is True
    assert ku.crl_sign is True
    # SubjectKeyIdentifier is present, and the AKI mirrors it (RFC convention).
    ski = ca.extensions.get_extension_for_class(x509.SubjectKeyIdentifier).value
    aki = ca.extensions.get_extension_for_class(x509.AuthorityKeyIdentifier).value
    assert aki.key_identifier == ski.key_identifier


def test_leaf_structure_and_san(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    leaf = _load_cert(tmp_path / "server.pem")

    bc = leaf.extensions.get_extension_for_class(x509.BasicConstraints).value
    assert bc.ca is False
    ku = leaf.extensions.get_extension_for_class(x509.KeyUsage).value
    assert ku.digital_signature is True
    # EC leaf over ECDHE needs no key_encipherment; it must be off (inert/inaccurate).
    assert ku.key_encipherment is False
    eku = leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    assert ExtendedKeyUsageOID.SERVER_AUTH in eku
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert FQDN in san.get_values_for_type(x509.DNSName)


def test_leaf_chain_verifies_against_ca(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    ca = _load_cert(tmp_path / "ca.pem")
    leaf = _load_cert(tmp_path / "server.pem")

    assert leaf.issuer == ca.subject
    ca_pubkey = ca.public_key()
    assert isinstance(ca_pubkey, ec.EllipticCurvePublicKey)
    assert leaf.signature_hash_algorithm is not None
    # Raises on failure; a clean return proves the leaf was signed by the CA.
    ca_pubkey.verify(
        leaf.signature,
        leaf.tbs_certificate_bytes,
        ec.ECDSA(leaf.signature_hash_algorithm),
    )


# --------------------------------------------------------------------------- #
# ca_certificate_pem
# --------------------------------------------------------------------------- #


def test_ca_certificate_pem_parses_to_ca(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)

    pem = creds.ca_certificate_pem
    parsed = x509.load_pem_x509_certificate(pem.encode())
    assert parsed == _load_cert(tmp_path / "ca.pem")


def test_ca_certificate_pem_raises_before_ensure(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    with pytest.raises(FileNotFoundError):
        _ = creds.ca_certificate_pem


# --------------------------------------------------------------------------- #
# rotate_server_cert
# --------------------------------------------------------------------------- #


def test_rotate_server_cert_changes_leaf_keeps_ca(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)

    ca_key = (tmp_path / "ca.key").read_bytes()
    ca_pem = (tmp_path / "ca.pem").read_bytes()
    server_key = (tmp_path / "server.key").read_bytes()
    server_pem = (tmp_path / "server.pem").read_bytes()

    creds.rotate_server_cert(FQDN)

    assert (tmp_path / "ca.key").read_bytes() == ca_key
    assert (tmp_path / "ca.pem").read_bytes() == ca_pem
    assert (tmp_path / "server.key").read_bytes() != server_key
    assert (tmp_path / "server.pem").read_bytes() != server_pem
    # New leaf still chains to the unchanged CA.
    assert _load_cert(tmp_path / "server.pem").issuer == _load_cert(tmp_path / "ca.pem").subject


def test_rotate_server_cert_raises_without_ca(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    with pytest.raises(FileNotFoundError):
        creds.rotate_server_cert(FQDN)


# --------------------------------------------------------------------------- #
# IP SAN handling
# --------------------------------------------------------------------------- #


def test_ensure_ip_literal_emits_ipaddress_san(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure("127.0.0.1")
    leaf = _load_cert(tmp_path / "server.pem")
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value

    ips = san.get_values_for_type(x509.IPAddress)
    assert ipaddress.ip_address("127.0.0.1") in ips
    assert san.get_values_for_type(x509.DNSName) == []


def test_ensure_fqdn_plus_ip_addresses(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure("host.example.com", ip_addresses=["10.0.0.5"])
    leaf = _load_cert(tmp_path / "server.pem")
    san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value

    assert "host.example.com" in san.get_values_for_type(x509.DNSName)
    assert ipaddress.ip_address("10.0.0.5") in san.get_values_for_type(x509.IPAddress)


# --------------------------------------------------------------------------- #
# build_ssl_context + tesla_ca_bundle_path
# --------------------------------------------------------------------------- #


def test_tesla_ca_bundle_path_exists() -> None:
    assert tesla_ca_bundle_path().exists()
    assert tesla_ca_bundle_path(staging=True).exists()
    assert tesla_ca_bundle_path() != tesla_ca_bundle_path(staging=True)


def test_build_ssl_context_is_cert_required_and_trusts_tesla(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    ctx = creds.build_ssl_context()

    assert ctx.verify_mode == ssl.CERT_REQUIRED
    # The server context satisfies TelemetryServer's CERT_REQUIRED contract.
    TelemetryServer(ssl_context=ctx)
    # Its trust store includes a Tesla CA (prod bundle).
    assert any("Tesla" in str(c.get("subject", ())) for c in ctx.get_ca_certs())


def test_build_ssl_context_staging_loads_eng_bundle(tmp_path: Path) -> None:
    creds = ServerCredentials(tmp_path)
    creds.ensure(FQDN)
    ctx = creds.build_ssl_context(staging=True)

    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert any("Tesla" in str(c.get("subject", ())) for c in ctx.get_ca_certs())


# --------------------------------------------------------------------------- #
# Real mTLS handshake with the generated CA -> leaf chain
# --------------------------------------------------------------------------- #

_NOT_BEFORE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
_NOT_AFTER = _NOT_BEFORE + datetime.timedelta(days=3650)


def _make_client_ca() -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    """A throwaway client-side CA whose CN authorizes vehicles via identity."""
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


def _make_client_cert(
    ca_key: ec.EllipticCurvePrivateKey, ca_cert: x509.Certificate
) -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, VIN)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOT_BEFORE)
        .not_valid_after(_NOT_AFTER)
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


async def test_real_handshake_with_generated_chain(tmp_path: Path) -> None:
    # Generate our real server CA + leaf (SAN covers localhost/127.0.0.1).
    creds = ServerCredentials(tmp_path)
    creds.ensure("localhost", ip_addresses=["127.0.0.1"])

    # A throwaway client CA + client cert (can't mint a real Tesla-signed one).
    client_ca_key, client_ca_cert = _make_client_ca()
    client_key, client_cert = _make_client_cert(client_ca_key, client_ca_cert)
    client_cert_path = tmp_path / "client.pem"
    client_key_path = tmp_path / "client.key"
    client_cert_path.write_bytes(client_cert.public_bytes(Encoding.PEM))
    client_key_path.write_bytes(
        client_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    )

    # Server context: present the GENERATED leaf, verify clients against the
    # throwaway client CA (stand-in for Tesla's real CA).
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(str(tmp_path / "server.pem"), str(tmp_path / "server.key"))
    server_ctx.load_verify_locations(
        cadata=client_ca_cert.public_bytes(Encoding.PEM).decode()
    )
    server_ctx.verify_mode = ssl.CERT_REQUIRED

    # Client context: trust the GENERATED root CA to verify the server leaf.
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.load_verify_locations(cadata=creds.ca_certificate_pem)
    client_ctx.load_cert_chain(str(client_cert_path), str(client_key_path))
    client_ctx.check_hostname = True

    port = _free_port()
    server = TelemetryServer(ssl_context=server_ctx, host="127.0.0.1", port=port)

    got_data = asyncio.Event()
    received: list[Record] = []

    def on_data(record: Record) -> None:
        received.append(record)
        got_data.set()

    server.on_data(on_data)

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

    assert len(received) == 1
    assert received[0].vin == VIN
    assert received[0].topic is Topic.DATA
