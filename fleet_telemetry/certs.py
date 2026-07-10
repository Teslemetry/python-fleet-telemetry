"""Server-side certificate tooling for the telemetry mTLS trust model.

The telemetry server terminates mutual TLS, which has two independent trust
directions:

* **The vehicle verifies the server.** The server presents a TLS leaf certificate
  for its fully-qualified domain name. The vehicle trusts that leaf via a CA whose
  PEM the operator registers in ``fleet_telemetry_config.ca``. A vehicle rejects a
  bare self-signed root used directly as the server certificate, so this module
  generates a self-signed **root CA** (its public certificate is the value for the
  config ``ca`` field) and a **server leaf** signed by that CA (with the FQDN in
  its ``SubjectAltName``). The leaf can be rotated without re-pushing the config,
  because the CA the vehicle trusts stays constant.

* **The server verifies the vehicle.** The vehicle presents a client certificate
  issued by *Tesla's* CA (fixed, not generated here). :meth:`ServerCredentials.
  build_ssl_context` loads the vendored Tesla CA bundle as the trust anchor so the
  mTLS handshake completes; :mod:`fleet_telemetry.identity` then re-checks the
  issuer Common Name.

Certificate generation is **synchronous**: it does blocking key generation and
disk I/O. Both are cheap, but inside an event loop (for example Home Assistant)
callers should run :meth:`ServerCredentials.ensure` and
:meth:`ServerCredentials.rotate_server_cert` via an executor
(``loop.run_in_executor(None, creds.ensure, fqdn)``) so the loop is never blocked.
"""

from __future__ import annotations

import datetime
import ipaddress
import os
import pathlib
import ssl
from collections.abc import Sequence

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

__all__ = ["DEFAULT_CA_COMMON_NAME", "ServerCredentials", "tesla_ca_bundle_path"]

#: Subject/Issuer Common Name for the generated self-signed root CA.
DEFAULT_CA_COMMON_NAME = "python-fleet-telemetry Root CA"

#: Directory holding the vendored Tesla CA bundles shipped with the package.
_CERTS_DIR = pathlib.Path(__file__).parent / "certs"

_CA_VALIDITY = datetime.timedelta(days=3650)
_LEAF_VALIDITY = datetime.timedelta(days=730)


def tesla_ca_bundle_path(*, staging: bool = False) -> pathlib.Path:
    """Return the path to the vendored Tesla CA bundle.

    The production bundle is returned by default; pass ``staging=True`` for the
    engineering bundle. Both are copied verbatim from the ``teslamotors/
    fleet-telemetry`` reference server and are used to verify the client
    certificates that vehicles present.
    """
    name = "tesla_eng_ca.pem" if staging else "tesla_prod_ca.pem"
    return _CERTS_DIR / name


#: X.509 caps a Common Name attribute at 64 characters; longer FQDNs live in
#: the SubjectAltName only (which modern validators use exclusively anyway).
_MAX_CN_LENGTH = 64


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _mkdir_private(directory: pathlib.Path) -> None:
    """Create ``directory`` if needed and force 0700 (it holds private keys)."""
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)


def _write_key(path: pathlib.Path, key: ec.EllipticCurvePrivateKey) -> None:
    """Write ``key`` as unencrypted PKCS#8 PEM with 0600 permissions."""
    data = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
    # Re-assert the mode in case the file pre-existed with looser permissions.
    os.chmod(path, 0o600)


def _write_cert(path: pathlib.Path, cert: x509.Certificate) -> None:
    """Write ``cert`` as PEM with 0644 permissions."""
    path.write_bytes(cert.public_bytes(Encoding.PEM))
    os.chmod(path, 0o644)


def _san_general_names(
    fqdn: str, ip_addresses: Sequence[str]
) -> list[x509.GeneralName]:
    """Build the SAN entries for ``fqdn`` plus any extra ``ip_addresses``.

    If ``fqdn`` is itself an IP literal it is emitted as an ``IPAddress`` entry
    rather than a ``DNSName``.
    """
    names: list[x509.GeneralName] = []
    try:
        names.append(x509.IPAddress(ipaddress.ip_address(fqdn)))
    except ValueError:
        names.append(x509.DNSName(fqdn))
    names.extend(
        x509.IPAddress(ipaddress.ip_address(ip)) for ip in ip_addresses
    )
    return names


def _san_covers(cert: x509.Certificate, fqdn: str) -> bool:
    """Return whether ``cert``'s SubjectAltName already covers ``fqdn``."""
    try:
        san = cert.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
    except x509.ExtensionNotFound:
        return False
    try:
        ip = ipaddress.ip_address(fqdn)
    except ValueError:
        return fqdn in san.get_values_for_type(x509.DNSName)
    return ip in san.get_values_for_type(x509.IPAddress)


class ServerCredentials:
    """Server-side mTLS certificates persisted under a directory.

    Files created under ``directory``:

    ``ca.pem`` / ``ca.key``
        The self-signed root CA. Register ``ca.pem`` as
        ``fleet_telemetry_config.ca`` (pull its text via
        :attr:`ca_certificate_pem`). The vehicle trusts this CA to verify the
        server, so it must stay constant across leaf rotations.

    ``server.pem`` / ``server.key``
        The server leaf signed by the CA (``SubjectAltName`` = the FQDN); the
        certificate the server presents. Rotatable without touching the CA.

    Private key files are written with ``0600`` permissions.

    :param directory: Where the certificate/key files live; created if absent.
    :param ca_common_name: Subject/Issuer Common Name for a freshly generated
        root CA. Ignored once ``ca.pem`` already exists on disk.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        ca_common_name: str = DEFAULT_CA_COMMON_NAME,
    ) -> None:
        self._dir = pathlib.Path(directory)
        self._ca_common_name = ca_common_name

    @property
    def ca_cert_path(self) -> pathlib.Path:
        return self._dir / "ca.pem"

    @property
    def ca_key_path(self) -> pathlib.Path:
        return self._dir / "ca.key"

    @property
    def server_cert_path(self) -> pathlib.Path:
        return self._dir / "server.pem"

    @property
    def server_key_path(self) -> pathlib.Path:
        return self._dir / "server.key"

    # ---- get-or-create -------------------------------------------------- #

    def ensure(self, fqdn: str, *, ip_addresses: Sequence[str] = ()) -> None:
        """Get-or-create the root CA and the server leaf.

        Creates and persists the root CA if ``ca.pem``/``ca.key`` are absent.
        Then creates and persists the server leaf if ``server.pem``/
        ``server.key`` are absent, or if the existing leaf's SANs do not cover
        ``fqdn``. Idempotent: a second call with the same ``fqdn`` regenerates
        nothing — the keys and certificates already on disk are reused.

        This performs blocking key generation and disk I/O; run it via an
        executor when called from within an event loop.

        Raises :class:`FileNotFoundError` if exactly one of ``ca.pem``/``ca.key``
        is present: regenerating the trust anchor invalidates every vehicle's
        registered ``ca``, so it must be a deliberate act (delete the whole
        directory), never a silent recovery from partial state.
        """
        _mkdir_private(self._dir)

        ca_created = self._ensure_ca()
        # A freshly (re)created CA must always be paired with a freshly issued
        # leaf: the previous leaf, if any, was signed by a now-gone CA key and
        # would no longer chain to the CA registered at the vehicle.
        need_leaf = ca_created or not (
            self.server_cert_path.exists() and self.server_key_path.exists()
        )
        if need_leaf or not _san_covers(_load_cert(self.server_cert_path), fqdn):
            self._create_leaf(fqdn, ip_addresses)

    def _ensure_ca(self) -> bool:
        """Create the CA only if the directory is fresh; return whether created.

        Auto-creates only when *both* CA files are absent. If exactly one is
        present the state is corrupt and regenerating would silently invalidate
        every vehicle's registered ``ca``, so this raises instead.
        """
        cert_exists = self.ca_cert_path.exists()
        key_exists = self.ca_key_path.exists()
        if cert_exists and key_exists:
            return False
        if cert_exists or key_exists:
            raise FileNotFoundError(
                f"partial CA state under {self._dir}: found "
                f"{'ca.pem' if cert_exists else 'ca.key'} but not "
                f"{'ca.key' if cert_exists else 'ca.pem'}. Regenerating the CA "
                "invalidates every vehicle's registered `ca`; to do so "
                "deliberately, delete the directory and call ensure() again."
            )
        self._create_ca()
        return True

    def rotate_server_cert(
        self, fqdn: str, *, ip_addresses: Sequence[str] = ()
    ) -> None:
        """Re-issue only the server leaf from the existing CA.

        The CA is left untouched, so the ``ca`` value already pushed to the
        vehicle config stays valid. Raises :class:`FileNotFoundError` if the CA
        does not yet exist (call :meth:`ensure` first).

        This performs blocking key generation and disk I/O; run it via an
        executor when called from within an event loop.
        """
        if not (self.ca_cert_path.exists() and self.ca_key_path.exists()):
            raise FileNotFoundError(
                f"no CA at {self.ca_cert_path}; call ensure() before rotating"
            )
        self._create_leaf(fqdn, ip_addresses)

    @property
    def ca_certificate_pem(self) -> str:
        """PEM text of the root CA certificate.

        This is the value to register as ``fleet_telemetry_config.ca`` (what
        ``tesla-fleet-api`` pushes to the vehicle). Raises
        :class:`FileNotFoundError` if the CA has not been created yet.
        """
        if not self.ca_cert_path.exists():
            raise FileNotFoundError(
                f"no CA at {self.ca_cert_path}; call ensure() first"
            )
        return self.ca_cert_path.read_text()

    # ---- SSL context ---------------------------------------------------- #

    def build_ssl_context(self, *, staging: bool = False) -> ssl.SSLContext:
        """Build the server-side mTLS ``SSLContext``.

        The context presents the generated server leaf and verifies vehicle
        client certificates against the vendored Tesla CA bundle (production, or
        the engineering bundle when ``staging=True``). ``verify_mode`` is set to
        :data:`ssl.CERT_REQUIRED`, so the result is ready to pass straight to
        :class:`~fleet_telemetry.server.TelemetryServer`.
        """
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(
            certfile=str(self.server_cert_path), keyfile=str(self.server_key_path)
        )
        ctx.load_verify_locations(cafile=str(tesla_ca_bundle_path(staging=staging)))
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx

    # ---- generation internals ------------------------------------------ #

    def _create_ca(self) -> None:
        """Generate and persist a fresh self-signed root CA."""
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, self._ca_common_name)]
        )
        now = _now()
        ski = x509.SubjectKeyIdentifier.from_public_key(key.public_key())
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + _CA_VALIDITY)
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=0), critical=True
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=False,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(ski, critical=False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(
                    ski
                ),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        _write_key(self.ca_key_path, key)
        _write_cert(self.ca_cert_path, cert)

    def _create_leaf(self, fqdn: str, ip_addresses: Sequence[str]) -> None:
        """Generate and persist a server leaf signed by the existing CA."""
        if not fqdn:
            raise ValueError("fqdn must be a non-empty hostname or IP literal")
        ca_cert = _load_cert(self.ca_cert_path)
        ca_key = _load_ec_key(self.ca_key_path)

        key = ec.generate_private_key(ec.SECP256R1())
        now = _now()
        ca_ski = ca_cert.extensions.get_extension_for_class(
            x509.SubjectKeyIdentifier
        ).value
        # The Subject CN is legacy and capped at 64 chars; modern validators use
        # the SAN (always set below) exclusively. Set the CN only when it fits,
        # so deep subdomains / long DDNS names remain issuable.
        subject_attrs = (
            [x509.NameAttribute(NameOID.COMMON_NAME, fqdn)]
            if len(fqdn) <= _MAX_CN_LENGTH
            else []
        )
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name(subject_attrs))
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + _LEAF_VALIDITY)
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None), critical=True
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
                critical=False,
            )
            .add_extension(
                x509.SubjectAlternativeName(
                    _san_general_names(fqdn, ip_addresses)
                ),
                critical=False,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(
                    ca_ski
                ),
                critical=False,
            )
            .sign(ca_key, hashes.SHA256())
        )
        _write_key(self.server_key_path, key)
        _write_cert(self.server_cert_path, cert)


def _load_cert(path: pathlib.Path) -> x509.Certificate:
    return x509.load_pem_x509_certificate(path.read_bytes())


def _load_ec_key(path: pathlib.Path) -> ec.EllipticCurvePrivateKey:
    key = load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise TypeError(f"expected an EC private key at {path}")
    return key
