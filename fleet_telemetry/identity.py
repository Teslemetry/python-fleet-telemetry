"""Client identity from a verified X.509 peer certificate.

A Tesla vehicle authenticates to the telemetry server with a client certificate
whose subject Common Name is the vehicle's VIN. This module turns an already
TLS-verified peer certificate into an :class:`Identity` — extracting the VIN and
rejecting any certificate whose issuer is not a recognized Tesla CA.

This layer is pure and synchronous: it inspects a parsed
:class:`~cryptography.x509.Certificate` and performs no I/O or TLS verification
of its own. The caller (the server) is responsible for having verified the
certificate chain before handing the peer cert here.
"""

from __future__ import annotations

from dataclasses import dataclass

from cryptography import x509
from cryptography.x509.oid import NameOID


class UnauthorizedCertificate(ValueError):
    """The client certificate was not issued by a recognized Tesla CA."""


@dataclass(frozen=True, slots=True)
class Identity:
    """A verified client's identity: its VIN and the kind of client it is."""

    vin: str
    client_type: str


#: Legacy Tesla issuing CAs, keyed by issuer Common Name to the client type.
_KNOWN_ISSUERS = {
    "TeslaMotors": "vehicle_device",
    "Tesla Issuing CA": "vehicle_device",
    "Tesla Motors Products CA": "vehicle_device",
}

#: Per-factory/product Tesla issuing CAs. Membership alone authorizes the cert.
_KNOWN_OID_ISSUERS = frozenset({
    "Tesla Motors Product Issuing CA", "Tesla Motors Product RSA Issuing CA",
    "Tesla Motors GF3 Product Issuing CA", "Tesla Motors GF3 Product RSA Issuing CA",
    "Tesla Energy Product Issuing CA", "Tesla Energy GF0 Product Issuing CA",
    "Tesla Motors GF0 Product Issuing CA", "Tesla Motors GF Austin Product Issuing CA",
    "Tesla Motors GF Berlin Product Issuing CA", "Tesla Product Access Issuing CA",
    "Tesla China Product Access Issuing CA", "Tesla GF0 Product Access Issuing CA",
    "Tesla GF3 Product Access Issuing CA", "Tesla GF Austin Product Access Issuing CA",
    "Tesla GF Berlin Product Access Issuing CA",
})


def _common_name(name: x509.Name) -> str:
    """Return the first Common Name attribute of ``name``, or ``""`` if absent."""
    attrs = name.get_attributes_for_oid(NameOID.COMMON_NAME)
    return str(attrs[0].value) if attrs else ""


def identity_from_cert(cert: x509.Certificate) -> Identity:
    """Extract an :class:`Identity` from a verified client certificate.

    The subject Common Name is the VIN, with dots normalized to dashes. The
    issuer Common Name must be a recognized Tesla CA, otherwise
    :class:`UnauthorizedCertificate` is raised.

    The VIN is returned verbatim (after normalization); a certificate with an
    empty subject Common Name yields an empty VIN rather than an error. Deciding
    how to treat an empty VIN is left to the caller.
    """
    vin = _common_name(cert.subject).replace(".", "-")
    issuer_cn = _common_name(cert.issuer)

    if issuer_cn in _KNOWN_OID_ISSUERS:
        return Identity(vin, "vehicle_device")
    if issuer_cn in _KNOWN_ISSUERS:
        return Identity(vin, _KNOWN_ISSUERS[issuer_cn])
    raise UnauthorizedCertificate(f"unrecognized issuer: {issuer_cn!r}")
