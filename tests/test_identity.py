"""Tests for the identity layer (:mod:`fleet_telemetry.identity`)."""

import dataclasses
import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from fleet_telemetry.identity import (
    Identity,
    UnauthorizedCertificate,
    identity_from_cert,
)


def make_cert(subject_cn: str, issuer_cn: str) -> x509.Certificate:
    """Mint a throwaway, self-signed cert with the given subject/issuer CNs."""
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    subject = (
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject_cn)])
        if subject_cn
        else x509.Name([])
    )
    issuer = (
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_cn)])
        if issuer_cn
        else x509.Name([])
    )
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )


def test_known_issuer_yields_vin_and_client_type() -> None:
    cert = make_cert("5YJ3E1EA7JF000001", "Tesla Issuing CA")
    identity = identity_from_cert(cert)
    assert identity.vin == "5YJ3E1EA7JF000001"
    assert identity.client_type == "vehicle_device"


def test_dotted_subject_cn_is_normalized_to_dashes() -> None:
    cert = make_cert("device.abc.123", "Tesla Issuing CA")
    identity = identity_from_cert(cert)
    assert identity.vin == "device-abc-123"


def test_oid_issuer_is_authorized() -> None:
    cert = make_cert("5YJ3E1EA7JF000002", "Tesla Energy Product Issuing CA")
    identity = identity_from_cert(cert)
    assert identity.client_type == "vehicle_device"


def test_unrecognized_issuer_raises() -> None:
    cert = make_cert("5YJ3E1EA7JF000003", "Some Random CA")
    with pytest.raises(UnauthorizedCertificate):
        identity_from_cert(cert)


def test_empty_subject_cn_yields_empty_vin() -> None:
    # Documents intentional behavior: a missing subject CN is not rejected here;
    # the empty VIN is surfaced for the caller to decide how to treat it.
    cert = make_cert("", "Tesla Issuing CA")
    identity = identity_from_cert(cert)
    assert identity.vin == ""


def test_identity_is_frozen() -> None:
    identity = Identity(vin="5YJ3E1EA7JF000001", client_type="vehicle_device")
    with pytest.raises(dataclasses.FrozenInstanceError):
        identity.vin = "other"  # type: ignore[misc]
