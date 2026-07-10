"""Tests for the records layer (:mod:`fleet_telemetry.records`)."""

import dataclasses
from datetime import datetime, timezone

import pytest
from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp

from fleet_telemetry.proto import (
    vehicle_alert_pb2 as va,
    vehicle_data_pb2 as vd,
)
from fleet_telemetry.records import Record, Topic, parse_record

_VIN = "5YJ3E1EA7JF000001"
_EPOCH = 1700000000


def _data_payload() -> bytes:
    payload = vd.Payload(
        vin=_VIN,
        created_at=Timestamp(seconds=_EPOCH),
        data=[
            vd.Datum(
                key=vd.Field.VehicleSpeed,
                value=vd.Value(float_value=42.5),
            ),
            vd.Datum(
                key=vd.Field.Gear,
                value=vd.Value(shift_state_value=vd.ShiftState.ShiftStateD),
            ),
        ],
    )
    return payload.SerializeToString()


def test_parse_record_data() -> None:
    record = parse_record(
        vin=_VIN,
        topic=b"V",
        txid=b"tx-123",
        created_at=_EPOCH,
        payload=_data_payload(),
    )
    assert record.topic is Topic.DATA
    assert record.vin == _VIN
    assert record.txid == "tx-123"
    assert isinstance(record.message, vd.Payload)
    assert isinstance(record.created_at, datetime)
    assert record.created_at.tzinfo is not None
    assert record.created_at == datetime.fromtimestamp(_EPOCH, tz=timezone.utc)
    assert record.raw == _data_payload()


def test_fields_data() -> None:
    record = parse_record(
        vin=_VIN,
        topic=b"V",
        txid=b"tx-123",
        created_at=_EPOCH,
        payload=_data_payload(),
    )
    fields = record.fields()
    assert fields["VehicleSpeed"] == 42.5
    assert "Gear" in fields
    assert fields["Gear"] == vd.ShiftState.ShiftStateD


def test_fields_unset_oneof_is_none() -> None:
    payload = vd.Payload(
        vin=_VIN,
        created_at=Timestamp(seconds=_EPOCH),
        data=[vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value())],
    )
    record = parse_record(
        vin=_VIN,
        topic=b"V",
        txid=b"tx",
        created_at=_EPOCH,
        payload=payload.SerializeToString(),
    )
    assert record.fields()["VehicleSpeed"] is None


def test_fields_unknown_key_is_synthesized() -> None:
    # Proto3 open enum: a firmware-new integer key survives ParseFromString and
    # must surface under "Field_<int>" rather than crashing fields().
    payload = vd.Payload(
        vin=_VIN,
        created_at=Timestamp(seconds=_EPOCH),
        # Deliberately an int outside the Field enum (firmware-new key); the
        # generated stub types key as Field | str, so ignore that here.
        data=[vd.Datum(key=987654, value=vd.Value(float_value=1.0))],  # pyright: ignore[reportArgumentType]
    )
    record = parse_record(
        vin=_VIN,
        topic=b"V",
        txid=b"tx",
        created_at=_EPOCH,
        payload=payload.SerializeToString(),
    )
    fields = record.fields()
    assert fields["Field_987654"] == 1.0


def test_fields_duplicate_key_last_wins() -> None:
    payload = vd.Payload(
        vin=_VIN,
        created_at=Timestamp(seconds=_EPOCH),
        data=[
            vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value(float_value=10.0)),
            vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value(float_value=20.0)),
        ],
    )
    record = parse_record(
        vin=_VIN,
        topic=b"V",
        txid=b"tx",
        created_at=_EPOCH,
        payload=payload.SerializeToString(),
    )
    assert record.fields()["VehicleSpeed"] == 20.0


def test_malformed_payload_raises_decode_error() -> None:
    # Untrusted-bytes boundary: a payload that isn't a valid Payload propagates
    # DecodeError (not ValueError, which is reserved for unknown topics).
    with pytest.raises(DecodeError):
        parse_record(
            vin=_VIN,
            topic=b"V",
            txid=b"tx",
            created_at=_EPOCH,
            payload=b"\xff\xff\xff\xff\xff\xff",
        )


def test_fields_non_data_is_empty() -> None:
    msg = va.VehicleAlerts(
        vin=_VIN,
        created_at=Timestamp(seconds=_EPOCH),
        alerts=[va.VehicleAlert(name="AlertBmsHvSystem")],
    )
    record = parse_record(
        vin=_VIN,
        topic=b"alerts",
        txid=b"tx",
        created_at=_EPOCH,
        payload=msg.SerializeToString(),
    )
    assert record.fields() == {}


def test_unknown_topic_raises() -> None:
    with pytest.raises(ValueError, match="topic"):
        parse_record(
            vin=_VIN,
            topic=b"bogus",
            txid=b"tx",
            created_at=_EPOCH,
            payload=b"",
        )


def test_parse_record_alerts() -> None:
    msg = va.VehicleAlerts(
        vin=_VIN,
        created_at=Timestamp(seconds=_EPOCH),
        alerts=[va.VehicleAlert(name="AlertBmsHvSystem")],
    )
    record = parse_record(
        vin=_VIN,
        topic=b"alerts",
        txid=b"tx",
        created_at=_EPOCH,
        payload=msg.SerializeToString(),
    )
    assert record.topic is Topic.ALERTS
    assert isinstance(record.message, va.VehicleAlerts)
    assert record.message.alerts[0].name == "AlertBmsHvSystem"


def test_record_is_frozen() -> None:
    record = parse_record(
        vin=_VIN,
        topic=b"V",
        txid=b"tx",
        created_at=_EPOCH,
        payload=_data_payload(),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.vin = "other"  # type: ignore[misc]
    assert isinstance(record, Record)
