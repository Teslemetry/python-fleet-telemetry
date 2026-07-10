"""Tests for the vendored protobuf bindings in ``fleet_telemetry.proto``."""

from google.protobuf.timestamp_pb2 import Timestamp

from fleet_telemetry.proto import (
    vehicle_alert_pb2 as va,
    vehicle_connectivity_pb2 as vc,
    vehicle_data_pb2 as vd,
    vehicle_error_pb2 as ve,
)


def test_payload_roundtrip() -> None:
    speed_field = vd.Field.VehicleSpeed
    payload = vd.Payload(
        vin="5YJ3E1EA7JF000001",
        created_at=Timestamp(seconds=1700000000),
        data=[vd.Datum(key=speed_field, value=vd.Value(float_value=42.5))],
    )
    raw = payload.SerializeToString()
    decoded = vd.Payload()
    decoded.ParseFromString(raw)
    assert decoded.vin == "5YJ3E1EA7JF000001"
    assert decoded.data[0].key == speed_field
    assert decoded.data[0].value.float_value == 42.5
    assert decoded.created_at.seconds == 1700000000


def test_vehicle_alerts_roundtrip() -> None:
    msg = va.VehicleAlerts(
        vin="5YJ3E1EA7JF000001",
        created_at=Timestamp(seconds=1700000000),
        alerts=[va.VehicleAlert(name="AlertBmsHvSystem")],
    )
    decoded = va.VehicleAlerts()
    decoded.ParseFromString(msg.SerializeToString())
    assert decoded.vin == "5YJ3E1EA7JF000001"
    assert decoded.alerts[0].name == "AlertBmsHvSystem"


def test_vehicle_errors_roundtrip() -> None:
    msg = ve.VehicleErrors(
        vin="5YJ3E1EA7JF000001",
        created_at=Timestamp(seconds=1700000000),
        errors=[ve.VehicleError(name="SomeError", body="details")],
    )
    decoded = ve.VehicleErrors()
    decoded.ParseFromString(msg.SerializeToString())
    assert decoded.vin == "5YJ3E1EA7JF000001"
    assert decoded.errors[0].name == "SomeError"
    assert decoded.errors[0].body == "details"


def test_vehicle_connectivity_roundtrip() -> None:
    msg = vc.VehicleConnectivity(
        vin="5YJ3E1EA7JF000001",
        connection_id="conn-1",
        status=vc.ConnectivityEvent.CONNECTED,
        created_at=Timestamp(seconds=1700000000),
        network_interface="wifi",
    )
    decoded = vc.VehicleConnectivity()
    decoded.ParseFromString(msg.SerializeToString())
    assert decoded.vin == "5YJ3E1EA7JF000001"
    assert decoded.status == vc.ConnectivityEvent.CONNECTED
    assert decoded.network_interface == "wifi"
