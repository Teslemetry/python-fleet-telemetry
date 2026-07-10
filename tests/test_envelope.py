import pytest

from fleet_telemetry import _envelope
from tests.fixtures.golden import GOLDEN_STREAM, GOLDEN_ACK


def test_decode_golden_stream():
    frame = _envelope.decode(GOLDEN_STREAM)
    assert frame.topic == b"V"
    assert frame.txid == b"txid-0001"
    assert frame.device_id == b"5YJ3E1EA7JF000001"
    assert frame.sender_id == b"vehicle_device.5YJ3E1EA7JF000001"
    assert frame.payload == bytes([0x08, 0x01, 0x10, 0x02])
    assert frame.created_at == 1700000000
    assert frame.message_id == b"msg-0001"


def test_encode_ack_is_decodable():
    ack = _envelope.encode_ack(txid=b"txid-0001", topic=b"V", message_id=b"msg-0001")
    assert _envelope.message_type(ack) == _envelope.MESSAGE_TYPE_STREAM_ACK
    parsed = _envelope.decode_ack(ack)
    assert parsed.txid == b"txid-0001"
    assert parsed.topic == b"V"
    assert parsed.message_id == b"msg-0001"


def test_encode_ack_default_empty_message_id_round_trips():
    ack = _envelope.encode_ack(txid=b"t", topic=b"V")
    assert _envelope.message_type(ack) == _envelope.MESSAGE_TYPE_STREAM_ACK
    parsed = _envelope.decode_ack(ack)
    assert parsed.txid == b"t"
    assert parsed.topic == b"V"
    assert parsed.message_id == b""


def test_go_ack_is_decodable():
    parsed = _envelope.decode_ack(GOLDEN_ACK)
    assert parsed.txid == b"txid-0001"
    assert parsed.topic == b"V"
    assert parsed.message_id == b"msg-0001"
    assert _envelope.message_type(GOLDEN_ACK) == _envelope.MESSAGE_TYPE_STREAM_ACK


def test_decode_rejects_non_stream_type():
    with pytest.raises(_envelope.EnvelopeError):
        _envelope.decode(GOLDEN_ACK)


def test_decode_ack_rejects_non_ack_type():
    with pytest.raises(_envelope.EnvelopeError):
        _envelope.decode_ack(GOLDEN_STREAM)


@pytest.mark.parametrize("bad", [b"", b"\x00\x01\x02"])
def test_decode_rejects_malformed_frame(bad: bytes):
    with pytest.raises(_envelope.EnvelopeError):
        _envelope.decode(bad)


def test_message_type_rejects_malformed_frame():
    with pytest.raises(_envelope.EnvelopeError):
        _envelope.message_type(b"")
