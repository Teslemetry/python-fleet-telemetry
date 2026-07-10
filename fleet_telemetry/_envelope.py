"""FlatBuffers transport-envelope codec.

This module is the sole boundary between the rest of the library and the
FlatBuffers wire format. All ``flatbuffers``-specific code lives here so the
backend can be swapped without touching callers. The public surface is:

* :data:`MESSAGE_TYPE_STREAM` / :data:`MESSAGE_TYPE_STREAM_ACK` union tags,
* :class:`EnvelopeError`,
* :class:`StreamFrame` / :class:`AckFrame` decoded views,
* :func:`message_type`, :func:`decode`, :func:`decode_ack`, :func:`encode_ack`.

The generated bindings (``_flatbuffers.tesla_generated``) are untyped, so every
value that crosses out of them is narrowed to a concrete type here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import flatbuffers as _flatbuffers_mod  # pyright: ignore[reportMissingTypeStubs]

from fleet_telemetry._flatbuffers import tesla_generated as _tesla_generated

# The generated bindings and the ``flatbuffers`` runtime ship no type
# information. Route both through ``Any``-typed aliases so attribute access
# yields ``Any`` (permitted under strict typing) rather than ``Unknown``; every
# value read out of them is narrowed with an explicit ``cast`` at the call site.
_fb: Any = _tesla_generated
_flatbuffers: Any = _flatbuffers_mod

MESSAGE_TYPE_STREAM = 4
MESSAGE_TYPE_STREAM_ACK = 5


class EnvelopeError(ValueError):
    """Raised when a frame cannot be decoded as the expected envelope type."""


@dataclass(frozen=True, slots=True)
class StreamFrame:
    """A decoded ``FlatbuffersStream`` envelope."""

    topic: bytes
    txid: bytes
    sender_id: bytes
    device_type: bytes
    device_id: bytes
    payload: bytes
    created_at: int
    message_id: bytes


@dataclass(frozen=True, slots=True)
class AckFrame:
    """A decoded ``FlatbuffersStreamAck`` envelope."""

    topic: bytes
    txid: bytes
    message_id: bytes


def _read_vector(accessor: Any, length: int) -> bytes:
    """Read a ``[ubyte]`` vector without pulling in numpy.

    ``accessor(i)`` returns the ``i``-th byte; ``length`` is its element count.
    Intentionally returns ``b""`` for BOTH an absent vector (``length`` 0 because
    the field is missing) and a present-but-empty vector; a future backend
    swapper must preserve this bytes-always behavior rather than returning None.
    """
    return bytes(cast(int, accessor(i)) for i in range(length))


def _root(frame: bytes) -> Any:
    try:
        return _fb.FlatbuffersEnvelope.GetRootAs(frame, 0)
    except Exception as exc:  # malformed/truncated buffer
        raise EnvelopeError("frame is not a valid envelope") from exc


def message_type(frame: bytes) -> int:
    """Return the union tag of ``frame`` (e.g. :data:`MESSAGE_TYPE_STREAM`)."""
    env = _root(frame)
    return cast(int, env.MessageType())


def decode(frame: bytes) -> StreamFrame:
    """Decode ``frame`` as a Stream envelope.

    Raises :class:`EnvelopeError` if the union tag is not
    :data:`MESSAGE_TYPE_STREAM`.
    """
    env = _root(frame)
    tag = cast(int, env.MessageType())
    if tag != MESSAGE_TYPE_STREAM:
        raise EnvelopeError(
            f"expected Stream (type {MESSAGE_TYPE_STREAM}), got type {tag}"
        )

    union = env.Message()
    if union is None:
        raise EnvelopeError("Stream envelope has no message value")

    stream = _fb.FlatbuffersStream()
    stream.Init(union.Bytes, union.Pos)

    return StreamFrame(
        topic=_read_vector(env.Topic, cast(int, env.TopicLength())),
        txid=_read_vector(env.Txid, cast(int, env.TxidLength())),
        sender_id=_read_vector(stream.SenderId, cast(int, stream.SenderIdLength())),
        device_type=_read_vector(
            stream.DeviceType, cast(int, stream.DeviceTypeLength())
        ),
        device_id=_read_vector(stream.DeviceId, cast(int, stream.DeviceIdLength())),
        payload=_read_vector(stream.Payload, cast(int, stream.PayloadLength())),
        created_at=cast(int, stream.CreatedAt()),
        message_id=_read_vector(env.MessageId, cast(int, env.MessageIdLength())),
    )


def decode_ack(frame: bytes) -> AckFrame:
    """Decode ``frame`` as a StreamAck envelope.

    Raises :class:`EnvelopeError` if the union tag is not
    :data:`MESSAGE_TYPE_STREAM_ACK`.
    """
    env = _root(frame)
    tag = cast(int, env.MessageType())
    if tag != MESSAGE_TYPE_STREAM_ACK:
        raise EnvelopeError(
            f"expected StreamAck (type {MESSAGE_TYPE_STREAM_ACK}), got type {tag}"
        )

    return AckFrame(
        topic=_read_vector(env.Topic, cast(int, env.TopicLength())),
        txid=_read_vector(env.Txid, cast(int, env.TxidLength())),
        message_id=_read_vector(env.MessageId, cast(int, env.MessageIdLength())),
    )


def encode_ack(*, txid: bytes, topic: bytes, message_id: bytes = b"") -> bytes:
    """Encode a StreamAck envelope and return its bytes."""
    builder = _flatbuffers.Builder(0)

    txid_off = builder.CreateByteVector(txid)
    topic_off = builder.CreateByteVector(topic)
    message_id_off = builder.CreateByteVector(message_id)

    _fb.FlatbuffersStreamAckStart(builder)
    ack_off = _fb.FlatbuffersStreamAckEnd(builder)

    _fb.FlatbuffersEnvelopeStart(builder)
    _fb.FlatbuffersEnvelopeAddTxid(builder, txid_off)
    _fb.FlatbuffersEnvelopeAddTopic(builder, topic_off)
    _fb.FlatbuffersEnvelopeAddMessageType(builder, MESSAGE_TYPE_STREAM_ACK)
    _fb.FlatbuffersEnvelopeAddMessage(builder, ack_off)
    _fb.FlatbuffersEnvelopeAddMessageId(builder, message_id_off)
    env_off = _fb.FlatbuffersEnvelopeEnd(builder)

    builder.Finish(env_off)
    return bytes(builder.Output())
