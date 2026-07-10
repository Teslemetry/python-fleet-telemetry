"""Domain records: typed, per-message views of decoded transport frames.

A transport :class:`~fleet_telemetry._envelope.StreamFrame` carries a topic and
an opaque protobuf payload. This module turns that pair into a :class:`Record` —
one event per WebSocket frame (never one per signal) — by selecting the right
protobuf message class for the topic and decoding the payload into it.

The public surface is :class:`Topic`, :class:`Record`, the :data:`Message`
alias, and :func:`parse_record`. The dispatch layer builds on
:meth:`Record.fields`, which flattens a DATA record's signals into a
``name -> value`` mapping for filtering.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Union

from fleet_telemetry.proto import vehicle_data_pb2 as vd
from fleet_telemetry.proto.vehicle_alert_pb2 import VehicleAlerts
from fleet_telemetry.proto.vehicle_connectivity_pb2 import VehicleConnectivity
from fleet_telemetry.proto.vehicle_error_pb2 import VehicleErrors


class Topic(StrEnum):
    """The WebSocket stream topics a vehicle publishes to."""

    DATA = "V"
    ALERTS = "alerts"
    ERRORS = "errors"
    CONNECTIVITY = "connectivity"


#: Union of every concrete protobuf message a :class:`Record` may carry.
Message = Union[vd.Payload, VehicleAlerts, VehicleErrors, VehicleConnectivity]

#: Maps each topic to the protobuf message class that decodes its payload.
_PARSERS: dict[Topic, type[Message]] = {
    Topic.DATA: vd.Payload,
    Topic.ALERTS: VehicleAlerts,
    Topic.ERRORS: VehicleErrors,
    Topic.CONNECTIVITY: VehicleConnectivity,
}


@dataclass(frozen=True, slots=True)
class Record:
    """A single decoded telemetry event.

    One :class:`Record` corresponds to one transport frame. :attr:`message` is
    the concrete protobuf message decoded from :attr:`raw`; :attr:`raw` retains
    the original payload bytes so callers can re-decode or forward them verbatim.
    """

    vin: str
    topic: Topic
    created_at: datetime
    txid: str
    message: Message
    raw: bytes

    def fields(self) -> dict[str, object]:
        """Return a DATA record's signals as a ``signal-name -> value`` mapping.

        Each ``Datum`` key is resolved to its ``Field`` enum name and paired
        with the value set in the ``Value`` oneof (``None`` if the oneof is
        unset). Returns an empty dict for any non-DATA topic, whose messages
        carry no per-signal data.
        """
        if self.topic is not Topic.DATA:
            return {}

        message = self.message
        assert isinstance(message, vd.Payload)

        result: dict[str, object] = {}
        for datum in message.data:
            name = vd.Field.Name(datum.key)
            which = datum.value.WhichOneof("value")
            result[name] = None if which is None else getattr(datum.value, which)
        return result


def parse_record(
    *,
    vin: str,
    topic: bytes,
    txid: bytes,
    created_at: int,
    payload: bytes,
) -> Record:
    """Decode one transport frame into a typed :class:`Record`.

    ``topic`` is decoded to a :class:`Topic`; an unknown topic raises
    :class:`ValueError`. ``payload`` is parsed with the message class mapped to
    that topic. ``created_at`` is epoch seconds, converted to a timezone-aware
    UTC :class:`~datetime.datetime`. ``txid`` bytes are decoded to ``str`` with
    replacement of any undecodable bytes.
    """
    try:
        topic_enum = Topic(topic.decode(errors="replace"))
    except ValueError as exc:
        raise ValueError(f"unknown topic: {topic!r}") from exc

    message: Message = _PARSERS[topic_enum]()
    message.ParseFromString(payload)

    return Record(
        vin=vin,
        topic=topic_enum,
        created_at=datetime.fromtimestamp(created_at, tz=timezone.utc),
        txid=txid.decode(errors="replace"),
        message=message,
        raw=payload,
    )
