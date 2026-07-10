# python-fleet-telemetry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an async Python 3.13+ library that terminates the raw Tesla vehicle telemetry connection (native mTLS WebSocket), decodes the FlatBuffers transport envelope and protobuf payloads, and delivers per-message records to consumers through flexible filtered event listeners.

**Architecture:** An `aiohttp` server terminates mTLS in-process and derives the VIN from the verified client certificate. Each WebSocket frame is decoded by a swappable `_envelope` module (FlatBuffers `Envelope`→`Stream`), its protobuf payload parsed into a `Record`, acked immediately with a FlatBuffers `StreamAck`, then dispatched to filtered listeners and an async-iterator queue. Connection state is the only server-held state, surfaced via an accessor plus synthesized connectivity events.

**Tech Stack:** Python 3.13+, `aiohttp`, `protobuf>=6.32.0`, `flatbuffers>=24`, `uv`, `ruff`, `pyright` (strict), `pytest` + `pytest-asyncio`. Toolchain parity with `python-tesla-fleet-api`.

**Design doc:** `docs/plans/2026-07-10-python-fleet-telemetry-design.md` — read it first.

**Reference sources (read-only, do not modify):**
- Go server: `/Users/brett/Teslemetry/tesla-fleet-telemetry`
- Consumer-side idioms: `/Users/brett/Teslemetry/python-teslemetry-stream`
- Tooling parity: `/Users/brett/Teslemetry/python-tesla-fleet-api`
- Generated protobuf bindings to vendor: `/Users/brett/Teslemetry/tesla-fleet-telemetry/protos/python/*_pb2.py`
- `.proto` sources: `/Users/brett/Teslemetry/tesla-fleet-telemetry/protos/*.proto`

**Conventions for every task:** TDD (failing test first). DRY. YAGNI. Commit after each task passes. Every commit message ends with the trailer:
```
Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
```
Run `git -c user.name='Brett Adams' -c user.email='brett.whynot@gmail.com' commit` (repo has no configured user).

---

## Task 0: Project scaffold & tooling

**Files:**
- Create: `pyproject.toml`
- Create: `fleet_telemetry/__init__.py`
- Create: `fleet_telemetry/py.typed` (empty)
- Create: `tests/__init__.py`
- Create: `tests/test_smoke.py`

**Step 1: Write `pyproject.toml`**

```toml
[build-system]
build-backend = "setuptools.build_meta"
requires = ["setuptools>=77.0"]

[project]
name = "python-fleet-telemetry"
version = "0.1.0"
license = "Apache-2.0"
description = "Receive Tesla Fleet Telemetry directly from vehicles via event listeners"
readme = "README.md"
authors = [{ name = "Brett Adams", email = "hello@teslemetry.com" }]
requires-python = ">=3.13"
classifiers = [
    "Development Status :: 3 - Alpha",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.13",
    "Operating System :: OS Independent",
]
dependencies = [
    "aiohttp>=3",
    "protobuf>=6.32.0",
    "flatbuffers>=24",
]

[project.urls]
"Homepage" = "https://github.com/Teslemetry/python-fleet-telemetry"

[dependency-groups]
dev = ["pyright>=1.1", "pytest>=8", "pytest-asyncio>=0.24", "ruff>=0.14"]

[tool.setuptools.packages.find]
include = ["fleet_telemetry*"]

[tool.setuptools.package-data]
fleet_telemetry = ["py.typed"]

[tool.ruff]
exclude = ["fleet_telemetry/proto/*", "fleet_telemetry/_flatbuffers/*"]

[tool.pyright]
include = ["fleet_telemetry"]
typeCheckingMode = "strict"
exclude = ["fleet_telemetry/proto/*", "fleet_telemetry/_flatbuffers/*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

**Step 2: Create empty `fleet_telemetry/__init__.py`, `fleet_telemetry/py.typed`, `tests/__init__.py`.**

**Step 3: Write `tests/test_smoke.py`**

```python
def test_package_imports():
    import fleet_telemetry
    assert fleet_telemetry is not None
```

**Step 4: Set up the environment and run**

Run:
```bash
cd /Users/brett/Teslemetry/python-fleet-telemetry
uv sync
uv run pytest tests/test_smoke.py -v
uv run ruff check .
uv run pyright
```
Expected: pytest PASS, ruff clean, pyright 0 errors.

**Step 5: Commit**

```bash
git add -A
git commit -m "chore: project scaffold and tooling"
```

---

## Task 1: Vendor protobuf bindings + regeneration script

**Context:** The Go repo already ships protobuf bindings generated with a compatible protoc. We vendor the `.proto` sources plus a regeneration script (matching `python-tesla-fleet-api`'s pattern), and generate fresh bindings so they match our `protobuf>=6.32.0` runtime.

**Files:**
- Create: `fleet_telemetry/proto/__init__.py` (empty)
- Copy: `/Users/brett/Teslemetry/tesla-fleet-telemetry/protos/*.proto` → `proto/` (source `.proto` files kept at repo root `proto/` for regeneration)
- Create: `proto/generate.sh`
- Create (generated): `fleet_telemetry/proto/*_pb2.py` + `*_pb2.pyi`
- Test: `tests/test_proto.py`

**Step 1: Copy the five `.proto` files**

```bash
mkdir -p proto fleet_telemetry/proto
cp /Users/brett/Teslemetry/tesla-fleet-telemetry/protos/vehicle_data.proto \
   /Users/brett/Teslemetry/tesla-fleet-telemetry/protos/vehicle_alert.proto \
   /Users/brett/Teslemetry/tesla-fleet-telemetry/protos/vehicle_error.proto \
   /Users/brett/Teslemetry/tesla-fleet-telemetry/protos/vehicle_connectivity.proto \
   /Users/brett/Teslemetry/tesla-fleet-telemetry/protos/vehicle_metric.proto \
   proto/
touch fleet_telemetry/proto/__init__.py
```

**Step 2: Write `proto/generate.sh`**

```bash
#!/usr/bin/env bash
# Regenerate protobuf bindings into fleet_telemetry/proto/.
# Requires grpcio-tools matching the protobuf runtime.
set -euo pipefail
cd "$(dirname "$0")/.."
uv run --with "grpcio-tools" python -m grpc_tools.protoc \
    -Iproto \
    --python_out=fleet_telemetry/proto \
    --pyi_out=fleet_telemetry/proto \
    proto/vehicle_data.proto \
    proto/vehicle_alert.proto \
    proto/vehicle_error.proto \
    proto/vehicle_connectivity.proto \
    proto/vehicle_metric.proto
# Fix imports: generated files reference each other by bare module name; make them package-relative.
sed -i '' -E 's/^import (vehicle_[a-z]+_pb2)/from fleet_telemetry.proto import \1/' fleet_telemetry/proto/*_pb2.py
```

Make executable: `chmod +x proto/generate.sh`.

**Step 3: Run generation**

Run: `./proto/generate.sh`
Expected: `fleet_telemetry/proto/vehicle_data_pb2.py` etc. created. Verify no cross-import errors:
`uv run python -c "from fleet_telemetry.proto import vehicle_data_pb2, vehicle_alert_pb2, vehicle_error_pb2, vehicle_connectivity_pb2"`
Expected: no output, exit 0.

> If `sed` import-rewrite proves fragile, alternative: pass `-Iproto` and generate with a package prefix, or hand-edit the 1–2 cross-imports. The connectivity/alert/error protos import `vehicle_data.proto`.

**Step 4: Write `tests/test_proto.py`**

```python
from google.protobuf.timestamp_pb2 import Timestamp
from fleet_telemetry.proto import vehicle_data_pb2 as vd


def test_payload_roundtrip():
    payload = vd.Payload(
        vin="5YJ3E1EA7JF000001",
        created_at=Timestamp(seconds=1700000000),
        data=[vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value(float_value=42.5))],
    )
    raw = payload.SerializeToString()

    decoded = vd.Payload()
    decoded.ParseFromString(raw)

    assert decoded.vin == "5YJ3E1EA7JF000001"
    assert decoded.data[0].key == vd.Field.VehicleSpeed
    assert decoded.data[0].value.float_value == 42.5
```

> Verify the exact enum member name (`Field.VehicleSpeed` vs `Field.Field_VehicleSpeed`) by inspecting the generated `vehicle_data_pb2.pyi` first; adjust the test to match.

**Step 5: Run**

Run: `uv run pytest tests/test_proto.py -v`
Expected: PASS.

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: vendor protobuf bindings and regeneration script"
```

---

## Task 2: FlatBuffers envelope codec (riskiest — golden-byte validated)

**Context:** Every raw WebSocket frame is a FlatBuffers `Envelope` wrapping either a `Stream` (incoming telemetry, message_type=4) or a `StreamAck` (our reply, message_type=5). We reconstruct the schema to **exactly** match the Go generated slot layout, generate Python bindings with `flatc`, and validate against **real golden frames captured from the Go reference** (below). The codec lives behind a narrow interface so the backend is swappable.

**Verified layout (from Go generated code):**

`FlatbuffersEnvelope` — `StartObject(5)`:
| slot | field | type |
|---|---|---|
| 0 | txid | `[ubyte]` |
| 1 | topic | `[ubyte]` |
| 2 | message_type | `Message` (union tag, ubyte) |
| 3 | message | union value (table offset) |
| 4 | message_id | `[ubyte]` |

`FlatbuffersStream` — `StartObject(6)`:
| slot | field | type |
|---|---|---|
| 0 | created_at | `uint32` |
| 1 | sender_id | `[ubyte]` |
| 2 | payload | `[ubyte]` |
| 3 | device_type | `[ubyte]` |
| 4 | device_id | `[ubyte]` |
| 5 | delivered_at_epoch_ms | `uint64` |

`FlatbuffersStreamAck` — `StartObject(0)` (empty table).

Union `Message`: `NONE=0`, `FlatbuffersStream=4`, `FlatbuffersStreamAck=5` — so three padding members occupy positions 1–3.

**Files:**
- Create: `fbs/tesla.fbs`
- Create: `fbs/generate.sh`
- Create (generated): `fleet_telemetry/_flatbuffers/tesla/*.py`
- Create: `fleet_telemetry/_envelope.py`
- Test: `tests/test_envelope.py`
- Test data: `tests/fixtures/golden.py`

**Step 1: Write `fbs/tesla.fbs`**

```fbs
namespace tesla;

table FlatbuffersStream {
  created_at:uint32;             // slot 0
  sender_id:[ubyte];            // slot 1
  payload:[ubyte];              // slot 2
  device_type:[ubyte];          // slot 3
  device_id:[ubyte];            // slot 4
  delivered_at_epoch_ms:uint64; // slot 5
}

table FlatbuffersStreamAck {}

// Padding tables force the union tags to Stream=4, StreamAck=5,
// matching Tesla's on-wire numbering. We never decode/encode the pads.
table _Pad1 {}
table _Pad2 {}
table _Pad3 {}

union Message { _Pad1, _Pad2, _Pad3, FlatbuffersStream, FlatbuffersStreamAck }

table FlatbuffersEnvelope {
  txid:[ubyte];        // slot 0
  topic:[ubyte];       // slot 1
  message:Message;     // slots 2 (type) + 3 (value)
  message_id:[ubyte];  // slot 4
}

root_type FlatbuffersEnvelope;
```

> A flatc `union` field auto-generates the `_type` slot immediately before the value slot, so declaring `message:Message` after `txid` and `topic` yields message_type=slot 2, message=slot 3, and `message_id`=slot 4 — matching the Go layout. **Validate this against the golden bytes in Step 6; if offsets differ, adjust field order.**

**Step 2: Write `fbs/generate.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v flatc >/dev/null || { echo "flatc not found: brew install flatbuffers"; exit 1; }
flatc --python --gen-onefile -o fleet_telemetry/_flatbuffers fbs/tesla.fbs
touch fleet_telemetry/_flatbuffers/__init__.py
```

Run: `chmod +x fbs/generate.sh && ./fbs/generate.sh`
Expected: generated Python module(s) under `fleet_telemetry/_flatbuffers/`.

> If `flatc` is unavailable, install via `brew install flatbuffers`. Confirm the generated package layout and adjust import paths in `_envelope.py` accordingly.

**Step 3: Write `tests/fixtures/golden.py` (real frames from the Go reference)**

```python
# Captured from github.com/teslamotors/fleet-telemetry via messages.StreamMessage.ToBytes().
# Stream frame: topic="V", txid="txid-0001", sender_id="vehicle_device.5YJ3E1EA7JF000001",
#   device_type="vehicle_device", device_id="5YJ3E1EA7JF000001",
#   payload=bytes([0x08,0x01,0x10,0x02]), created_at=1700000000, message_id="msg-0001".
GOLDEN_STREAM_HEX = (
    "1400000000000e001800140010000f00080004000e000000140000004400000000000004"
    "180000001c000000080000006d73672d3030303100000000010000005600000009000000"
    "747869642d30303031000e001800100014000c00080004000e0000001400000028000000"
    "3800000000f153653c0000001100000035594a3345314541374a463030303030310000000e"
    "00000076656869636c655f64657669636500000400000008011002000000002000000076"
    "656869636c655f6465766963652e35594a3345314541374a4630303030303100000000"
)

# StreamAck frame: topic="V", txid="txid-0001", message_id="msg-0001".
GOLDEN_ACK_HEX = (
    "1400000000000e001800140010000f00080004000e000000140000003800000000000005"
    "180000001c000000080000006d73672d3030303100000000010000005600000009000000"
    "747869642d303030310000000000000000000000"
)

GOLDEN_STREAM = bytes.fromhex(GOLDEN_STREAM_HEX)
GOLDEN_ACK = bytes.fromhex(GOLDEN_ACK_HEX)
```

**Step 4: Write the failing test `tests/test_envelope.py`**

```python
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


def test_encode_ack_matches_go_semantics():
    # Our encoder need not be byte-identical to Go, but must be decodable
    # as a StreamAck carrying the same txid/topic/message_id.
    ack = _envelope.encode_ack(txid=b"txid-0001", topic=b"V", message_id=b"msg-0001")
    assert _envelope.message_type(ack) == _envelope.MESSAGE_TYPE_STREAM_ACK
    parsed = _envelope.decode_ack(ack)
    assert parsed.txid == b"txid-0001"
    assert parsed.topic == b"V"


def test_go_ack_is_decodable():
    parsed = _envelope.decode_ack(GOLDEN_ACK)
    assert parsed.txid == b"txid-0001"
    assert parsed.topic == b"V"
    assert _envelope.message_type(GOLDEN_ACK) == _envelope.MESSAGE_TYPE_STREAM_ACK


def test_decode_rejects_non_stream_type():
    import pytest
    with pytest.raises(_envelope.EnvelopeError):
        _envelope.decode(GOLDEN_ACK)  # ack is type 5, decode() expects a Stream (4)
```

**Step 5: Run to verify it fails**

Run: `uv run pytest tests/test_envelope.py -v`
Expected: FAIL (`_envelope` has no `decode`).

**Step 6: Write `fleet_telemetry/_envelope.py`**

Narrow, backend-swappable interface. Uses the generated `_flatbuffers` bindings.

```python
"""FlatBuffers transport envelope codec (swappable backend).

Isolates the only FlatBuffers dependency in the library. If the `flatbuffers`
dependency ever needs replacing with a hand-rolled vtable codec, only this
module changes; the golden-byte tests pin the wire behavior across backends.
"""
from __future__ import annotations

from dataclasses import dataclass

import flatbuffers

# Import generated bindings. Adjust module path to match flatc --gen-onefile output.
from fleet_telemetry._flatbuffers import tesla as _t  # noqa

MESSAGE_TYPE_STREAM = 4
MESSAGE_TYPE_STREAM_ACK = 5


class EnvelopeError(ValueError):
    """Raised when a frame cannot be decoded as the expected envelope type."""


@dataclass(frozen=True, slots=True)
class StreamFrame:
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
    topic: bytes
    txid: bytes
    message_id: bytes


def message_type(frame: bytes) -> int:
    env = _t.FlatbuffersEnvelope.GetRootAs(frame, 0)
    return env.MessageType()


def decode(frame: bytes) -> StreamFrame:
    """Decode a Stream frame. Raises EnvelopeError if not a Stream (type 4)."""
    env = _t.FlatbuffersEnvelope.GetRootAs(frame, 0)
    if env.MessageType() != MESSAGE_TYPE_STREAM:
        raise EnvelopeError(f"expected Stream (4), got message_type={env.MessageType()}")
    tbl = flatbuffers.Table(env._tab.Bytes, 0)  # union table
    env.Message(tbl)
    stream = _t.FlatbuffersStream()
    stream.Init(tbl.Bytes, tbl.Pos)
    return StreamFrame(
        topic=env.TopicAsNumpy().tobytes() if env.TopicLength() else b"",
        txid=env.TxidAsNumpy().tobytes() if env.TxidLength() else b"",
        message_id=env.MessageIdAsNumpy().tobytes() if env.MessageIdLength() else b"",
        sender_id=stream.SenderIdAsNumpy().tobytes() if stream.SenderIdLength() else b"",
        device_type=stream.DeviceTypeAsNumpy().tobytes() if stream.DeviceTypeLength() else b"",
        device_id=stream.DeviceIdAsNumpy().tobytes() if stream.DeviceIdLength() else b"",
        payload=stream.PayloadAsNumpy().tobytes() if stream.PayloadLength() else b"",
        created_at=stream.CreatedAt(),
    )


def decode_ack(frame: bytes) -> AckFrame:
    env = _t.FlatbuffersEnvelope.GetRootAs(frame, 0)
    if env.MessageType() != MESSAGE_TYPE_STREAM_ACK:
        raise EnvelopeError(f"expected StreamAck (5), got message_type={env.MessageType()}")
    return AckFrame(
        topic=env.TopicAsNumpy().tobytes() if env.TopicLength() else b"",
        txid=env.TxidAsNumpy().tobytes() if env.TxidLength() else b"",
        message_id=env.MessageIdAsNumpy().tobytes() if env.MessageIdLength() else b"",
    )


def encode_ack(*, txid: bytes, topic: bytes, message_id: bytes = b"") -> bytes:
    b = flatbuffers.Builder(64)
    txid_off = b.CreateByteVector(txid)
    topic_off = b.CreateByteVector(topic)
    mid_off = b.CreateByteVector(message_id) if message_id else None

    _t.FlatbuffersStreamAckStart(b)
    ack_off = _t.FlatbuffersStreamAckEnd(b)

    _t.FlatbuffersEnvelopeStart(b)
    _t.FlatbuffersEnvelopeAddTxid(b, txid_off)
    _t.FlatbuffersEnvelopeAddTopic(b, topic_off)
    _t.FlatbuffersEnvelopeAddMessageType(b, MESSAGE_TYPE_STREAM_ACK)
    _t.FlatbuffersEnvelopeAddMessage(b, ack_off)
    if mid_off is not None:
        _t.FlatbuffersEnvelopeAddMessageId(b, mid_off)
    env_off = _t.FlatbuffersEnvelopeEnd(b)

    b.Finish(env_off)
    return bytes(b.Output())
```

> The generated API names (`GetRootAs`, `TopicAsNumpy`, `FlatbuffersEnvelopeAddTxid`, `_tab`) depend on the flatc version. **Inspect the generated file and adjust accessor names.** `*AsNumpy().tobytes()` requires numpy (a flatbuffers transitive dep for vectors); if numpy is undesirable, iterate `Topic(i)` over `TopicLength()` into a `bytearray` instead — prefer this to avoid a numpy dependency:
> ```python
> def _vec(get, length) -> bytes:
>     return bytes(get(i) for i in range(length))
> ```

**Step 7: Run to verify pass**

Run: `uv run pytest tests/test_envelope.py -v`
Expected: PASS (all four tests). If `decode` offsets are wrong, the golden test fails loudly — re-check `.fbs` field order against the layout table.

**Step 8: Commit**

```bash
git add -A
git commit -m "feat: FlatBuffers envelope codec with golden-byte tests"
```

---

## Task 3: Records layer (Record, Topic, payload parsing)

**Files:**
- Create: `fleet_telemetry/records.py`
- Test: `tests/test_records.py`

**Step 1: Write the failing test `tests/test_records.py`**

```python
from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from fleet_telemetry.proto import vehicle_data_pb2 as vd
from fleet_telemetry.records import Record, Topic, parse_record


def _stream_payload() -> bytes:
    return vd.Payload(
        vin="5YJ3E1EA7JF000001",
        created_at=Timestamp(seconds=1700000000),
        data=[
            vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value(float_value=42.5)),
            vd.Datum(key=vd.Field.Gear, value=vd.Value(shift_state_value=vd.ShiftState.ShiftStateDrive)),
        ],
    ).SerializeToString()


def test_parse_data_record():
    rec = parse_record(vin="5YJ...", topic=b"V", txid=b"t1",
                        created_at=1700000000, payload=_stream_payload())
    assert rec.topic is Topic.DATA
    assert rec.vin == "5YJ..."
    assert isinstance(rec.message, vd.Payload)
    assert isinstance(rec.created_at, datetime)


def test_data_record_fields():
    rec = parse_record(vin="5YJ...", topic=b"V", txid=b"t1",
                       created_at=1700000000, payload=_stream_payload())
    fields = rec.fields()
    assert fields["VehicleSpeed"] == 42.5
    assert "Gear" in fields


def test_unknown_topic_raises():
    import pytest
    with pytest.raises(ValueError):
        parse_record(vin="v", topic=b"nope", txid=b"t", created_at=0, payload=b"")
```

> Confirm the `Field`/`ShiftState` enum member names against the generated `.pyi` and adjust.

**Step 2: Run to verify fail**

Run: `uv run pytest tests/test_records.py -v` → FAIL (no `records` module).

**Step 3: Write `fleet_telemetry/records.py`**

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum

from fleet_telemetry.proto import vehicle_data_pb2 as _vd
from fleet_telemetry.proto import vehicle_alert_pb2 as _va
from fleet_telemetry.proto import vehicle_error_pb2 as _ve
from fleet_telemetry.proto import vehicle_connectivity_pb2 as _vc


class Topic(StrEnum):
    DATA = "V"
    ALERTS = "alerts"
    ERRORS = "errors"
    CONNECTIVITY = "connectivity"


_PARSERS = {
    Topic.DATA: _vd.Payload,
    Topic.ALERTS: _va.VehicleAlerts,
    Topic.ERRORS: _ve.VehicleErrors,
    Topic.CONNECTIVITY: _vc.VehicleConnectivity,
}

Message = _vd.Payload | _va.VehicleAlerts | _ve.VehicleErrors | _vc.VehicleConnectivity


@dataclass(frozen=True, slots=True)
class Record:
    vin: str
    topic: Topic
    created_at: datetime
    txid: str
    message: Message
    raw: bytes

    def fields(self) -> dict[str, object]:
        """For DATA records: signal-name -> extracted value. Empty for other topics."""
        if self.topic is not Topic.DATA:
            return {}
        out: dict[str, object] = {}
        for datum in self.message.data:  # type: ignore[union-attr]
            name = _vd.Field.Name(datum.key)
            out[name] = _extract_value(datum.value)
        return out


def _extract_value(value: _vd.Value) -> object:
    field = value.WhichOneof("value")
    if field is None:
        return None
    return getattr(value, field)


def parse_record(*, vin: str, topic: bytes, txid: bytes,
                 created_at: int, payload: bytes) -> Record:
    try:
        topic_enum = Topic(topic.decode())
    except ValueError as exc:
        raise ValueError(f"unknown topic: {topic!r}") from exc
    message = _PARSERS[topic_enum]()
    message.ParseFromString(payload)
    return Record(
        vin=vin,
        topic=topic_enum,
        created_at=datetime.fromtimestamp(created_at, tz=timezone.utc),
        txid=txid.decode(errors="replace"),
        message=message,
        raw=payload,
    )
```

> `_vd.Field.Name(...)` may yield names like `VehicleSpeed` or `Field_VehicleSpeed` depending on the proto's enum style. Check and, if prefixed, strip the `Field_` prefix in `fields()` for clean signal names.

**Step 4: Run** → `uv run pytest tests/test_records.py -v` PASS.

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: records layer with per-message parsing and fields()"
```

---

## Task 4: Dispatch layer (filtered listeners + async iterator)

**Files:**
- Create: `fleet_telemetry/dispatch.py`
- Test: `tests/test_dispatch.py`

**Step 1: Write the failing test `tests/test_dispatch.py`**

```python
import asyncio
from datetime import datetime, timezone

import pytest

from fleet_telemetry.dispatch import Dispatcher
from fleet_telemetry.records import Record, Topic


def _rec(vin="v1", topic=Topic.DATA, fields=None):
    from fleet_telemetry.proto import vehicle_data_pb2 as vd
    data = [vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value(float_value=1.0))]
    msg = vd.Payload(vin=vin, data=data)
    return Record(vin=vin, topic=topic, created_at=datetime.now(timezone.utc),
                  txid="t", message=msg, raw=b"")


async def test_callback_receives_matching_record():
    d = Dispatcher()
    seen = []
    d.add_listener(lambda r: seen.append(r.vin), topic=Topic.DATA)
    await d.dispatch(_rec(vin="v1"))
    assert seen == ["v1"]


async def test_vin_filter():
    d = Dispatcher()
    seen = []
    d.add_listener(lambda r: seen.append(r.vin), vin="v2")
    await d.dispatch(_rec(vin="v1"))
    await d.dispatch(_rec(vin="v2"))
    assert seen == ["v2"]


async def test_field_filter():
    d = Dispatcher()
    seen = []
    d.add_listener(lambda r: seen.append(r), field="VehicleSpeed")
    d.add_listener(lambda r: seen.append(r), field="Soc")  # not present
    await d.dispatch(_rec())
    assert len(seen) == 1


async def test_async_callback_awaited():
    d = Dispatcher()
    seen = []
    async def cb(r): 
        await asyncio.sleep(0); seen.append(r.vin)
    d.add_listener(cb)
    await d.dispatch(_rec(vin="v9"))
    assert seen == ["v9"]


async def test_unsubscribe():
    d = Dispatcher()
    seen = []
    unsub = d.add_listener(lambda r: seen.append(r))
    unsub()
    await d.dispatch(_rec())
    assert seen == []


async def test_listener_exception_isolated():
    d = Dispatcher()
    seen = []
    d.add_listener(lambda r: (_ for _ in ()).throw(RuntimeError("boom")))
    d.add_listener(lambda r: seen.append(r.vin))
    await d.dispatch(_rec(vin="ok"))  # must not raise
    assert seen == ["ok"]


async def test_records_iterator():
    d = Dispatcher()
    async def produce():
        await asyncio.sleep(0)
        await d.dispatch(_rec(vin="a"))
        await d.dispatch(_rec(vin="b"))
    it = d.records()
    task = asyncio.create_task(produce())
    got = [(await anext(it)).vin, (await anext(it)).vin]
    await task
    assert got == ["a", "b"]
```

**Step 2: Run to verify fail** → FAIL (no `dispatch` module).

**Step 3: Write `fleet_telemetry/dispatch.py`**

```python
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from fleet_telemetry.records import Record, Topic

_LOGGER = logging.getLogger(__name__)

Listener = Callable[[Record], Awaitable[None] | None]


def _as_set(value: object | Iterable[object] | None) -> frozenset[Any] | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        return frozenset({value})
    return frozenset(value)


@dataclass(slots=True)
class _Registration:
    callback: Listener
    vins: frozenset[str] | None
    topics: frozenset[Topic] | None
    fields: frozenset[str] | None

    def matches(self, record: Record) -> bool:
        if self.vins is not None and record.vin not in self.vins:
            return False
        if self.topics is not None and record.topic not in self.topics:
            return False
        if self.fields is not None:
            if not (self.fields & record.fields().keys()):
                return False
        return True


class Dispatcher:
    def __init__(self, *, queue_maxsize: int = 1000) -> None:
        self._registrations: dict[object, _Registration] = {}
        self._queues: set[asyncio.Queue[Record]] = set()
        self._queue_maxsize = queue_maxsize

    def add_listener(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        topic: Topic | Iterable[Topic] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        key = object()
        self._registrations[key] = _Registration(
            callback=callback,
            vins=_as_set(vin),
            topics=_as_set(topic),
            fields=_as_set(field),
        )
        def unsubscribe() -> None:
            self._registrations.pop(key, None)
        return unsubscribe

    def on_data(self, cb: Listener, **kw: Any) -> Callable[[], None]:
        return self.add_listener(cb, topic=Topic.DATA, **kw)

    def on_alert(self, cb: Listener, **kw: Any) -> Callable[[], None]:
        return self.add_listener(cb, topic=Topic.ALERTS, **kw)

    def on_error(self, cb: Listener, **kw: Any) -> Callable[[], None]:
        return self.add_listener(cb, topic=Topic.ERRORS, **kw)

    def on_connectivity(self, cb: Listener, **kw: Any) -> Callable[[], None]:
        return self.add_listener(cb, topic=Topic.CONNECTIVITY, **kw)

    async def dispatch(self, record: Record) -> None:
        for reg in list(self._registrations.values()):
            if not reg.matches(record):
                continue
            try:
                result = reg.callback(record)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # noqa: BLE001 — isolate listener failures
                _LOGGER.exception("listener raised while handling %s record", record.topic)
        for queue in self._queues:
            if queue.full():
                try:
                    queue.get_nowait()  # drop oldest
                except asyncio.QueueEmpty:
                    pass
                _LOGGER.warning("records() queue full; dropped oldest record")
            queue.put_nowait(record)

    async def records(self) -> AsyncIterator[Record]:
        queue: asyncio.Queue[Record] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._queues.add(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            self._queues.discard(queue)
```

**Step 4: Run** → `uv run pytest tests/test_dispatch.py -v` PASS. Also `uv run pyright` clean.

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: filtered listener dispatch and records() iterator"
```

---

## Task 5: Identity extraction from client certificate

**Context:** VIN = `cert.subject.CN` with dots replaced by dashes (Go: `strings.Replace(CommonName, ".", "-", -1)`). The client type is derived from the issuer CN / OID; for our purposes we validate the cert is an authorized Tesla device cert and extract the VIN. Keep the authorized-issuer set minimal but faithful.

**Files:**
- Create: `fleet_telemetry/identity.py`
- Test: `tests/test_identity.py`

**Step 1: Write the failing test `tests/test_identity.py`**

```python
import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from fleet_telemetry.identity import UnauthorizedCertificate, identity_from_cert


def _make_cert(common_name: str, issuer_cn: str) -> x509.Certificate:
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_cn)])
    now = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    return (
        x509.CertificateBuilder()
        .subject_name(subject).issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, __import__("cryptography.hazmat.primitives.hashes", fromlist=["SHA256"]).SHA256())
    )


def test_vin_from_cn_dots_to_dashes():
    cert = _make_cert("5YJ3E1EA7JF000001", "Tesla Issuing CA")
    ident = identity_from_cert(cert)
    assert ident.vin == "5YJ3E1EA7JF000001"


def test_dotted_cn_normalized():
    cert = _make_cert("device.abc.123", "TeslaMotors")
    ident = identity_from_cert(cert)
    assert ident.vin == "device-abc-123"


def test_unauthorized_issuer_rejected():
    cert = _make_cert("5YJ...", "Some Random CA")
    with pytest.raises(UnauthorizedCertificate):
        identity_from_cert(cert)
```

**Step 2: Run to verify fail** → FAIL.

**Step 3: Write `fleet_telemetry/identity.py`**

Port the authorized-issuer sets from `messages/identity.go`.

```python
from __future__ import annotations

from dataclasses import dataclass

from cryptography import x509
from cryptography.x509.oid import NameOID

_KNOWN_ISSUERS = {
    "TeslaMotors": "vehicle_device",
    "Tesla Issuing CA": "vehicle_device",
    "Tesla Motors Products CA": "vehicle_device",
}

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


class UnauthorizedCertificate(ValueError):
    """The client certificate was not issued by a recognized Tesla CA."""


@dataclass(frozen=True, slots=True)
class Identity:
    vin: str
    client_type: str


def _cn(name: x509.Name) -> str:
    values = name.get_attributes_for_oid(NameOID.COMMON_NAME)
    return values[0].value if values else ""  # type: ignore[return-value]


def identity_from_cert(cert: x509.Certificate) -> Identity:
    vin = _cn(cert.subject).replace(".", "-")
    issuer_cn = _cn(cert.issuer)
    if issuer_cn in _KNOWN_OID_ISSUERS:
        return Identity(vin=vin, client_type="vehicle_device")
    client_type = _KNOWN_ISSUERS.get(issuer_cn)
    if client_type is None:
        raise UnauthorizedCertificate(f"unrecognized issuer: {issuer_cn!r}")
    return Identity(vin=vin, client_type=client_type)
```

> The OID-based `client_type` refinement (`createIdentifyFromOID`) is elided as YAGNI; we only need the VIN and an authorized/unauthorized decision. Revisit if energy/das device types must be distinguished.

**Step 4: Run** → `uv run pytest tests/test_identity.py -v` PASS.

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: VIN identity extraction from client certificate"
```

---

## Task 6: TLS server, connection registry & lifecycle

**Context:** The public `TelemetryServer`. Uses `aiohttp` for the WebSocket upgrade and an `ssl.SSLContext` (supplied by the caller, `CERT_REQUIRED`) for mTLS. On connect: extract the peer cert, derive identity, register the connection, synthesize a `CONNECTIVITY: CONNECTED` record. Per frame: decode envelope, parse record, **ack immediately**, dispatch. On disconnect: deregister, synthesize `CONNECTIVITY: DISCONNECTED`.

**Files:**
- Create: `fleet_telemetry/server.py`
- Modify: `fleet_telemetry/__init__.py` (public exports)
- Test: `tests/test_server.py`

**Step 1: Write `fleet_telemetry/server.py`**

```python
from __future__ import annotations

import logging
import ssl
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType

from aiohttp import WSMsgType, web
from cryptography import x509

from fleet_telemetry import _envelope
from fleet_telemetry.dispatch import Dispatcher, Listener
from fleet_telemetry.identity import UnauthorizedCertificate, identity_from_cert
from fleet_telemetry.proto import vehicle_connectivity_pb2 as _vc
from fleet_telemetry.records import Record, Topic, parse_record

_LOGGER = logging.getLogger(__name__)
SIZE_LIMIT = 1_000_000  # 1 MB, matches Go SizeLimit


@dataclass(frozen=True, slots=True)
class Connection:
    vin: str
    connected_at: datetime
    peer: str
    client_version: str | None


class TelemetryServer:
    def __init__(self, *, ssl_context: ssl.SSLContext,
                 host: str = "0.0.0.0", port: int = 443) -> None:
        self._ssl_context = ssl_context
        self._host = host
        self._port = port
        self._dispatcher = Dispatcher()
        self._connections: dict[str, Connection] = {}
        self._app = web.Application()
        self._app.router.add_get("/", self._handle)
        self._runner: web.AppRunner | None = None

    # ---- listener API (delegates to Dispatcher) ----
    def add_listener(self, cb: Listener, *, vin: str | Iterable[str] | None = None,
                     topic: Topic | Iterable[Topic] | None = None,
                     field: str | Iterable[str] | None = None) -> Callable[[], None]:
        return self._dispatcher.add_listener(cb, vin=vin, topic=topic, field=field)

    def on_data(self, cb: Listener, **kw: object) -> Callable[[], None]:
        return self._dispatcher.on_data(cb, **kw)  # type: ignore[arg-type]

    def on_alert(self, cb: Listener, **kw: object) -> Callable[[], None]:
        return self._dispatcher.on_alert(cb, **kw)  # type: ignore[arg-type]

    def on_error(self, cb: Listener, **kw: object) -> Callable[[], None]:
        return self._dispatcher.on_error(cb, **kw)  # type: ignore[arg-type]

    def on_connectivity(self, cb: Listener, **kw: object) -> Callable[[], None]:
        return self._dispatcher.on_connectivity(cb, **kw)  # type: ignore[arg-type]

    def records(self):
        return self._dispatcher.records()

    # ---- connection state ----
    @property
    def connections(self) -> dict[str, Connection]:
        return dict(self._connections)

    def is_connected(self, vin: str) -> bool:
        return vin in self._connections

    # ---- lifecycle ----
    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port,
                           ssl_context=self._ssl_context)
        await site.start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def __aenter__(self) -> "TelemetryServer":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # ---- request handling ----
    async def _handle(self, request: web.Request) -> web.StreamResponse:
        peercert = request.transport.get_extra_info("peercert") if request.transport else None
        ssl_object = request.transport.get_extra_info("ssl_object") if request.transport else None
        cert = _peer_cert(ssl_object)
        if cert is None:
            return web.Response(status=496, text="missing client certificate")
        try:
            ident = identity_from_cert(cert)
        except UnauthorizedCertificate:
            return web.Response(status=496, text="unauthorized client certificate")

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        peer = request.remote or ""
        client_version = request.headers.get("Version")
        await self._on_connect(ident.vin, peer, client_version)
        try:
            async for msg in ws:
                if msg.type is not WSMsgType.BINARY:
                    continue
                if len(msg.data) > SIZE_LIMIT:
                    _LOGGER.warning("frame over size limit from %s", ident.vin)
                    continue
                await self._process_frame(ws, ident.vin, msg.data)
        finally:
            await self._on_disconnect(ident.vin)
        return ws

    async def _process_frame(self, ws: web.WebSocketResponse, vin: str, data: bytes) -> None:
        try:
            frame = _envelope.decode(data)
        except _envelope.EnvelopeError:
            _LOGGER.warning("undecodable frame from %s", vin)
            return
        # Ack immediately (reliable_ack=false semantics).
        try:
            await ws.send_bytes(_envelope.encode_ack(
                txid=frame.txid, topic=frame.topic, message_id=frame.message_id))
        except ConnectionError:
            return
        try:
            record = parse_record(vin=vin, topic=frame.topic, txid=frame.txid,
                                  created_at=frame.created_at, payload=frame.payload)
        except (ValueError, Exception):  # noqa: BLE001
            _LOGGER.exception("failed to parse payload from %s", vin)
            return
        await self._dispatcher.dispatch(record)

    async def _on_connect(self, vin: str, peer: str, client_version: str | None) -> None:
        now = datetime.now(timezone.utc)
        self._connections[vin] = Connection(vin, now, peer, client_version)
        await self._emit_connectivity(vin, connected=True)

    async def _on_disconnect(self, vin: str) -> None:
        self._connections.pop(vin, None)
        await self._emit_connectivity(vin, connected=False)

    async def _emit_connectivity(self, vin: str, *, connected: bool) -> None:
        msg = _vc.VehicleConnectivity(vin=vin)
        status = getattr(_vc.ConnectivityEvent, "CONNECTED" if connected else "DISCONNECTED", None)
        if status is not None and hasattr(msg, "status"):
            msg.status = status  # field name per generated proto — verify
        record = Record(vin=vin, topic=Topic.CONNECTIVITY,
                        created_at=datetime.now(timezone.utc), txid="",
                        message=msg, raw=b"")
        await self._dispatcher.dispatch(record)


def _peer_cert(ssl_object: ssl.SSLObject | None) -> x509.Certificate | None:
    if ssl_object is None:
        return None
    der = ssl_object.getpeercert(binary_form=True)
    if not der:
        return None
    return x509.load_der_x509_certificate(der)
```

> Inspect `vehicle_connectivity_pb2.pyi` for the real `VehicleConnectivity` field names (the Go builder sets `Vin`, `ConnectionId`, `NetworkInterface`, `CreatedAt`, `Status`). Adjust `_emit_connectivity` to the actual generated API. Keep it minimal — VIN + status is enough for the first pass.

**Step 2: Update `fleet_telemetry/__init__.py`**

```python
from fleet_telemetry.records import Record, Topic
from fleet_telemetry.server import Connection, TelemetryServer

__all__ = ["TelemetryServer", "Connection", "Record", "Topic"]
```

**Step 3: Write the integration test `tests/test_server.py`**

Generate a throwaway CA + server cert + client cert (issuer CN `Tesla Issuing CA`), start the server on `127.0.0.1:0`, connect a real `aiohttp` client over TLS presenting the client cert, send the golden Stream frame, assert (a) an ack is received, (b) an `on_data` listener fired with the right VIN, (c) `is_connected(vin)` was true during the session and false after.

```python
import asyncio
import datetime
import ssl

import pytest
from aiohttp import ClientSession, TCPConnector
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from fleet_telemetry import TelemetryServer, Topic
from tests.fixtures.golden import GOLDEN_STREAM


def _keypair():
    return ec.generate_private_key(ec.SECP256R1())


def _cert(subject_cn, issuer_cn, key, issuer_key, *, ca=False, san=None):
    now = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject_cn)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer_cn)]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=3650))
    )
    if ca:
        builder = builder.add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
    if san:
        builder = builder.add_extension(x509.SubjectAlternativeName([x509.DNSName(san)]), critical=False)
    return builder.sign(issuer_key, hashes.SHA256())


# Full fixture wiring (write files to tmp_path, build SSLContexts) omitted here for brevity —
# implement per aiohttp TLS testing docs. Key assertions below.

async def test_end_to_end_stream(tmp_path):
    # ... build CA, server cert (SAN=localhost), client cert (issuer "Tesla Issuing CA",
    #     subject CN "5YJ3E1EA7JF000001"), server_ctx (CERT_REQUIRED, load CA),
    #     client_ctx (load CA as trust + client cert/key) ...
    server = TelemetryServer(ssl_context=server_ctx, host="127.0.0.1", port=0)
    # NOTE: port=0 → read actual bound port from the runner's site; expose a test helper
    #       or bind an explicit free port.
    got = asyncio.Event()
    seen = []
    server.on_data(lambda r: (seen.append(r.vin), got.set()))
    async with server:
        # connect client, send GOLDEN_STREAM, await ack + got.set()
        ...
    assert seen == ["5YJ3E1EA7JF000001"]
```

> This test needs care around binding a known port and reading the peer cert through aiohttp. If `port=0` dynamic binding is awkward, bind an explicit free port via `socket`. Budget time here; it is the most involved test. If real-TLS proves too fiddly for CI, split into: (a) a unit test of `_process_frame` with a fake `ws` object (fast, no TLS), and (b) a slower TLS smoke test marked `@pytest.mark.integration`.

**Step 4: Run** → `uv run pytest tests/test_server.py -v` PASS; `uv run pyright` clean; `uv run ruff check .` clean.

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: TLS telemetry server with connection registry and lifecycle"
```

---

## Task 7: README, example, and final polish

**Files:**
- Create: `README.md`
- Create: `examples/basic.py`
- Modify: `pyproject.toml` (bump to Beta if all green)

**Step 1: Write `README.md`** — install, the mTLS/cert prerequisites, a minimal usage example mirroring the design doc's Public API, and a clear statement that config/registration is handled by `python-tesla-fleet-api`.

**Step 2: Write `examples/basic.py`**

```python
import asyncio
import ssl

from fleet_telemetry import TelemetryServer, Topic


def build_ssl_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain("server-cert.pem", "server-key.pem")
    ctx.load_verify_locations("tesla-ca.pem")
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def main() -> None:
    server = TelemetryServer(ssl_context=build_ssl_context(), port=443)

    server.on_data(lambda r: print(r.vin, r.fields()))
    server.on_connectivity(lambda r: print("connectivity", r.vin))

    async with server:
        print("listening on :443")
        await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
```

**Step 3: Full green check**

Run:
```bash
uv run pytest -v
uv run ruff check .
uv run pyright
```
Expected: all pass.

**Step 4: Commit**

```bash
git add -A
git commit -m "docs: README, example, and polish"
```

---

## Verification checklist (run before declaring done)

- [ ] `uv run pytest -v` — all tests pass, including the golden-byte envelope tests and the end-to-end TLS test.
- [ ] `uv run ruff check .` — clean.
- [ ] `uv run pyright` — 0 errors (strict).
- [ ] `./proto/generate.sh` and `./fbs/generate.sh` reproduce the committed generated files.
- [ ] The golden Stream frame decodes to the expected VIN/topic/payload; the golden Ack decodes as a StreamAck.
- [ ] `examples/basic.py` imports and type-checks.

## Known follow-ups (out of scope for this plan)

- Typed per-signal `listen_<Signal>` methods generated from protobuf field types, hosted on a `server.vehicle(vin)` object (design doc "Typing & end-state vision").
- OID-based `client_type` refinement for energy/das/robotics devices.
- Configurable reliable-ack mode (ack after listeners succeed).
- Backpressure policy configuration for `records()`.
