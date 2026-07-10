# python-fleet-telemetry — Design

**Date:** 2026-07-10
**Status:** Approved design, pending implementation

## Purpose

A greenfield asynchronous Python library that **receives** Tesla vehicle telemetry
directly from vehicles — the same role played by the Go
[`tesla-fleet-telemetry`](https://github.com/teslamotors/fleet-telemetry) server —
and delivers parsed records to consumers through **event listeners** instead of
Kafka/Kinesis producers.

It is the mirror image of the sibling consumer libraries
(`python-tesla-fleet-api`, `python-teslemetry-stream`): those consume Teslemetry's
*relay* (which has already unwrapped the vehicle connection); this library **is** the
server that terminates the raw vehicle connection.

Targets **Python 3.13+**, matching Home Assistant and the sibling libraries.

## Scope

**In scope** — the two halves of the Go reference:

1. **Connection handling** — an asyncio WebSocket server that terminates **mTLS
   in-process**, verifies each vehicle's client certificate against Tesla's CA, and
   derives the VIN from the certificate's Subject CommonName.
2. **Data parsing** — decode the FlatBuffers `Envelope` → `Stream` transport frame,
   then the protobuf payload (`VehicleData` / `VehicleAlerts` / `VehicleErrors`), and
   acknowledge each frame with a FlatBuffers `StreamAck`.

**Out of scope** (handled by `python-tesla-fleet-api` / operator tooling):

- App registration, partner tokens, virtual-key pairing.
- Pushing `fleet_telemetry_config` (selecting fields/intervals) to vehicles.
- Certificate/CA issuance.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| TLS boundary | **Library terminates mTLS itself** | Self-contained; VIN from verified peer cert. |
| Event granularity | **Per-message record** | One event per WebSocket frame; close to the Go `Record` model. |
| Delivery API | **Callbacks (primary) + async-iterator adapter** | Callbacks match HA's listener idiom; iterator for pipeline use. |
| Listener model | **One flexible `add_listener` filtered by vin / topic / field** | No per-vehicle class; "flexibility is key". |
| Connection state | **Accessor (`server.connections`, `is_connected`) + `on_connectivity` events** | The only real server-held state; exposed without a Vehicle object graph. |
| Payload codec | **protobuf ≥ 6.32.0**, reusing shipped `*_pb2.py` | Matches every sibling library. |
| Envelope codec | **`flatbuffers` dependency + flatc-generated bindings**, isolated behind an internal interface | Official toolchain now; swappable for a hand-rolled codec if the dep bites. |
| Ack mode | **Ack immediately on parse** (Go `reliable_ack=false`) | Listener errors never block the vehicle. |
| Python | **3.13+** | HA + sibling parity. |
| Tooling | `uv`, `ruff`, `pyright` strict, `setuptools`, `py.typed` | Sibling parity. |
| WebSocket transport | **`aiohttp`** | Always preferred with Home Assistant; composes `ssl.SSLContext` + upgrade cleanly. |

## Architecture

```
                    ┌─────────────────────────────────────┐
  vehicle ──mTLS──► │  TelemetryServer (asyncio + ssl)     │
   (wss)            │   • SSLContext: verify peer cert      │
                    │   • VIN ← cert.subject.CN             │
                    │   • one task per connection           │
                    │   • connection registry (state)       │
                    └───────────────┬─────────────────────┘
                                    │ raw frame bytes
                    ┌───────────────▼─────────────────────┐
                    │  _envelope   (swappable backend)     │
                    │   decode() → (topic, txid, payload)  │
                    │   encode_ack(txid, topic) → bytes    │
                    │   [flatbuffers/flatc under the hood] │
                    └───────────────┬─────────────────────┘
                                    │ topic + payload bytes
                    ┌───────────────▼─────────────────────┐
                    │  records: parse payload via *_pb2     │
                    │   → Record(vin, topic, created_at,    │
                    │            txid, message, raw)        │
                    └───────────────┬─────────────────────┘
                                    │
                    ┌───────────────▼─────────────────────┐
                    │  dispatch: filtered listeners         │
                    │   add_listener(cb, vin=, topic=,      │
                    │                field=)               │
                    │   + records() async iterator          │
                    └──────────────────────────────────────┘
```

### Envelope isolation (escape hatch)

The FlatBuffers concern is confined to a single internal module with a narrow
interface:

```python
# fleet_telemetry/_envelope.py
def decode(frame: bytes) -> StreamFrame: ...          # topic, txid, sender_id, device_id, created_at, payload
def encode_ack(txid: bytes, topic: bytes) -> bytes: ...
```

The default backend uses `flatbuffers` + flatc-generated bindings. If the dependency
causes problems (packaging, HA review, version drift), this module can be replaced by
a hand-rolled vtable codec with **no change to any other layer**. Golden-byte tests
(below) pin the behavior across either backend.

## Public API

```python
from fleet_telemetry import TelemetryServer, Topic, Record

server = TelemetryServer(
    ssl_context=ctx,        # caller supplies mTLS context (cert, key, CA verify)
    host="0.0.0.0",
    port=443,
)

# ── flexible filtered listener (the one primitive) ─────────────────────────
Listener = Callable[[Record], Awaitable[None] | None]

unsub = server.add_listener(
    cb,
    vin="5YJ...",                    # str | Iterable[str] | None
    topic=Topic.DATA,               # Topic | Iterable[Topic] | None
    field=("VehicleSpeed", "Soc"),  # str | Iterable[str] | None — fires if record has any
)
unsub()  # detach

# ── thin convenience wrappers = add_listener with a preset filter ──────────
server.on_data(cb)          # topic=DATA
server.on_alert(cb)         # topic=ALERTS
server.on_error(cb)         # topic=ERRORS
server.on_connectivity(cb)  # topic=CONNECTIVITY (server-synthesized)

# ── iterator view over all records ────────────────────────────────────────
async for record in server.records():
    ...

# ── connection state (accessor, not an object graph) ──────────────────────
server.connections            # Mapping[str, Connection]
server.is_connected(vin)      # bool

# ── lifecycle ─────────────────────────────────────────────────────────────
async with server:            # start on enter, graceful stop on exit
    await server.serve_forever()
```

### Data model

```python
class Topic(StrEnum):
    DATA = "V"
    ALERTS = "alerts"
    ERRORS = "errors"
    CONNECTIVITY = "connectivity"

@dataclass(frozen=True, slots=True)
class Record:
    vin: str
    topic: Topic
    created_at: datetime            # from Stream.created_at
    txid: str
    message: VehicleData | VehicleAlerts | VehicleErrors | VehicleConnectivity
    raw: bytes                      # original protobuf payload bytes

    def fields(self) -> dict[str, object]:
        """For DATA records: signal-name -> value (drives field= filtering)."""

@dataclass(frozen=True, slots=True)
class Connection:
    vin: str
    connected_at: datetime
    peer: str                       # remote address
    client_version: str | None      # from the `Version` header
```

`field=` is a **predicate** deciding whether the callback fires; the callback still
receives the whole per-message `Record`. Connectivity records are synthesized by the
server on connect/disconnect (mirroring the Go `dispatchConnectivityEvent`), not sent
by the vehicle.

## Connection lifecycle & acks

1. TLS handshake; reject if the client cert fails CA verification.
2. Extract VIN from `cert.subject.CN`; reject if malformed.
3. Register the connection; synthesize a `CONNECTIVITY: CONNECTED` record.
4. Per frame:
   a. `_envelope.decode()` → topic, txid, payload.
   b. Parse payload via the matching `*_pb2` message.
   c. **Send `StreamAck` immediately** (`reliable_ack=false` semantics).
   d. Build `Record`; dispatch to matching listeners and the iterator queue.
5. On close/error: deregister; synthesize a `CONNECTIVITY: DISCONNECTED` record.

Size limit: reject frames > 1 MB (Go `SizeLimit`).

## Error handling

- **Malformed frame / unknown topic** — log, do not crash the connection; optionally
  emit a decode-error event (config flag). Mirrors Go `guessError`.
- **Sender/VIN mismatch** — log once per connection (Go `ShouldLogVinMismatch`), then
  ignore to avoid log floods.
- **Listener exception** — isolated per listener; logged, never propagated to the
  vehicle or other listeners (acks already sent).
- **Backpressure on `records()`** — bounded queue; documented drop-oldest or block
  policy (default: drop-oldest with a warning counter).

## Testing strategy

- **Golden-byte frames** — capture real frames encoded by the Go implementation
  (or `flatc`) and assert round-trip decode/ack equality. These pin the wire format
  independent of the envelope backend, protecting the swap escape hatch.
- **Protobuf fixtures** — decode representative `VehicleData` / `VehicleAlerts` /
  `VehicleErrors` payloads; assert `Record.fields()`.
- **TLS** — spin up the server with a test CA; assert a valid client cert connects and
  yields the right VIN, and an untrusted/malformed cert is rejected.
- **Listener filtering** — matrix over vin / topic / field filters, including
  combinations and unsubscribe.
- **Lifecycle** — connectivity events on connect/disconnect; `connections` accuracy.
- `pytest` + `pytest-asyncio`; `pyright` strict; `ruff` clean.

## Package layout

```
fleet_telemetry/
  __init__.py            # TelemetryServer, Topic, Record, Connection exports
  server.py              # asyncio + aiohttp + ssl; connection registry
  records.py             # Record, payload parsing, Topic
  dispatch.py            # listener registry + filtering + records() iterator
  _envelope.py           # narrow decode()/encode_ack() interface (swappable)
  _flatbuffers/          # flatc-generated bindings (default backend)
  proto/                 # vendored *_pb2.py + regeneration script
  py.typed
tests/
docs/plans/
pyproject.toml           # uv, ruff, pyright-strict, setuptools
```

## Dependencies

```toml
requires-python = ">=3.13"
dependencies = [
    "aiohttp>=3",
    "protobuf>=6.32.0",
    "flatbuffers>=24",
]
[dependency-groups]
dev = ["pyright>=1.1", "pytest>=8", "pytest-asyncio", "ruff>=0.14"]
```

## Typing & end-state vision

Protobuf decoding gives us type information we should preserve for consumers **as far
as the API shape allows**:

- **First pass (per-message records).** `Record.message` is the concrete decoded
  protobuf message, so message-level typing is preserved. But `Record.fields()` for a
  `DATA` record flattens the `Datum` `oneof` (string / int / float / bool / enum /
  location …) to `dict[str, object]` — the *per-signal* type is erased at that surface.
  A consumer subscribing with `field="VehicleSpeed"` still gets the whole record and
  must read the value as `object`.
- **End state (typed per-signal listeners).** The only way to hand a consumer a
  precisely-typed value (`VehicleSpeed -> float`, `Gear -> enum`, `Location ->
  LatLng`) is a per-signal typed callback:
  `listen_VehicleSpeed(cb: Callable[[float], ...])`. These are generated from the
  protobuf field types.
- **This is where a `Vehicle` class earns its place.** In `teslemetry_stream` the
  per-signal `listen_<Signal>` methods live on the `Vehicle` object; that same shape
  gives a `Vehicle` here real value as the typed namespace holding the generated
  listeners, layered over the `add_listener(field=...)` primitive. Not needed for the
  first pass — but the intended complete-vision end state.

## Open follow-ups (non-blocking)

- End-state: generate typed per-signal `listen_<Signal>` methods from protobuf field
  types, hosted on a `server.vehicle(vin)` object (see above). First pass ships the
  flexible `add_listener` primitive only.
