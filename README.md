# python-fleet-telemetry

An async Python library that receives [Tesla Fleet Telemetry](https://github.com/teslamotors/fleet-telemetry)
**directly from vehicles**. It terminates the mutual-TLS WebSocket connection
in-process, decodes the FlatBuffers transport envelope and the protobuf payloads,
and exposes each frame as a typed `Record` through event listeners or an async
record stream.

This is the *receive side* — the mirror image of the Teslemetry relay consumed by
[`python-teslemetry-stream`](https://github.com/Teslemetry/python-teslemetry-stream).
Where that library is a client of a relay, this library **is** the server that
vehicles connect to. Python 3.13+.

## How it fits

Tesla vehicles can be configured to stream telemetry over an mTLS WebSocket to a
server you run. The reference implementation is the Go
[`tesla-fleet-telemetry`](https://github.com/teslamotors/fleet-telemetry) server.
`python-fleet-telemetry` plays the same role: it is the endpoint vehicles connect
to, not a consumer of a downstream message bus.

**In scope:** accepting vehicle connections, terminating mTLS, deriving the VIN
from the verified client certificate, decoding envelopes and payloads, tracking
live connections, and delivering records to listeners and streams.

**Out of scope** (use [`python-tesla-fleet-api`](https://github.com/Teslemetry/python-tesla-fleet-api)):
application registration, partner tokens, virtual-key pairing, pushing the
`fleet_telemetry_config` that tells vehicles where to stream, and issuing the
TLS/CA material. This library assumes those are already in place.

## Install

```sh
pip install python-fleet-telemetry
```

> Not yet published to PyPI — this is the intended install command.

Runtime dependencies: `aiohttp`, `protobuf>=6.32`, `flatbuffers`, `cryptography`.
Requires Python 3.13+.

## Prerequisites

Before this server can receive anything:

- Your vehicles must be configured (via the Fleet API — out of scope here, see
  `python-tesla-fleet-api`) to stream to your server's fully-qualified domain
  name.
- You need a **TLS server certificate** for that FQDN and its private key.
- You need the **Tesla CA bundle** used to verify the client certificates the
  vehicles present.

**mTLS is mandatory.** The VIN of each connection is derived from the verified
client certificate's subject Common Name — there is no other authentication path.
Client certificates must be issued by a recognized Tesla CA; a certificate from
an unrecognized issuer is rejected with HTTP `496` *before* the WebSocket upgrade.
The `TelemetryServer` constructor enforces this by requiring
`ssl_context.verify_mode == ssl.CERT_REQUIRED`, raising `ValueError` otherwise.

## Quickstart

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

    # Callback style
    server.on_data(lambda r: print(r.vin, r.fields()))
    server.on_connectivity(lambda r: print("connectivity:", r.vin, r.topic))

    async with server:
        print("listening for vehicles on :443")

        # Stream style: iterate records as they arrive
        async with server.records() as stream:
            async for record in stream:
                print(record.vin, record.topic, record.created_at)


if __name__ == "__main__":
    asyncio.run(main())
```

A complete version is in [`examples/basic.py`](examples/basic.py).

The server is an async context manager (`async with server:` starts on enter and
stops gracefully on exit); you can also call `await server.start()` /
`await server.stop()` directly.

Callbacks may be **sync or async** and each receives a single `Record`. Listener
exceptions are isolated: a raising or hanging listener never blocks the vehicle
(the frame is acked before any listener runs) nor the other listeners. Every
registration method returns an **unsubscribe callable**.

`server.records()` returns an independent async iterator; multiple concurrent
iterators each receive every record. Its queue is bounded (`queue_maxsize`,
default 1000) and drops the oldest record under backpressure, so a slow consumer
can never stall ingestion.

## Filtering

`add_listener` filters across three independent dimensions — VIN, topic, and
signal name (`field`) — combined with **AND** semantics. Each accepts a single
value, an iterable of values, or `None` (no constraint on that dimension):

```python
from fleet_telemetry import Topic

server.add_listener(
    lambda r: print(r.vin, r.fields()["VehicleSpeed"]),
    vin="LRW...123",
    topic=Topic.DATA,
    field="VehicleSpeed",
)
```

A `field=` constraint fires when the record's `fields()` **contains** that signal
name. An empty iterable (e.g. `vin=[]`) is a constraint nothing can satisfy, so
the listener never fires.

The topic-preset helpers `on_data`, `on_alert`, `on_error`, and
`on_connectivity` are thin wrappers over `add_listener` with `topic` fixed; they
accept `vin` and `field`.

Live connection state is also available: `server.connections` returns a
`dict[str, Connection]` snapshot keyed by VIN, and `server.is_connected(vin)`
returns a bool.

## Record shape

Every WebSocket frame becomes exactly one frozen `Record` (never one per signal):

| Attribute | Type | Notes |
| --- | --- | --- |
| `vin` | `str` | Derived from the verified client certificate |
| `topic` | `Topic` | `DATA` / `ALERTS` / `ERRORS` / `CONNECTIVITY` |
| `created_at` | `datetime` | Timezone-aware UTC |
| `txid` | `str` | Transport transaction id (empty for synthetic records) |
| `message` | protobuf message | `Payload` / `VehicleAlerts` / `VehicleErrors` / `VehicleConnectivity` |
| `raw` | `bytes` | Original payload bytes, for re-decode or verbatim forwarding |

`Record.fields()` returns a `dict[str, object]` mapping signal name to value —
**for DATA records only** (`{}` for every other topic). Values come from the
`Value` oneof set on each datum (`None` if unset).

**Forward compatibility:** the bundled protobuf schema is a point-in-time
snapshot. A vehicle on newer firmware can emit a signal this library doesn't yet
know by name; rather than raising, such signals appear in `fields()` under the
synthetic key `Field_<int>` (the raw enum number). If a payload repeats a field
key, the last occurrence wins.

`Topic` is a `StrEnum`: `DATA="V"`, `ALERTS="alerts"`, `ERRORS="errors"`,
`CONNECTIVITY="connectivity"`. **Connectivity records are synthesized** by the
server as vehicles connect and disconnect (with a `CONNECTED` / `DISCONNECTED`
`ConnectivityEvent` status), in addition to any connectivity frames a vehicle
sends itself.

`Connection` is frozen with `.vin`, `.connected_at` (tz-aware UTC), `.peer`
(remote address), and `.client_version` (from the connection's `Version` header,
may be `None`).

## Known limitations and hardening

This library handles connection termination and parsing; it does **not** yet
include production abuse protections. Be honest with yourself about your threat
model before exposing it to the open internet:

- **No built-in rate limiting.** Frames are acked and dispatched with no
  throttle.
- **No per-VIN connection cap.**
- **No idle / half-open connection timeout by default.** A silent or hostile peer
  holds a connection slot indefinitely.

The available knob is aiohttp's WebSocket `receive_timeout` / `heartbeat`, which
would drop silent peers — but enabling it needs a chosen policy, so it is **not**
turned on by default. For production fleets, front this server with
infrastructure-level protections (load balancer connection limits, WAF, network
ACLs, per-source rate limiting).

The bundled protobuf and FlatBuffers schemas are vendored from a point-in-time
Tesla reference and regenerated via `proto/generate.sh` and `fbs/generate.sh`.

## Development

```sh
uv sync
uv run pytest          # test suite
uv run ruff check .    # lint
uv run pyright         # type-check (strict)
```

Regenerate the vendored bindings after updating the schemas in `proto/` or
`fbs/`:

```sh
./proto/generate.sh    # protobuf bindings -> fleet_telemetry/proto/
./fbs/generate.sh      # FlatBuffers bindings -> fleet_telemetry/_flatbuffers/  (needs flatc)
```

## Roadmap

Possible future directions (not implemented today): typed per-signal listener
helpers (e.g. `listen_VehicleSpeed(...)`), and an opt-in idle-timeout policy.

## License

Apache-2.0.
