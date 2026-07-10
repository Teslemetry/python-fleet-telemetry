# Backlog

Outstanding work and future improvement ideas for `python-fleet-telemetry`.
Shipped state: v0.1.0-alpha (see `docs/plans/2026-07-10-*`). This file is the
running list; promote items into a dated plan before implementing.

---

## 1. End-to-end example (needs `tesla-fleet-api`)

A complete, runnable example that stands up the receive server AND configures a
real vehicle to stream to it — the missing half that `examples/basic.py` (server
only) doesn't cover.

- Use the sibling `python-tesla-fleet-api` to push `fleet_telemetry_config` to the
  vehicle (fields, intervals, the server hostname/port, and the `ca` chain the
  vehicle uses to verify the server — see §2).
- Poll `fleet_telemetry_config` until `synced == true`, then show records arriving
  through `on_data` / `server.records()`.
- Document the prerequisites the example cannot do for you: a developer app,
  partner token, virtual-key pairing, and a publicly reachable FQDN (see §3).
- Cross-link from the README once it exists.

## 2. Certificate tooling

### Three key/cert artifacts — independent (confirm empirically)

Easy to conflate; they share *purpose overlap*, not keys:
1. **App / partner EC private key** (`prime256v1`, public half hosted at
   `/.well-known/appspecific/com.tesla.3p.public-key.pem`). Used for the partner
   token, virtual-key pairing, and **signing the `fleet_telemetry_config` delivery**
   to virtual-key vehicles (the JWT/command signing).
2. **Telemetry CA + server cert** (the `ca` field in `fleet_telemetry_config`).
   Self-signed is fine; the vehicle uses the CA's *public* cert to verify the
   server's TLS leaf.
3. **Vehicle client cert** — issued by Tesla, used for mTLS client auth.

The telemetry `ca` (#2) does **NOT** need to share a private key with the app key
(#1) or anything vehicle-side. The app key merely *signs the request that carries*
the `ca` (authorization to change vehicle config) — no cryptographic binding to the
CA's keypair. Reference: `fleet_telemetry_config_create` is a plain JSON POST
(`tesla-fleet-api .../vehicle/fleet.py:731`); `ca` is documented as "the full
certificate chain used to generate the server's TLS certificate."
- [ ] **Verify empirically**: register a telemetry config whose `ca` is a fresh,
      independent self-signed CA (unrelated to the app key) and confirm a vehicle
      connects and streams. Assumed true; untested.

### Tooling for the two trust directions

The mTLS model has **two independent trust directions**; tooling must serve both.

**Direction A — server verifies the vehicle (client-cert auth).**
The server's `SSLContext` needs `verify_mode = CERT_REQUIRED` + `load_verify_locations(<Tesla prod CA bundle>)`.
This bundle is **fixed and issued by Tesla** — its issuing-CA CNs are exactly the
allowlist in `fleet_telemetry/identity.py` (e.g. `Tesla Issuing CA`,
`Tesla Motors GF Austin Product Issuing CA`). The operator does NOT generate it.
Reference copy lives at `tesla-fleet-telemetry/config/files/prod_ca.crt` (26 KB;
`eng_ca.crt` for staging).
- [ ] **Ship the Tesla CA bundle** with the package (e.g. `fleet_telemetry/certs/tesla_prod_ca.pem`)
      and expose a helper like `fleet_telemetry.tesla_ca_bundle_path()` (or a
      `build_server_ssl_context(server_cert, server_key, *, staging=False)` convenience)
      so callers don't have to hunt for it. Decide on refresh policy — Tesla can
      rotate/add issuing CAs; keep it regenerable and versioned.

**Direction B — vehicle verifies the server (normal server cert).**
The server presents a TLS cert for its FQDN; the vehicle trusts it via the `ca`
field registered in `fleet_telemetry_config`. Operators can self-sign. This is the
helper set discussed:
- [ ] Helper 1: given a private key (or generating one), create a **self-signed CA
      cert**. Its PEM is what goes into `fleet_telemetry_config.ca`.
- [ ] Helper 2: given the CA cert + CA key, issue the **server (leaf) cert + key**
      for the FQDN, with `SubjectAltName` = the DNS name / IP the vehicle connects to.
- [ ] Decide implementation: pure-Python via `cryptography` (already a dep — no
      openssl needed, testable, cross-platform) **vs.** an `openssl` shell script.
      Lean `cryptography` for the library helpers; optionally also ship a
      `scripts/gen_certs.sh` for operators who prefer openssl. The Go reference
      ships `tools/check_server_cert.sh` to validate a server cert against a config —
      worth a Python equivalent or a doc pointer.

Open question: should cert generation live in THIS library, in `tesla-fleet-api`,
or a small shared helper? Direction-A (ship Tesla's bundle) clearly belongs here;
Direction-B (issue your own CA/leaf) is generic PKI and could live either place.

## 3. Home Assistant integration story (design — unresolved)

The hard part is **not** certs, it's **reachability**. Fleet Telemetry requires the
vehicle to open an outbound WSS to a stable, publicly-resolvable FQDN:port,
presenting a cert the vehicle trusts. A typical HA install is behind NAT with no
public FQDN and no inbound 443. Things to resolve before an HA integration:
- [ ] How does a home user expose a public endpoint? (public IP + DNS + port
      forward; a tunnel such as Cloudflare Tunnel; Nabu Casa Cloud — but does Cloud
      pass through raw client certs / arbitrary ports? likely not.) Client-cert mTLS
      and a fixed port make most managed tunnels unsuitable.
- [ ] Given the above, direct self-hosting is an **advanced** path; most HA users
      would still use the Teslemetry relay (`python-teslemetry-stream`). Position
      this library as the self-hosted/advanced option and document the requirements
      honestly.
- [ ] FQDN in the server cert (SAN) and the hostname in `fleet_telemetry_config`
      must match and be reachable by the vehicle. Capture the full checklist.
- [ ] Config/credential flow inside HA: storing the CA key, server cert, and Tesla
      CA bundle; renewing the leaf cert; re-pushing config on change.

## 4. Production hardening (documented as known limitations in README)

- [ ] No rate limiting, no per-VIN connection cap, no idle/half-open timeout. A
      silent/hostile peer holds a slot; frames are acked/dispatched with no throttle.
      Available knob: `receive_timeout` / `heartbeat` on the aiohttp
      `WebSocketResponse` (needs a chosen policy). Consider surfacing as ctor params.

## 5. Test coverage (post-alpha)

- [ ] Integration coverage is thin: one real-mTLS test (single connection, single
      DATA frame). Add: two simultaneous vehicles; an ALERTS/ERRORS frame end-to-end
      (only DATA is exercised over the wire); `records()` driven through the real
      server; assert a DISCONNECTED record is synthesized on teardown.
- [ ] Add `examples/` to the ruff/pyright include set so the example can't rot on an
      API rename (currently only checked when invoked explicitly).
- [ ] `test_server.py::_free_port()` has a bind/close TOCTOU flake risk; prefer
      `port=0` + resolve the bound port.

## 6. API polish (post-alpha)

- [ ] `server.records()` publicly returns the private `_RecordStream` type — add a
      public `RecordStream` alias for a clean signature.
- [ ] `Record.fields()` is recomputed on every dispatch even with no field-filtered
      listener — gate the compute on "any registration has a `field` constraint".
- [ ] `_authorize` in `server.py` doesn't wrap `x509.load_der_x509_certificate` /
      `getpeercert` in try/except (risk ~nil since TLS pre-verifies, but wrapping
      makes the 496 rejection path total).

## 7. End-state vision — typed per-signal listeners

Generate typed `listen_<Signal>` methods from the protobuf field types, hosted on a
`server.vehicle(vin)` object (mirrors `python-teslemetry-stream`). First pass ships
only the flexible `add_listener(vin/topic/field)` primitive; per-signal typing is
where a `Vehicle` class earns its place (see the design doc's "Typing & end-state
vision"). Non-breaking to add later.
