"""End-to-end example: configure a real vehicle AND receive its telemetry.

This is the complete flow that ``examples/basic.py`` (receive-only) leaves out. It

1. reads its settings from environment variables (documented below),
2. gets-or-creates the server's mTLS certificates with
   :class:`fleet_telemetry.ServerCredentials`,
3. pushes a ``fleet_telemetry_config`` to the vehicle over the Fleet API (using
   ``tesla-fleet-api``), registering the generated root CA so the vehicle trusts
   this server,
4. polls the config until the vehicle reports ``synced == true``, then
5. starts :class:`fleet_telemetry.TelemetryServer` and prints records as they
   arrive.

Run it with ``--dry-run`` (or ``DRY_RUN=1``) to generate certificates and print
the exact config body it *would* push, then exit -- no Fleet API calls, no
listening socket. Use that to inspect the payload without live credentials or a
publicly reachable server.

Prerequisites this example CANNOT do for you
--------------------------------------------
The ``fleet_telemetry_config`` push only takes effect once the account and
vehicle are fully onboarded to *your own* Tesla developer application:

* A **Tesla developer app** with a registered domain, and its **partner
  (application) EC public key** hosted at
  ``https://<domain>/.well-known/appspecific/com.tesla.3p.public-key.pem``.
* A **partner token** generated for that app (a one-time registration step).
* The app's **virtual key paired** with the vehicle (the owner adds it via
  ``https://tesla.com/_ak/<domain>``). Without pairing, the config push is
  accepted by the API but never delivered to the car -- ``key_paired`` stays
  false and the config never syncs.
* A **user OAuth access token** with vehicle *device-data* and *command* scopes
  (this example takes it from ``TESLA_ACCESS_TOKEN``; it does NOT run the OAuth
  flow for you).
* ``TELEMETRY_FQDN`` must be **publicly reachable** on ``TELEMETRY_PORT`` from
  the internet, and DNS for it must resolve to this host. Vehicles open the
  WebSocket *to* you. Exposing a home server is your responsibility -- a
  port-forward, a Cloudflare Tunnel, an ngrok TCP tunnel, or a cloud VM all
  work. If you would rather not run any of this, the fully-managed alternative
  is Teslemetry (https://teslemetry.com), which hosts the receive server and the
  Fleet API plumbing for you.

Environment variables
----------------------
======================  ========  =============================================
Name                    Required  Meaning
======================  ========  =============================================
TESLA_ACCESS_TOKEN      yes       User OAuth access token (device-data+command).
TESLA_VIN               yes       The vehicle VIN to configure.
TELEMETRY_FQDN          yes       Public hostname the vehicle connects to; must
                                  match the server cert SAN (handled for you) and
                                  be publicly reachable.
TESLA_REGION            no        ``na`` (default), ``eu``, or ``cn``.
TELEMETRY_PORT          no        Port the vehicle connects to (default ``443``).
TELEMETRY_CERT_DIR      no        Where certs persist (default
                                  ``./fleet_telemetry_certs``).
TELEMETRY_SYNC_TIMEOUT  no        Seconds to wait for ``synced`` (default ``120``).
DRY_RUN                 no        ``1``/``true`` == ``--dry-run``.
======================  ========  =============================================

Edit :data:`DEFAULT_FIELDS` below to change which signals stream and how often.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from typing import TYPE_CHECKING, Any

import aiohttp

from fleet_telemetry import ServerCredentials, TelemetryServer

if TYPE_CHECKING:
    from tesla_fleet_api.const import Region

_LOGGER = logging.getLogger("fleet_telemetry.example")

#: Default signals to stream: signal name -> interval in seconds. These names are
#: Tesla's ``Field`` enum names -- the same vocabulary as ``Record.fields()``
#: keys. Keep the set small and edit freely.
DEFAULT_FIELDS: dict[str, int] = {
    "VehicleSpeed": 10,
    "Location": 10,
    "Soc": 60,
    "Gear": 5,
}

_VALID_REGIONS = ("na", "eu", "cn")


def build_config_body(
    *,
    vin: str,
    fqdn: str,
    port: int,
    ca_pem: str,
    fields: dict[str, int],
) -> dict[str, Any]:
    """Build the ``fleet_telemetry_config`` request body.

    ``fields`` maps each signal name to its reporting interval in seconds. The
    result is the *full* request body -- shape ``{"vins": [vin], "config": ...}``
    -- ready to hand to ``VehicleFleet.fleet_telemetry_config_create``. Pure: no
    I/O, so it is unit-tested directly.
    """
    return {
        "vins": [vin],
        "config": {
            "hostname": fqdn,
            "port": port,
            "ca": ca_pem,
            "fields": {
                name: {"interval_seconds": interval}
                for name, interval in fields.items()
            },
            "prefer_typed": True,
        },
    }


class _Settings:
    """Validated configuration read from the environment."""

    def __init__(
        self,
        *,
        access_token: str,
        region: Region,
        vin: str,
        fqdn: str,
        port: int,
        cert_dir: str,
        sync_timeout: float,
    ) -> None:
        # Explicit attribute annotations: pyright otherwise widens the Literal
        # ``region`` to ``str`` when it is inferred from the assignment.
        self.access_token: str = access_token
        self.region: Region = region
        self.vin: str = vin
        self.fqdn: str = fqdn
        self.port: int = port
        self.cert_dir: str = cert_dir
        self.sync_timeout: float = sync_timeout


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"error: environment variable {name} is required "
            "(see the module docstring for all settings)"
        )
    return value


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"error: {name} must be an integer, got {raw!r}") from None


def load_settings() -> _Settings:
    """Read and validate all settings from the environment."""
    region_raw = os.environ.get("TESLA_REGION", "na")
    if region_raw not in _VALID_REGIONS:
        raise SystemExit(
            f"error: TESLA_REGION must be one of {_VALID_REGIONS}, "
            f"got {region_raw!r}"
        )
    return _Settings(
        access_token=_require("TESLA_ACCESS_TOKEN"),
        region=region_raw,
        vin=_require("TESLA_VIN"),
        fqdn=_require("TELEMETRY_FQDN"),
        port=_int_env("TELEMETRY_PORT", 443),
        cert_dir=os.environ.get("TELEMETRY_CERT_DIR", "./fleet_telemetry_certs"),
        sync_timeout=float(_int_env("TELEMETRY_SYNC_TIMEOUT", 120)),
    )


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


async def _push_config(
    session: aiohttp.ClientSession, settings: _Settings, body: dict[str, Any]
) -> None:
    """Push the config, poll until synced, then hand off to the server.

    Imports ``tesla_fleet_api`` lazily so ``--dry-run`` and the unit test that
    imports :func:`build_config_body` never require live Fleet API plumbing.
    """
    from tesla_fleet_api import TeslaFleetApi

    api = TeslaFleetApi(
        session=session,
        access_token=settings.access_token,
        region=settings.region,
    )
    vehicle = api.vehicles.createFleet(settings.vin)

    # A sleeping/offline vehicle never syncs its config. Waking is best-effort:
    # it may fail if the app is not paired or the car is unreachable, which the
    # poll below then surfaces as a timeout with guidance.
    _LOGGER.info("waking %s (best-effort) before pushing config", settings.vin)
    try:
        await vehicle.wake_up()
    except Exception as exc:  # noqa: BLE001 - non-fatal; log and continue
        _LOGGER.warning("wake_up failed (continuing anyway): %s", exc)

    _LOGGER.info("pushing fleet_telemetry_config to %s", settings.vin)
    try:
        await vehicle.fleet_telemetry_config_create(body)
    except Exception as exc:  # noqa: BLE001 - surface the Fleet API error clearly
        raise SystemExit(
            f"error: fleet_telemetry_config push failed: {exc}\n"
            "Common causes: the vehicle is offline/asleep, the OAuth token lacks "
            "the required scopes, or the app's virtual key is not paired with the "
            "vehicle (pair it at https://tesla.com/_ak/<your-domain>)."
        ) from exc

    await _poll_until_synced(vehicle, settings)


async def _poll_until_synced(vehicle: Any, settings: _Settings) -> None:
    """Poll ``fleet_telemetry_config_get`` until ``synced`` or timeout."""
    deadline = time.monotonic() + settings.sync_timeout
    attempt = 0
    while True:
        attempt += 1
        elapsed = time.monotonic() - (deadline - settings.sync_timeout)
        try:
            result = await vehicle.fleet_telemetry_config_get()
        except Exception as exc:  # noqa: BLE001 - surface then keep polling
            _LOGGER.warning("config_get failed (attempt %d): %s", attempt, exc)
            response: dict[str, Any] = {}
        else:
            response = result.get("response", {}) or {}

        synced = bool(response.get("synced"))
        key_paired = response.get("key_paired")
        _LOGGER.info(
            "poll %d (%.0fs): synced=%s key_paired=%s",
            attempt,
            elapsed,
            synced,
            key_paired,
        )
        if synced:
            _LOGGER.info("config synced; the vehicle will now stream to us")
            return
        if key_paired is False:
            _LOGGER.warning(
                "key_paired is false: the app's virtual key is not paired with "
                "this vehicle, so the config will never sync. Pair it at "
                "https://tesla.com/_ak/<your-domain> and re-run."
            )
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"error: config did not sync within {settings.sync_timeout:.0f}s. "
                "Check that the vehicle is online, the virtual key is paired, and "
                f"{settings.fqdn}:{settings.port} is publicly reachable."
            )
        await asyncio.sleep(5)


async def _serve(settings: _Settings, creds: ServerCredentials) -> None:
    """Start the telemetry server and print records until interrupted."""
    server = TelemetryServer(
        ssl_context=creds.build_ssl_context(), port=settings.port
    )
    server.on_data(lambda r: print("data:", r.vin, r.fields()))
    server.on_connectivity(
        lambda r: print("connectivity:", r.vin, r.topic)
    )
    async with server:
        _LOGGER.info(
            "listening for vehicles on :%d -- press Ctrl-C to stop", settings.port
        )
        async with server.records() as stream:
            async for record in stream:
                print("stream:", record.vin, record.topic, record.created_at)


async def _run(dry_run: bool) -> None:
    settings = load_settings()

    # Cert generation is synchronous (blocking key-gen + disk I/O); off-load it so
    # the event loop is never blocked, mirroring the guidance in certs.py.
    creds = ServerCredentials(settings.cert_dir)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, creds.ensure, settings.fqdn)
    _LOGGER.info("certificates ready under %s", settings.cert_dir)

    body = build_config_body(
        vin=settings.vin,
        fqdn=settings.fqdn,
        port=settings.port,
        ca_pem=creds.ca_certificate_pem,
        fields=DEFAULT_FIELDS,
    )

    if dry_run:
        print("--dry-run: config body that WOULD be pushed (not sent):")
        print(json.dumps(body, indent=2, sort_keys=True))
        return

    async with aiohttp.ClientSession() as session:
        await _push_config(session, settings, body)
    await _serve(settings, creds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__ or None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="generate certs, print the config body, and exit without any "
        "network calls or listening socket (also enabled by DRY_RUN=1).",
    )
    args = parser.parse_args()
    dry_run = bool(args.dry_run) or _env_flag("DRY_RUN")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(_run(dry_run))
    except KeyboardInterrupt:
        print("\nstopped", file=sys.stderr)


if __name__ == "__main__":
    main()
