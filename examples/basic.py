"""Minimal runnable example: receive Tesla Fleet Telemetry from vehicles.

This starts an mTLS WebSocket server that vehicles connect to directly. It
requires a TLS server certificate for the FQDN your vehicles are configured to
stream to, the matching private key, and the Tesla CA bundle used to verify the
client certificates the vehicles present.

Replace the certificate paths below, point your fleet's ``fleet_telemetry_config``
hostname at this server, and run it. See the project README for how vehicles are
configured (that setup is done via the Fleet API and is out of scope here).
"""

import asyncio
import ssl

from fleet_telemetry import TelemetryServer, Topic


def build_ssl_context() -> ssl.SSLContext:
    """Build the mandatory mutual-TLS context for the telemetry server."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain("server-cert.pem", "server-key.pem")
    ctx.load_verify_locations("tesla-ca.pem")
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def main() -> None:
    server = TelemetryServer(ssl_context=build_ssl_context(), port=443)

    # Callback style: every DATA record, flattened to signal-name -> value.
    server.on_data(lambda r: print(r.vin, r.fields()))

    # Synthetic connect/disconnect events (plus any vehicle-sent connectivity).
    server.on_connectivity(lambda r: print("connectivity:", r.vin, r.topic))

    # Filtered listener: only DATA records that carry the VehicleSpeed signal.
    server.add_listener(
        lambda r: print("speed listener:", r.fields().get("VehicleSpeed")),
        topic=Topic.DATA,
        field="VehicleSpeed",
    )

    async with server:
        print("listening for vehicles on :443")

        # Pull style: iterate records as they arrive, alongside the callbacks.
        async with server.records() as stream:
            async for record in stream:
                print("stream:", record.vin, record.topic, record.created_at)


if __name__ == "__main__":
    asyncio.run(main())
