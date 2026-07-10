"""Unit test for the end-to-end example's pure config-body builder.

The example lives under ``examples/`` and is not an importable package, so it is
loaded by path via :mod:`importlib`. Importing it pulls in ``tesla_fleet_api``
(only for a ``TYPE_CHECKING`` hint, but the module must still import cleanly) and
``fleet_telemetry``; if ``tesla_fleet_api`` is not installed the whole test is
skipped rather than erroring.
"""

from __future__ import annotations

import importlib.util
import pathlib
from types import ModuleType

import pytest

pytest.importorskip("tesla_fleet_api")

_EXAMPLE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "examples"
    / "end_to_end.py"
)


def _load_example() -> ModuleType:
    spec = importlib.util.spec_from_file_location("end_to_end_example", _EXAMPLE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_config_body_shape_and_values() -> None:
    example = _load_example()
    vin = "5YJ3E1EA7KF000000"
    fqdn = "telemetry.example.com"
    port = 8443
    ca_pem = "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n"
    fields = {"VehicleSpeed": 10, "Soc": 60, "Gear": 5}

    body = example.build_config_body(
        vin=vin, fqdn=fqdn, port=port, ca_pem=ca_pem, fields=fields
    )

    assert body["vins"] == [vin]
    config = body["config"]
    assert config["ca"] == ca_pem
    assert config["hostname"] == fqdn
    assert config["port"] == port
    assert config["prefer_typed"] is True
    assert config["fields"] == {
        "VehicleSpeed": {"interval_seconds": 10},
        "Soc": {"interval_seconds": 60},
        "Gear": {"interval_seconds": 5},
    }
