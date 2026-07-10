#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v flatc >/dev/null || { echo "flatc not found: brew install flatbuffers"; exit 1; }
flatc --python --gen-onefile -o fleet_telemetry/_flatbuffers fbs/tesla.fbs
touch fleet_telemetry/_flatbuffers/__init__.py
