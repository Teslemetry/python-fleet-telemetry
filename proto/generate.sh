#!/usr/bin/env bash
# Regenerate protobuf bindings into fleet_telemetry/proto/.
set -euo pipefail
cd "$(dirname "$0")/.."
uv run --with "grpcio-tools>=1.76" python -m grpc_tools.protoc \
    -Iproto \
    --python_out=fleet_telemetry/proto \
    --pyi_out=fleet_telemetry/proto \
    proto/vehicle_data.proto \
    proto/vehicle_alert.proto \
    proto/vehicle_error.proto \
    proto/vehicle_connectivity.proto \
    proto/vehicle_metric.proto
# Make cross-imports package-relative (generated files import each other by bare
# module name). Uses a tmp-file rewrite so it works with both BSD and GNU sed.
for f in fleet_telemetry/proto/*_pb2.py; do
    sed -E 's/^import (vehicle_[a-z]+_pb2)/from fleet_telemetry.proto import \1/' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
