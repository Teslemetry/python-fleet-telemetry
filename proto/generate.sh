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
# Make cross-imports package-relative (generated files import each other by bare module name).
sed -i '' -E 's/^import (vehicle_[a-z]+_pb2)/from fleet_telemetry.proto import \1/' fleet_telemetry/proto/*_pb2.py
