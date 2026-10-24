#!/usr/bin/env bash
# Generate Yellowstone gRPC python stubs into rugsnipe/proto (also done automatically on first import).
set -euo pipefail
cd "$(dirname "$0")/.."
python -m grpc_tools.protoc -Irugsnipe/proto --python_out=rugsnipe/proto --grpc_python_out=rugsnipe/proto \
  rugsnipe/proto/geyser.proto rugsnipe/proto/solana-storage.proto
echo "stubs generated in rugsnipe/proto"
