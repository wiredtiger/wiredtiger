#!/bin/bash
#
# Regenerate the Python gRPC stubs from the page_service proto.
# Requires: uv (https://docs.astral.sh/uv/)
#
# Usage: ./generate_proto.sh [PROTO_DIR]
#
# PROTO_DIR defaults to the sls-proto location in the mongo repo.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="${1:-$HOME/mongo/src/mongo/db/modules/atlas/src/disagg_storage/sls-proto/dist/storage/etc/protos}"

if [[ ! -d "$PROTO_DIR" ]]; then
    echo "Error: Proto directory not found: $PROTO_DIR" >&2
    echo "Usage: $0 [PROTO_DIR]" >&2
    exit 1
fi

# Ensure the uv environment is up to date before generating stubs.
cd "$SCRIPT_DIR"
uv sync --quiet

echo "Generating Python gRPC stubs from: $PROTO_DIR"
uv run python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$SCRIPT_DIR" \
    --grpc_python_out="$SCRIPT_DIR" \
    ds/v1/ds_service_common.proto \
    pageservice/v1/page_service.proto

# Create __init__.py files for packages.
for dir in ds ds/v1 pageservice pageservice/v1; do
    touch "$SCRIPT_DIR/$dir/__init__.py"
done

echo "Done. Stubs generated in: $SCRIPT_DIR"
