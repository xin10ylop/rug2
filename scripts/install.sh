#!/usr/bin/env bash
# One-shot setup on the trading box (Ubuntu/Debian, Python 3.11+).
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
. .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt pytest
./scripts/gen_protos.sh
[ -f config/config.toml ] || cp config/config.example.toml config/config.toml
[ -f config/operators.json ] || cp config/operators.example.json config/operators.json
python -m pytest -q tests
echo
echo "next: export TRITON_RPC_URL / TRITON_GRPC_URL / TRITON_X_TOKEN, then"
echo "  python -m rugsnipe.cli keygen --out keys/trader.json"
echo "  python -m rugsnipe.cli check --config config/config.toml"
echo "and follow RUNBOOK.md"
