#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
if [ ! -f frontend/dist/index.html ]; then
  bash "$REPO_ROOT/scripts/build_frontend.sh"
fi
export PYTHONPATH="${REPO_ROOT}/backend${PYTHONPATH:+:$PYTHONPATH}"
PORT="${PORT:-8000}"
exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
