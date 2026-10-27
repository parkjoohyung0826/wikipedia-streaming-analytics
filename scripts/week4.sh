#!/usr/bin/env bash
# Compatibility wrapper; the Python runner also works on Windows.
set -euo pipefail
cd "$(dirname "$0")/.."
python_bin="${PYTHON_BIN:-python3}"
if [[ -x .venv/bin/python ]]; then python_bin=.venv/bin/python; fi
exec "$python_bin" scripts/week4.py "$@"
