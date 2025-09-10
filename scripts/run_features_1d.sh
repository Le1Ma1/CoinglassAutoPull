#!/usr/bin/env bash
set -euo pipefail
source ./.venv/Scripts/activate 2>/dev/null || source ./.venv/bin/activate
python -m src.cli.build_and_upload_features_1d
