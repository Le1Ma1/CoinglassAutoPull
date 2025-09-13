#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="${PYTHONPATH:-$PWD}"

# 預設 END_DATE = 昨天（UTC）
if [[ -z "${END_DATE:-}" ]]; then
  if date -u -d "yesterday" +%F >/dev/null 2>&1; then
    END_DATE="$(date -u -d "yesterday" +%F)"
  else
    END_DATE="$(date -u -v-1d +%F)"   # BSD date（本機 macOS）
  fi
fi

echo "DAYS=${DAYS:-7} END_DATE=${END_DATE}"
python -m src.cli.build_features_labels
