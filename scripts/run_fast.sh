#!/usr/bin/env bash
set -euo pipefail
# 近 N 天（預設 4），到昨天為止
DAYS="${FAST_DAYS:-4}"
export START_DATE="$(date -u -d "${DAYS} days ago" +%Y-%m-%d)"
export END_DATE="$(date -u -d "yesterday" +%Y-%m-%d)"
python Dataupsert.py
