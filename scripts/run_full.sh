#!/usr/bin/env bash
set -euo pipefail
# 走全量：讓程式用預設 START_DATE=2015-01-01、END_DATE=昨天
unset START_DATE END_DATE || true
python Dataupsert.py
