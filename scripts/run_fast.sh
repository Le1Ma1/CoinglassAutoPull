#!/usr/bin/env bash
set -euo pipefail

# 快速回補窗：預設抓最近 4 天（含昨天），避免 T+0 晚到
FAST_DAYS="${FAST_DAYS:-4}"

# Dataupsert.py 會讀 START_DATE/END_DATE（END_DATE 不設=自動到“昨天 UTC”）
export START_DATE="$(date -u -d "${FAST_DAYS} days ago" +%F)"
unset END_DATE

# 可選：限流、交易所/幣種（不設走預設）
export CG_QPM="${CG_QPM:-60}"
export CG_EXLISTS="${CG_EXLISTS:-Binance,OKX,Bybit}"
export CG_EXCHANGES="${CG_EXCHANGES:-Binance}"
export CG_COINS="${CG_COINS:-BTC,ETH,XRP,BNB,SOL,DOGE,ADA}"

python Dataupsert.py
