#!/usr/bin/env bash
set -euo pipefail

# 從最早一路補到“昨天 UTC”
export START_DATE="${START_DATE:-2015-01-01}"
unset END_DATE

# 同上：可調參
export CG_QPM="${CG_QPM:-60}"
export CG_EXLISTS="${CG_EXLISTS:-Binance,OKX,Bybit}"
export CG_EXCHANGES="${CG_EXCHANGES:-Binance}"
export CG_COINS="${CG_COINS:-BTC,ETH,XRP,BNB,SOL,DOGE,ADA}"

python Dataupsert.py
