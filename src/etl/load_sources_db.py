# -*- coding: utf-8 -*-
"""
load_sources_db.py
- 從 SUPABASE(Postgres) 讀取各來源，回傳 dict
- 嚴格對應你提供的 schema 欄位；日頻僅有 date_utc 的表直接照原樣返回（不造 ts_utc）
"""

from __future__ import annotations
import os
import psycopg2
import pandas as pd
from datetime import date
from typing import Dict, Optional, Tuple

PG_DSN_ENV = "SUPABASE_DB_URL"  # <- 依你要求

def get_conn():
    dsn = os.getenv(PG_DSN_ENV)
    if not dsn:
        raise RuntimeError(f"{PG_DSN_ENV} is empty")
    return psycopg2.connect(dsn)

def _read(conn, sql: str, params: Tuple) -> pd.DataFrame:
    # pandas 會跳 userwarning（非 SQLAlchemy），忽略即可
    return pd.read_sql(sql, conn, params=params)

def _range(s: date, e: date) -> Tuple[str, str]:
    return (s.isoformat(), e.isoformat())

def load_all_sources_between(start_date: date, end_date: date) -> Dict[str, Optional[pd.DataFrame]]:
    s, e = _range(start_date, end_date)
    c = get_conn()

    # 價格
    spot = _read(c, """
        select exchange, symbol, ts_utc, date_utc, open, high, low, close, volume_usd
        from spot_candles_1d
        where date_utc between %s and %s
    """, (s, e))

    fut = _read(c, """
        select exchange, symbol, ts_utc, date_utc, open, high, low, close, volume_usd
        from futures_candles_1d
        where date_utc between %s and %s
    """, (s, e))

    # OI
    oi = _read(c, """
        select symbol, ts_utc, date_utc, close
        from futures_oi_agg_1d
        where date_utc between %s and %s
    """, (s, e))

    oi_stable = _read(c, """
        select exchange_list, symbol, ts_utc, date_utc, close
        from futures_oi_stablecoin_1d
        where date_utc between %s and %s
    """, (s, e))

    oi_coinm = _read(c, """
        select exchange_list, symbol, ts_utc, date_utc, close
        from futures_oi_coin_margin_1d
        where date_utc between %s and %s
    """, (s, e))

    # Funding（兩種加權）
    funding_oiw = _read(c, """
        select symbol, ts_utc, date_utc, close
        from funding_oi_weight_1d
        where date_utc between %s and %s
    """, (s, e))

    funding_volw = _read(c, """
        select symbol, ts_utc, date_utc, close
        from funding_vol_weight_1d
        where date_utc between %s and %s
    """, (s, e))

    # Long/Short（3 張）
    lsr_g = _read(c, """
        select exchange, symbol, ts_utc, date_utc, long_percent, short_percent, long_short_ratio
        from long_short_global_1d
        where date_utc between %s and %s
    """, (s, e))

    lsr_a = _read(c, """
        select exchange, symbol, ts_utc, date_utc, long_percent, short_percent, long_short_ratio
        from long_short_top_accounts_1d
        where date_utc between %s and %s
    """, (s, e))

    lsr_p = _read(c, """
        select exchange, symbol, ts_utc, date_utc, long_percent, short_percent, long_short_ratio
        from long_short_top_positions_1d
        where date_utc between %s and %s
    """, (s, e))

    # Orderbook / Taker / Liq
    ob = _read(c, """
        select exchange_list, symbol, ts_utc, date_utc, bids_usd, bids_qty, asks_usd, asks_qty, range_pct
        from orderbook_agg_futures_1d
        where date_utc between %s and %s
          and range_pct = 0  -- 若表中有多檔距離，預設取 0；如需其他距離可調整
    """, (s, e))

    taker = _read(c, """
        select exchange_list, symbol, ts_utc, date_utc, buy_vol_usd, sell_vol_usd
        from taker_vol_agg_futures_1d
        where date_utc between %s and %s
    """, (s, e))

    liq = _read(c, """
        select exchange_list, symbol, ts_utc, date_utc, long_liq_usd, short_liq_usd
        from liquidation_agg_1d
        where date_utc between %s and %s
    """, (s, e))

    # ETF（只有 date_utc）
    etf_flow = _read(c, """
        select date_utc, total_flow_usd, price_usd, details
        from etf_bitcoin_flow_1d
        where date_utc between %s and %s
    """, (s, e))

    etf_aum = _read(c, """
        select date_utc, net_assets_usd, change_usd, price_usd
        from etf_bitcoin_net_assets_1d
        where date_utc between %s and %s
    """, (s, e))

    etf_prem = _read(c, """
        select date_utc, ticker, nav_usd, market_price_usd, premium_discount
        from etf_premium_discount_1d
        where date_utc between %s and %s
    """, (s, e))

    etf_hk = _read(c, """
        select date_utc, total_flow_usd, price_usd, details
        from hk_etf_flow_1d
        where date_utc between %s and %s
    """, (s, e))

    # Coinbase premium index（有 ts_utc）
    cpi = _read(c, """
        select ts_utc, date_utc, premium_usd, premium_rate
        from coinbase_premium_index_1d
        where date_utc between %s and %s
    """, (s, e))

    # Bitfinex margin long/short（有 ts_utc）
    bfx = _read(c, """
        select symbol, ts_utc, date_utc, long_qty, short_qty
        from bitfinex_margin_long_short_1d
        where date_utc between %s and %s
    """, (s, e))

    # 借貸利率（有多交易所）
    bir = _read(c, """
        select exchange, symbol, ts_utc, date_utc, interest_rate
        from borrow_interest_rate_1d
        where date_utc between %s and %s
    """, (s, e))

    # 指數（僅 date_utc）
    puell = _read(c, """
        select date_utc, price, puell_multiple
        from idx_puell_multiple_daily
        where date_utc between %s and %s
    """, (s, e))

    s2f = _read(c, """
        select date_utc, price, next_halving
        from idx_stock_to_flow_daily
        where date_utc between %s and %s
    """, (s, e))

    pi = _read(c, """
        select date_utc, price, ma_110, ma_350_x2
        from idx_pi_cycle_daily
        where date_utc between %s and %s
    """, (s, e))

    out = {
        "spot": spot, "fut": fut,
        "oi": oi, "oi_stable": oi_stable, "oi_coinm": oi_coinm,
        "funding_oiw": funding_oiw, "funding_volw": funding_volw,
        "lsr_g": lsr_g, "lsr_a": lsr_a, "lsr_p": lsr_p,
        "ob": ob, "taker": taker, "liq": liq,
        "etf_flow": etf_flow, "etf_aum": etf_aum, "etf_prem": etf_prem, "etf_hk": etf_hk,
        "cpi": cpi, "bfx": bfx, "bir": bir,
        "puell": puell, "s2f": s2f, "pi": pi,
    }

    # 方便主程式打印
    for k, v in out.items():
        if v is not None:
            print(f"[load_db] {k:<26} rows={len(v)}")
    return out
