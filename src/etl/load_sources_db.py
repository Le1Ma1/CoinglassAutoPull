# -*- coding: utf-8 -*-
import pandas as pd
from datetime import date
from typing import Dict, Optional
from src.common.db import get_conn

# 以 pandas.read_sql 讀取，沿用你原本欄位命名（每日彙總 1d 表）
# 只做必要的欄位選取與時間範圍過濾，不改變你的資料內容

def _read(sql: str, params: tuple) -> pd.DataFrame:
    with get_conn() as c:
        df = pd.read_sql(sql, c, params=params)
    return df

def load_sources_db(start: date, end: date) -> Dict[str, Optional[pd.DataFrame]]:
    p = (start, end)

    spot = _read("""
        select exchange, symbol, ts_utc, date_utc, open, high, low, close, volume_usd
        from spot_candles_1d
        where date_utc between %s and %s
    """, p)

    fut = _read("""
        select exchange, symbol, ts_utc, date_utc, open, high, low, close, volume_usd
        from futures_candles_1d
        where date_utc between %s and %s
    """, p)

    oi_agg = _read("""
        select symbol, ts_utc, date_utc, open, high, low, close, unit
        from futures_oi_agg_1d
        where date_utc between %s and %s
    """, p)

    oi_stable = _read("""
        select exchange_list, symbol, ts_utc, date_utc, open, high, low, close
        from futures_oi_stablecoin_1d
        where date_utc between %s and %s
    """, p)

    oi_coinm = _read("""
        select exchange_list, symbol, ts_utc, date_utc, open, high, low, close
        from futures_oi_coin_margin_1d
        where date_utc between %s and %s
    """, p)

    funding_oiw = _read("""
        select symbol, ts_utc, date_utc, open, high, low, close
        from funding_oi_weight_1d
        where date_utc between %s and %s
    """, p)

    funding_volw = _read("""
        select symbol, ts_utc, date_utc, open, high, low, close
        from funding_vol_weight_1d
        where date_utc between %s and %s
    """, p)

    lsr_g = _read("""
        select exchange, symbol, ts_utc, date_utc, long_percent, short_percent, long_short_ratio
        from long_short_global_1d
        where date_utc between %s and %s
    """, p)

    lsr_a = _read("""
        select exchange, symbol, ts_utc, date_utc, long_percent, short_percent, long_short_ratio
        from long_short_top_accounts_1d
        where date_utc between %s and %s
    """, p)

    lsr_p = _read("""
        select exchange, symbol, ts_utc, date_utc, long_percent, short_percent, long_short_ratio
        from long_short_top_positions_1d
        where date_utc between %s and %s
    """, p)

    ob = _read("""
        select exchange_list, symbol, ts_utc, date_utc, bids_usd, bids_qty, asks_usd, asks_qty, range_pct
        from orderbook_agg_futures_1d
        where date_utc between %s and %s
    """, p)

    taker = _read("""
        select exchange_list, symbol, ts_utc, date_utc, buy_vol_usd, sell_vol_usd
        from taker_vol_agg_futures_1d
        where date_utc between %s and %s
    """, p)

    liq = _read("""
        select exchange_list, symbol, ts_utc, date_utc, long_liq_usd, short_liq_usd
        from liquidation_agg_1d
        where date_utc between %s and %s
    """, p)

    cpi = _read("""
        select ts_utc, date_utc, premium_usd, premium_rate
        from coinbase_premium_index_1d
        where date_utc between %s and %s
    """, p)

    bfx = _read("""
        select symbol, ts_utc, date_utc, long_qty, short_qty
        from bitfinex_margin_long_short_1d
        where date_utc between %s and %s
    """, p)

    bir = _read("""
        select exchange, symbol, ts_utc, date_utc, interest_rate
        from borrow_interest_rate_1d
        where date_utc between %s and %s
    """, p)

    puell = _read("""
        select date_utc, price, puell_multiple
        from idx_puell_multiple_daily
        where date_utc between %s and %s
    """, p)

    s2f = _read("""
        select date_utc, price, next_halving
        from idx_stock_to_flow_daily
        where date_utc between %s and %s
    """, p)

    pi = _read("""
        select date_utc, price, ma_110, ma_350_x2
        from idx_pi_cycle_daily
        where date_utc between %s and %s
    """, p)

    return {
        "spot": spot,
        "fut": fut,
        "oi_agg": oi_agg,
        "oi_stable": oi_stable,
        "oi_coinm": oi_coinm,
        "funding_oiw": funding_oiw,
        "funding_volw": funding_volw,
        "lsr_g": lsr_g,
        "lsr_a": lsr_a,
        "lsr_p": lsr_p,
        "ob": ob,
        "taker": taker,
        "liq": liq,
        "cpi": cpi,
        "bfx": bfx,
        "bir": bir,
        "puell": puell,
        "s2f": s2f,
        "pi": pi,
    }
