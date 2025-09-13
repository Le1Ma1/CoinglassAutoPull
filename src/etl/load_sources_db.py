# -*- coding: utf-8 -*-
"""
從資料庫載入 1d 各來源資料，時間區間 [start_date, end_date]
僅「讀取」，不改動原始表。供特徵 ETL 使用。
"""
from __future__ import annotations
import pandas as pd
from datetime import date
from src.common.db import get_conn

def _read(sql: str, params: tuple) -> pd.DataFrame:
    with get_conn() as c:
        # pandas 會提示 SQLAlchemy，這裡沿用 psycopg2 以保持你原設計
        return pd.read_sql(sql, c, params=params)

def load_all_sources_between(start_date: date, end_date: date) -> dict[str, pd.DataFrame]:
    p = (start_date, end_date)

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

    oi = _read("""
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

    etf_flow = _read("""
        select date_utc, total_flow_usd, price_usd, details
        from etf_bitcoin_flow_1d
        where date_utc between %s and %s
    """, p)

    etf_aum = _read("""
        select date_utc, net_assets_usd, change_usd, price_usd
        from etf_bitcoin_net_assets_1d
        where date_utc between %s and %s
    """, p)

    etf_prem = _read("""
        select date_utc, ticker, nav_usd, market_price_usd, premium_discount
        from etf_premium_discount_1d
        where date_utc between %s and %s
    """, p)

    etf_hk = _read("""
        select date_utc, total_flow_usd, price_usd, details
        from hk_etf_flow_1d
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
        "spot": spot, "fut": fut,
        "oi": oi, "oi_stable": oi_stable, "oi_coinm": oi_coinm,
        "funding_oiw": funding_oiw, "funding_volw": funding_volw,
        "lsr_g": lsr_g, "lsr_a": lsr_a, "lsr_p": lsr_p,
        "ob": ob, "taker": taker, "liq": liq,
        "etf_flow": etf_flow, "etf_aum": etf_aum, "etf_prem": etf_prem, "etf_hk": etf_hk,
        "cpi": cpi, "bfx": bfx, "bir": bir,
        "puell": puell, "s2f": s2f, "pi": pi,
    }
