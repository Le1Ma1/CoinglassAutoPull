# -*- coding: utf-8 -*-
from __future__ import annotations
import os
from datetime import date
import psycopg2
import pandas as pd

# 以 SUPABASE_DB_URL 為主；留底 DATABASE_URL 以防本機舊習慣
def get_conn():
    url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL) for PostgreSQL connection")
    # 你的 URL 內已含 host/port/ssl 等參數，這裡直接丟給 psycopg2
    return psycopg2.connect(url)

def _read(conn, sql: str, params: tuple[str, str]) -> pd.DataFrame:
    # pandas 會發出 warning（建議用 SQLAlchemy），但不影響功能
    return pd.read_sql(sql, conn, params=params)

def load_all_sources_between(start: date, end: date) -> dict[str, pd.DataFrame]:
    s, e = start.isoformat(), end.isoformat()
    with get_conn() as c:
        S: dict[str, pd.DataFrame] = {}

        S["spot"] = _read(c, """
            select exchange,symbol,ts_utc,date_utc,open,high,low,close,volume_usd
            from spot_candles_1d
            where date_utc between %s and %s
        """, (s, e))

        S["fut"] = _read(c, """
            select exchange,symbol,ts_utc,date_utc,open,high,low,close,volume_usd
            from futures_candles_1d
            where date_utc between %s and %s
        """, (s, e))

        S["oi"] = _read(c, """
            select symbol,ts_utc,date_utc,open,high,low,close,unit
            from futures_oi_agg_1d
            where date_utc between %s and %s
        """, (s, e))

        S["oi_stable"] = _read(c, """
            select exchange_list,symbol,ts_utc,date_utc,open,high,low,close
            from futures_oi_stablecoin_1d
            where date_utc between %s and %s
        """, (s, e))

        S["oi_coinm"] = _read(c, """
            select exchange_list,symbol,ts_utc,date_utc,open,high,low,close
            from futures_oi_coin_margin_1d
            where date_utc between %s and %s
        """, (s, e))

        S["funding_oiw"] = _read(c, """
            select symbol,ts_utc,date_utc,open,high,low,close
            from funding_oi_weight_1d
            where date_utc between %s and %s
        """, (s, e))

        S["funding_volw"] = _read(c, """
            select symbol,ts_utc,date_utc,open,high,low,close
            from funding_vol_weight_1d
            where date_utc between %s and %s
        """, (s, e))

        S["lsr_g"] = _read(c, """
            select exchange,symbol,ts_utc,date_utc,long_percent,short_percent,long_short_ratio
            from long_short_global_1d
            where date_utc between %s and %s
        """, (s, e))

        S["lsr_a"] = _read(c, """
            select exchange,symbol,ts_utc,date_utc,long_percent,short_percent,long_short_ratio
            from long_short_top_accounts_1d
            where date_utc between %s and %s
        """, (s, e))

        S["lsr_p"] = _read(c, """
            select exchange,symbol,ts_utc,date_utc,long_percent,short_percent,long_short_ratio
            from long_short_top_positions_1d
            where date_utc between %s and %s
        """, (s, e))

        S["ob"] = _read(c, """
            select exchange_list,symbol,ts_utc,date_utc,bids_usd,bids_qty,asks_usd,asks_qty,range_pct
            from orderbook_agg_futures_1d
            where date_utc between %s and %s
        """, (s, e))

        S["taker"] = _read(c, """
            select exchange_list,symbol,ts_utc,date_utc,buy_vol_usd,sell_vol_usd
            from taker_vol_agg_futures_1d
            where date_utc between %s and %s
        """, (s, e))

        S["liq"] = _read(c, """
            select exchange_list,symbol,ts_utc,date_utc,long_liq_usd,short_liq_usd
            from liquidation_agg_1d
            where date_utc between %s and %s
        """, (s, e))

        # 下列 ETF 與指標表雖不是每個特徵都要用，但保留載入以便 left join 時可用
        S["etf_flow"] = _read(c, """
            select date_utc,total_flow_usd,price_usd,details
            from etf_bitcoin_flow_1d
            where date_utc between %s and %s
        """, (s, e))

        S["etf_aum"] = _read(c, """
            select date_utc,net_assets_usd,change_usd,price_usd
            from etf_bitcoin_net_assets_1d
            where date_utc between %s and %s
        """, (s, e))

        S["etf_prem"] = _read(c, """
            select date_utc,ticker,nav_usd,market_price_usd,premium_discount
            from etf_premium_discount_1d
            where date_utc between %s and %s
        """, (s, e))

        S["etf_hk"] = _read(c, """
            select date_utc,total_flow_usd,price_usd,details
            from hk_etf_flow_1d
            where date_utc between %s and %s
        """, (s, e))

        # ⚠️ cpi 沒有 symbol 欄位，不要查 symbol
        S["cpi"] = _read(c, """
            select ts_utc,date_utc,premium_usd,premium_rate
            from coinbase_premium_index_1d
            where date_utc between %s and %s
        """, (s, e))

        S["bfx"] = _read(c, """
            select symbol,ts_utc,date_utc,long_qty,short_qty
            from bitfinex_margin_long_short_1d
            where date_utc between %s and %s
        """, (s, e))

        S["bir"] = _read(c, """
            select exchange,symbol,ts_utc,date_utc,interest_rate
            from borrow_interest_rate_1d
            where date_utc between %s and %s
        """, (s, e))

        S["puell"] = _read(c, """
            select date_utc,price,puell_multiple
            from idx_puell_multiple_daily
            where date_utc between %s and %s
        """, (s, e))

        S["s2f"] = _read(c, """
            select date_utc,price,next_halving
            from idx_stock_to_flow_daily
            where date_utc between %s and %s
        """, (s, e))

        S["pi"] = _read(c, """
            select date_utc,price,ma_110,ma_350_x2
            from idx_pi_cycle_daily
            where date_utc between %s and %s
        """, (s, e))

        return S
