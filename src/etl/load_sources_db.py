# -*- coding: utf-8 -*-
"""
src/etl/load_sources_db.py

目的：
- 只針對「特徵 ETL」讀取來源資料。
- 修正：coinbase_premium_index_1d 沒有 symbol 欄位，不再 select symbol（避免 UndefinedColumn）。
- 連線：優先使用 SUPABASE_DB_URL（Render/Supabase Pooler 可含非 5432 連接埠），
       其次 PG_DSN，再者 DATABASE_URL；其餘不變。
- 保持原有回傳鍵名，讓後續 build_coordinate / compute_features_1d 不需改。
"""
from __future__ import annotations
import os
import psycopg2
import pandas as pd

# ------- DB 連線 -------

def _dsn() -> str:
    dsn = (
        os.getenv("SUPABASE_DB_URL")
    )
    if not dsn:
        raise RuntimeError("No DB URL found. Set SUPABASE_DB_URL (preferred) or PG_DSN / DATABASE_URL.")
    return dsn

def get_conn():
    # 直接把 DSN 丟給 psycopg2（可包含自訂連接埠，如 :6543）
    return psycopg2.connect(_dsn())

# ------- 小工具 -------

def _read(conn, sql: str, params: tuple[str, str] | None = None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params)

def q(tbl: str, cols: str) -> str:
    return f"select {cols} from {tbl} where date_utc between %s and %s "

# ------- 封裝所有讀取 -------

def load_all_sources_between(start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    s, e = str(start_date), str(end_date)
    c = get_conn()
    S: dict[str, pd.DataFrame] = {}

    # 價格
    S["spot"] = _read(c, q("spot_candles_1d", "exchange,symbol,ts_utc,date_utc,open,high,low,close,volume_usd"), (s, e))
    S["fut"]  = _read(c, q("futures_candles_1d", "exchange,symbol,ts_utc,date_utc,open,high,low,close,volume_usd"), (s, e))

    # OI / Funding
    S["oi"]        = _read(c, q("futures_oi_agg_1d", "symbol,ts_utc,date_utc,open,high,low,close,unit"), (s, e))
    S["oi_stable"] = _read(c, q("futures_oi_stablecoin_1d", "exchange_list,symbol,ts_utc,date_utc,open,high,low,close"), (s, e))
    S["oi_coinm"]  = _read(c, q("futures_oi_coin_margin_1d", "exchange_list,symbol,ts_utc,date_utc,open,high,low,close"), (s, e))
    S["funding_oiw"]  = _read(c, q("funding_oi_weight_1d", "symbol,ts_utc,date_utc,open,high,low,close"), (s, e))
    S["funding_volw"] = _read(c, q("funding_vol_weight_1d", "symbol,ts_utc,date_utc,open,high,low,close"), (s, e))

    # Long/Short
    S["lsr_g"] = _read(c, q("long_short_global_1d", "exchange,symbol,ts_utc,date_utc,long_percent,short_percent,long_short_ratio"), (s, e))
    S["lsr_a"] = _read(c, q("long_short_top_accounts_1d", "exchange,symbol,ts_utc,date_utc,long_percent,short_percent,long_short_ratio"), (s, e))
    S["lsr_p"] = _read(c, q("long_short_top_positions_1d", "exchange,symbol,ts_utc,date_utc,long_percent,short_percent,long_short_ratio"), (s, e))

    # Orderbook / Taker / Liquidation
    S["ob"]    = _read(c, q("orderbook_agg_futures_1d", "exchange_list,symbol,ts_utc,date_utc,bids_usd,bids_qty,asks_usd,asks_qty,range_pct"), (s, e))
    S["taker"] = _read(c, q("taker_vol_agg_futures_1d", "exchange_list,symbol,ts_utc,date_utc,buy_vol_usd,sell_vol_usd"), (s, e))
    S["liq"]   = _read(c, q("liquidation_agg_1d", "exchange_list,symbol,ts_utc,date_utc,long_liq_usd,short_liq_usd"), (s, e))

    # ETF 與市場指標
    S["etf_flow"] = _read(c, q("etf_bitcoin_flow_1d", "date_utc,total_flow_usd,price_usd,details"), (s, e))
    S["etf_aum"]  = _read(c, q("etf_bitcoin_net_assets_1d", "date_utc,net_assets_usd,change_usd,price_usd"), (s, e))
    S["etf_prem"] = _read(c, q("etf_premium_discount_1d", "date_utc,ticker,nav_usd,market_price_usd,premium_discount"), (s, e))
    S["etf_hk"]   = _read(c, q("hk_etf_flow_1d", "date_utc,total_flow_usd,price_usd,details"), (s, e))

    # Coinbase Premium Index（⚠️ 無 symbol 欄位）
    S["cpi"] = _read(c, q("coinbase_premium_index_1d", "ts_utc,date_utc,premium_usd,premium_rate"), (s, e))

    # Bitfinex 借貸 / 淨多空
    S["bfx"] = _read(c, q("bitfinex_margin_long_short_1d", "symbol,ts_utc,date_utc,long_qty,short_qty"), (s, e))
    S["bir"] = _read(c, q("borrow_interest_rate_1d", "exchange,symbol,ts_utc,date_utc,interest_rate"), (s, e))

    # On-chain/指標
    S["puell"] = _read(c, q("idx_puell_multiple_daily", "date_utc,price,puell_multiple"), (s, e))
    S["s2f"]   = _read(c, q("idx_stock_to_flow_daily", "date_utc,price,next_halving"), (s, e))
    S["pi"]    = _read(c, q("idx_pi_cycle_daily", "date_utc,price,ma_110,ma_350_x2"), (s, e))

    c.close()
    return S
