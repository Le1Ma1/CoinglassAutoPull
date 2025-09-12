import pandas as pd
from src.common.db import connect

TABLES = {
    "spot_candles_1d": "spot",
    "futures_candles_1d": "fut",
    "futures_oi_agg_1d": "oi",
    "futures_oi_stablecoin_1d": "oi_stable",
    "futures_oi_coin_margin_1d": "oi_coinm",
    "funding_oi_weight_1d": "funding_oiw",
    "funding_vol_weight_1d": "funding_volw",
    "long_short_global_1d": "lsr_g",
    "long_short_top_accounts_1d": "lsr_a",
    "long_short_top_positions_1d": "lsr_p",
    "orderbook_agg_futures_1d": "ob",
    "taker_vol_agg_futures_1d": "taker",
    "liquidation_agg_1d": "liq",
    "etf_bitcoin_flow_1d": "etf_flow",
    "etf_bitcoin_net_assets_1d": "etf_aum",
    "etf_premium_discount_1d": "etf_prem",
    "hk_etf_flow_1d": "etf_hk",
    "coinbase_premium_index_1d": "cpi",
    "bitfinex_margin_long_short_1d": "bfx",
    "borrow_interest_rate_1d": "bir",
    "idx_puell_multiple_daily": "puell",
    "idx_stock_to_flow_daily": "s2f",
    "idx_pi_cycle_daily": "pi",
}

def load_sources_db(start_date, end_date):
    """直接從 DB 撈資料（ts_utc/date_utc 在區間內）"""
    out = {}
    with connect() as c:
        for tbl, key in TABLES.items():
            q = f"select * from public.{tbl} where date_utc >= %s and date_utc <= %s"
            try:
                df = pd.read_sql(q, c, params=(start_date, end_date))
                print(f"[load_db] {tbl:30s} rows={len(df)}")
                out[key] = df
            except Exception as e:
                print(f"[load_db] {tbl} error: {e}")
    return out
