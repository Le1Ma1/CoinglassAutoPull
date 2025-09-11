import os
from dotenv import load_dotenv
from datetime import date, timedelta

from src.cli.build_and_upload_features_1d import main as build_all
from src.common.db import connect   # ✅ 共用連線

load_dotenv()

def log(m): 
    print(f"[auto_features] {m}", flush=True)

TABLES_TO_CHECK = [
    "spot_candles_1d",
    "futures_candles_1d",
    "futures_oi_agg_1d",
    "futures_oi_stablecoin_1d",
    "futures_oi_coin_margin_1d",
    "funding_oi_weight_1d",
    "funding_vol_weight_1d",
    "long_short_global_1d",
    "long_short_top_accounts_1d",
    "long_short_top_positions_1d",
    "orderbook_agg_futures_1d",
    "taker_vol_agg_futures_1d",
    "liquidation_agg_1d",
    "etf_bitcoin_flow_1d",
    "etf_bitcoin_net_assets_1d",
    "etf_premium_discount_1d",
    "hk_etf_flow_1d",
    "coinbase_premium_index_1d",
    "bitfinex_margin_long_short_1d",
    "borrow_interest_rate_1d",
    "idx_puell_multiple_daily",
    "idx_stock_to_flow_daily",
    "idx_pi_cycle_daily",
]

def latest_date(conn):
    with conn.cursor() as cur:
        cur.execute("select max(date_utc) from spot_candles_1d;")
        r = cur.fetchone()
        return r[0]

def check_completeness(conn, d: date) -> bool:
    with conn.cursor() as cur:
        for t in TABLES_TO_CHECK:
            cur.execute(f"select count(*) from {t} where date_utc=%s;", (d,))
            n = cur.fetchone()[0]
            if n == 0:
                log(f"{t} 缺少 {d} 資料")
                return False
    return True

def find_latest_complete_date(conn, lookback=7):
    """往前檢查最近 N 天，找出最後一個完整的日期"""
    latest = latest_date(conn)
    if not latest:
        return None
    for i in range(lookback):
        d = latest - timedelta(days=i)
        if check_completeness(conn, d):
            return d
    return None

def main():
    conn = connect()
    d = find_latest_complete_date(conn, lookback=7)
    if not d:
        log("最近 7 天都不完整 → 跳過")
        return

    log(f"找到完整日期 {d} → 開始加工")
    build_all()
    conn.close()

if __name__ == "__main__":
    main()
