import os
import psycopg2
from dotenv import load_dotenv
from datetime import date

from src.cli.build_and_upload_features_1d import main as build_all

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

def main():
    dsn = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(dsn)
    d = latest_date(conn)
    if not d:
        log("來源沒有任何資料")
        return

    log(f"最新日期 {d}")
    if check_completeness(conn, d):
        log(f"{d} 完整 → 開始加工")
        build_all()
    else:
        log(f"{d} 不完整 → 跳過")

    conn.close()

if __name__ == "__main__":
    main()
