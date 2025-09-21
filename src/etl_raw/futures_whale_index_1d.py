from dataupsert import pull_range, to_utc_ts, fnum, upsert, daterange_utc, log

def ingest_futures_whale_index_1d(conn, exchanges=["Binance"], pairs=["BTCUSDT","ETHUSDT","XRPUSDT","BNBUSDT","SOLUSDT","DOGEUSDT","ADAUSDT"]):
    table="futures_whale_index_1d"
    sql = """
    insert into futures_whale_index_1d (exchange, symbol, ts_utc, whale_index_value)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set whale_index_value=excluded.whale_index_value;
    """
    s_ms, e_ms = daterange_utc()
    for ex in exchanges:
        for sym in pairs:
            lst = pull_range("/api/futures/whale-index/history",
                             {"exchange":ex,"symbol":sym,"interval":"1d","limit":4500},
                             s_ms, e_ms, "time")
            rows=[(ex, sym, to_utc_ts(it["time"]), fnum(it.get("whale_index_value"))) for it in lst]
            log(f"[{table}] {ex}|{sym} 得 {len(rows)} 行")
            upsert(conn, sql, rows, table)

if __name__ == "__main__":
    from dataupsert import pg
    conn = pg()
    ingest_futures_whale_index_1d(conn)
    conn.close()
