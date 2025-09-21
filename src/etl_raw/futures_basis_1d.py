from dataupsert import pull_range, to_utc_ts, fnum, upsert, daterange_utc, log
import datetime as dt

def ingest_futures_basis_1d(conn, exchanges=["Binance"], pairs=["BTCUSDT","ETHUSDT","XRPUSDT","BNBUSDT","SOLUSDT","DOGEUSDT","ADAUSDT"]):
    table="futures_basis_1d"
    sql = """
    insert into futures_basis_1d (exchange, symbol, ts_utc, open_basis, close_basis, open_change, close_change)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set open_basis=excluded.open_basis, close_basis=excluded.close_basis,
                  open_change=excluded.open_change, close_change=excluded.close_change;
    """
    s_ms, e_ms = daterange_utc()
    for ex in exchanges:
        for sym in pairs:
            lst = pull_range("/api/futures/basis/history",
                             {"exchange":ex,"symbol":sym,"interval":"1d","limit":500},  # limit 調小避免 500
                             s_ms, e_ms, "time")
            rows=[(ex, sym, to_utc_ts(it["time"]),
                   fnum(it.get("open_basis")), fnum(it.get("close_basis")),
                   fnum(it.get("open_change")), fnum(it.get("close_change"))) for it in lst]
            log(f"[{table}] {ex}|{sym} 得 {len(rows)} 行")
            upsert(conn, sql, rows, table)

if __name__ == "__main__":
    from dataupsert import pg
    conn = pg()
    ingest_futures_basis_1d(conn)
    conn.close()
