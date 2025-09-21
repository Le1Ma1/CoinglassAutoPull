from dataupsert import pull_range, to_utc_ts, fnum, upsert, daterange_utc, log

def ingest_futures_cgdi_index_1d(conn):
    table="futures_cgdi_index_1d"
    sql = """
    insert into futures_cgdi_index_1d (ts_utc, cgdi_index_value)
    values %s
    on conflict (ts_utc)
    do update set cgdi_index_value=excluded.cgdi_index_value;
    """
    s_ms, e_ms = daterange_utc()
    lst = pull_range("/api/futures/cgdi-index/history", {"limit":4500}, s_ms, e_ms, "time")
    rows=[(to_utc_ts(it["time"]), fnum(it.get("cgdi_index_value"))) for it in lst]
    log(f"[{table}] 得 {len(rows)} 行")
    upsert(conn, sql, rows, table)

if __name__ == "__main__":
    from dataupsert import pg
    conn = pg()
    ingest_futures_cgdi_index_1d(conn)
    conn.close()
