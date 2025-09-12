import io, math, pandas as pd
from src.common.db import connect

def copy_upsert_chunks(df: pd.DataFrame, table="public.features_1d", chunk_rows=100_000, prefix=""):
    import io, math
    df = df.copy()
    exclude = {"created_at", "updated_at"}
    cols = [c for c in df.columns if c not in exclude]

    # 如果目標表有 updated_at 欄位才加
    add_updated = (table.endswith("features_1d"))

    set_clause = ", ".join([
        f"{c}=excluded.{c}" for c in cols if c not in ("asset", "ts_utc", "date_utc")
    ])
    if add_updated:
        set_clause += ", updated_at=now()"

    with connect() as conn, conn.cursor() as cur:
        tmp_tbl = "_stage_" + table.split(".")[-1]
        cur.execute(f"drop table if exists {tmp_tbl};")
        cur.execute(f"""
            create temp table {tmp_tbl}
            (like {table} including all)
            on commit drop;
        """)

        n = len(df)
        chunks = max(1, math.ceil(n / chunk_rows))
        for i in range(chunks):
            g = df.iloc[i * chunk_rows:(i + 1) * chunk_rows]
            buf = io.StringIO()
            g.to_csv(buf, index=False, na_rep="")
            buf.seek(0)
            cur.copy_expert(
                f"copy {tmp_tbl} ({', '.join(cols)}) from stdin with (format csv, header true, null '')",
                buf
            )
            pct = int((i + 1) * 100 / chunks)
            print(f"[upload]{prefix} chunk {i+1}/{chunks} rows={len(g)} ({pct}%)")

        cur.execute(f"""
            insert into {table} ({', '.join(cols)})
            select {', '.join(cols)} from {tmp_tbl}
            on conflict (asset, ts_utc) do update
            set {set_clause};
        """)
        conn.commit()
