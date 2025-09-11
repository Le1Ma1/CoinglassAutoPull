import io, math, pandas as pd
from src.common.db import connect

def copy_upsert_chunks(df: pd.DataFrame, table="public.features_1d", chunk_rows=100_000, prefix=""):
    df = df.copy()

    # 與雲端欄名對齊
    if "s2f" in df.columns and "s2f_next_halving" not in df.columns:
        df["s2f_next_halving"] = df["s2f"]

    # 強制整數化；NaN 保留為缺失
    if "s2f_next_halving" in df.columns:
        x = pd.to_numeric(df["s2f_next_halving"], errors="coerce")
        df["s2f_next_halving"] = x.round().astype("Int64")

    exclude = {"created_at", "updated_at"}
    cols = [c for c in df.columns if c not in exclude]
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in ("asset", "ts_utc", "date_utc")])

    with connect() as conn, conn.cursor() as cur:
        # ✅ 每次執行都保證乾淨，不會衝突
        cur.execute("drop table if exists _stage_features_1d;")
        cur.execute("""
            create temp table _stage_features_1d
            (like public.features_1d including all)
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
                f"copy _stage_features_1d ({', '.join(cols)}) from stdin with (format csv, header true, null '')",
                buf
            )
            pct = int((i + 1) * 100 / chunks)
            print(f"[upload]{prefix} chunk {i+1}/{chunks} rows={len(g)} ({pct}%)")

        cur.execute(f"""
            insert into {table} ({', '.join(cols)})
            select {', '.join(cols)} from _stage_features_1d
            on conflict (asset, ts_utc) do update
            set {set_clause}, updated_at=now();
        """)
        conn.commit()
