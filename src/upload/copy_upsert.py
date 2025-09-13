# -*- coding: utf-8 -*-
"""
src/upload/copy_upsert.py
- 以 COPY -> 暫存表 -> UPSERT 方式高速上傳
- 自動排除 generated columns（例如 date_utc），避免 COPY 失敗
- 僅針對特徵 ETL 使用 SUPABASE_DB_URL，不影響原始數據 ETL
"""
from __future__ import annotations
import os
import io
import re
import psycopg2
import pandas as pd
from typing import List, Tuple

# ------- 連線 --------
def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("缺少資料庫連線字串：請設定 SUPABASE_DB_URL（或 DATABASE_URL）")
    return dsn

def _connect():
    return psycopg2.connect(_get_dsn())

# ------- 資訊查詢 --------
def _parse_table(fullname: str) -> Tuple[str, str]:
    # e.g. "public.features_1d"
    if "." in fullname:
        schema, table = fullname.split(".", 1)
    else:
        schema, table = "public", fullname
    return schema, table

def _get_table_columns(conn, schema: str, table: str) -> pd.DataFrame:
    q = """
    select
      c.ordinal_position,
      c.column_name,
      c.is_generated,
      c.is_nullable,
      c.data_type
    from information_schema.columns c
    where c.table_schema = %s and c.table_name = %s
    order by c.ordinal_position;
    """
    return pd.read_sql(q, conn, params=(schema, table))

def _get_pk_columns(conn, schema: str, table: str) -> List[str]:
    q = """
    select a.attname as column_name
    from pg_index i
    join pg_class t on t.oid = i.indrelid
    join pg_namespace n on n.oid = t.relnamespace
    join pg_attribute a on a.attrelid = t.oid and a.attnum = any(i.indkey)
    where i.indisprimary and n.nspname = %s and t.relname = %s
    order by a.attnum;
    """
    df = pd.read_sql(q, conn, params=(schema, table))
    return df["column_name"].tolist()

# ------- 欄位處理 --------
_SYNONYM_MAP = {
    # 以防上游偶爾帶來不同命名（這裡只是保險；主要輸入來自 features DataFrame）
    "lsr_top_accounts": "lsr_top_accts",
    "lsr_top_positions": "lsr_top_pos",
    "taker_buy_vol_usd": "taker_buy_usd",
    "taker_sell_vol_usd": "taker_sell_usd",
    "liq_long_liq_usd": "liq_long_usd",
    "liq_short_liq_usd": "liq_short_usd",
}

def _apply_synonyms(df: pd.DataFrame) -> pd.DataFrame:
    rename = {k: v for k, v in _SYNONYM_MAP.items() if k in df.columns and v not in df.columns}
    if rename:
        df = df.rename(columns=rename)
    return df

def _choose_upload_columns(df: pd.DataFrame, target_cols: pd.DataFrame, pk_cols: List[str]) -> List[str]:
    # 排除 generated columns 與不可/不需要寫入的欄（例如 updated_at 由 DB/trigger 負責）
    generated = set(target_cols.loc[target_cols["is_generated"].str.upper().eq("ALWAYS"), "column_name"])
    skip = generated.union({"date_utc", "updated_at"})  # date_utc 為 generated stored；updated_at 有預設/trigger
    allowed = [c for c in target_cols["column_name"].tolist() if c not in skip]

    # 僅取交集，且維持目標表順序
    cols = [c for c in allowed if c in df.columns]
    # 確保 PK 在內
    for k in pk_cols:
        if k not in cols and k in df.columns:
            cols.insert(0, k)
    return cols

# ------- COPY + UPSERT 主流程 --------
def copy_upsert_chunks(
    df: pd.DataFrame,
    table: str,
    chunk_rows: int = 200_000,
    prefix: str = ""
):
    if not len(df):
        print(f"{prefix} [upload] 無資料可上傳，略過。")
        return

    df = _apply_synonyms(df.copy())

    # 重要：避免 to_csv 將 tz-aware 轉成 object 雜訊；在上游已經保證 ts_utc 為 UTC tz-aware
    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")

    schema, tbl = _parse_table(table)

    with _connect() as conn, conn.cursor() as cur:
        tgt_cols_df = _get_table_columns(conn, schema, tbl)
        pk_cols = _get_pk_columns(conn, schema, tbl)
        if not pk_cols:
            raise RuntimeError(f"{prefix} 目標表 {schema}.{tbl} 未找到主鍵，無法 upsert")
        # 預期 PK: asset, ts_utc
        up_cols = _choose_upload_columns(df, tgt_cols_df, pk_cols)

        # 非 PK 欄位要更新（排除 PK）
        non_pk_update_cols = [c for c in up_cols if c not in pk_cols]

        # 逐塊處理
        total = len(df)
        n_chunks = (total + chunk_rows - 1) // chunk_rows
        for i in range(n_chunks):
            lo = i * chunk_rows
            hi = min((i + 1) * chunk_rows, total)
            part = df.iloc[lo:hi, :].copy()

            # 只輸出 up_cols 欄位
            part = part.loc[:, up_cols]

            # 建暫存表（欄位皆用 text，插入時讓 PG 做隱式轉換）
            temp_name = f"tmp_copy_{re.sub(r'[^a-zA-Z0-9_]', '_', tbl)}_{os.getpid()}_{i}"
            col_defs = ", ".join([f"{c} text" for c in up_cols])
            cur.execute(f"CREATE TEMP TABLE {temp_name} ({col_defs}) ON COMMIT DROP;")

            # COPY 進暫存表（空字串視為 NULL）
            csv_buf = io.StringIO()
            part.to_csv(csv_buf, index=False, header=True, na_rep="")
            csv_buf.seek(0)
            copy_sql = f"COPY {temp_name} ({', '.join(up_cols)}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
            cur.copy_expert(copy_sql, csv_buf)

            # INSERT … ON CONFLICT
            cols_list = ", ".join(up_cols)
            sel_list = ", ".join([f"NULLIF({c}, '')" for c in up_cols])  # 將空字串轉 NULL，再交由 PG 做型別轉換
            conflict_cols = ", ".join(pk_cols)

            if non_pk_update_cols:
                set_list = ", ".join([f"{c}=EXCLUDED.{c}" for c in non_pk_update_cols])
                set_list += ", updated_at=now()"
            else:
                set_list = "updated_at=now()"

            sql = f"""
            INSERT INTO {schema}.{tbl} ({cols_list})
            SELECT {sel_list} FROM {temp_name}
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {set_list};
            """
            cur.execute(sql)
            conn.commit()

            pct = int((hi / total) * 100) if total else 100
            print(f"{prefix} [upload] chunk {i+1}/{n_chunks} rows={hi-lo} ({pct}%) -> {schema}.{tbl}")

        print(f"{prefix} [upload] 完成上傳 -> {schema}.{tbl}")
