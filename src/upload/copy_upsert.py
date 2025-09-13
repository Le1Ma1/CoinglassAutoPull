# -*- coding: utf-8 -*-
"""
src/upload/copy_upsert.py

將 DataFrame 以 COPY + UPSERT 的方式寫入 Postgres（Supabase）。
- 自動對齊目標表欄位與型別
- 排除 generated 欄位（如 date_utc）
- 支援分批上傳（chunk_by 或 chunk_rows）
"""

from __future__ import annotations
import io
import os
from typing import List, Sequence, Tuple, Optional, Dict

import pandas as pd
import psycopg2

from src.common.db import get_conn  # 你專案內的連線工具（讀 SUPABASE_DB_URL）


def _split_schema_table(table_full: str | Tuple[str, str]) -> Tuple[str, str]:
    if isinstance(table_full, tuple):
        return table_full[0], table_full[1]
    if "." in table_full:
        s, t = table_full.split(".", 1)
        return s, t
    return "public", table_full


def _get_target_columns(conn, schema: str, table: str) -> pd.DataFrame:
    """
    讀取目標表欄位資訊（排除 system schemas）。回傳欄位順序、型別、是否 generated。
    """
    q = """
    SELECT
        c.column_name,
        c.data_type,
        c.is_generated
    FROM information_schema.columns c
    WHERE c.table_schema = %s
      AND c.table_name = %s
    ORDER BY c.ordinal_position
    """
    df = pd.read_sql(q, conn, params=(schema, table))
    # is_generated 一般為 'ALWAYS' 或 'NEVER'
    return df


def _create_temp_table(conn, schema: str, table: str, tmp: str, drop_generated: Sequence[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TEMP TABLE "{tmp}" (LIKE "{schema}"."{table}" INCLUDING DEFAULTS) ON COMMIT DROP;')
        for col in drop_generated:
            cur.execute(f'ALTER TABLE "{tmp}" DROP COLUMN IF EXISTS "{col}";')
    conn.commit()


def _prepare_columns_for_copy(df: pd.DataFrame,
                              target_cols_meta: pd.DataFrame,
                              key_cols: Sequence[str]) -> List[str]:
    """
    依目標表欄位順序，挑選實際要 COPY/UPSERT 的欄位。
    - 排除 is_generated='ALWAYS'
    - 必定包含 key_cols
    - 其他欄位只取 DataFrame 真的有的
    - 不主動加入 updated_at/ext_features（交給 default/SET now()）
    """
    gen = set(target_cols_meta.loc[target_cols_meta["is_generated"] == "ALWAYS", "column_name"].tolist())
    target_cols = target_cols_meta["column_name"].tolist()

    cols: List[str] = []
    # 先確保 key 在前
    for k in target_cols:
        if k in key_cols and k not in gen and k in df.columns and k not in cols:
            cols.append(k)
    # 其餘欄位（依目標表順序）
    for c in target_cols:
        if c in gen:
            continue
        if c in ("updated_at",):   # 由 UPSERT 時間戳自己更新
            continue
        if c in key_cols:
            continue
        if c in df.columns and c not in cols:
            cols.append(c)
    return cols


def _df_to_csv_buffer(df: pd.DataFrame, columns: List[str]) -> io.StringIO:
    # 轉換為 CSV；NaN 以空字串表示，COPY 會視為 NULL
    buf = io.StringIO()
    # 盡量保持 ts_utc 為 tz-aware ISO 字串（若來自 pandas datetime64[ns, UTC] to_csv 會輸出 +00:00）
    df.to_csv(buf, index=False, header=True, columns=columns, na_rep="")
    buf.seek(0)
    return buf


def _upsert_from_temp(conn,
                      schema: str,
                      table: str,
                      tmp: str,
                      cols: List[str],
                      key_cols: Sequence[str]) -> None:
    non_keys = [c for c in cols if c not in key_cols]
    if not non_keys:
        # 理論上不會發生；至少會有一個非鍵欄位
        return
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_keys] + ['"updated_at" = now()'])
    col_list = ", ".join([f'"{c}"' for c in cols])

    sql = f'''
    INSERT INTO "{schema}"."{table}" ({col_list})
    SELECT {col_list} FROM "{tmp}"
    ON CONFLICT ("{'","'.join(key_cols)}")
    DO UPDATE SET {set_clause};
    '''
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _copy_into_temp(conn, tmp: str, df: pd.DataFrame, cols: List[str]) -> None:
    buf = _df_to_csv_buffer(df, cols)
    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY "{tmp}" ({", ".join([f"""\"{c}\"""" for c in cols])}) FROM STDIN WITH CSV HEADER',
            buf
        )
    conn.commit()


def _copy_upsert_one_batch(conn,
                           df_batch: pd.DataFrame,
                           schema: str,
                           table: str,
                           key_cols: Sequence[str],
                           log=print) -> None:
    if df_batch.empty:
        return

    meta = _get_target_columns(conn, schema, table)
    gen_cols = meta.loc[meta["is_generated"] == "ALWAYS", "column_name"].tolist()

    # 只保留目標表會用到的欄位（避免 COPY 出現未知欄）
    cols = _prepare_columns_for_copy(df_batch, meta, key_cols)

    # 建 staging 表
    tmp = f"tmp_{table}_{os.getpid()}_{abs(hash(tuple(cols)))%10_000_000}"
    _create_temp_table(conn, schema, table, tmp, drop_generated=gen_cols)

    # COPY -> UPSERT
    _copy_into_temp(conn, tmp, df_batch[cols], cols)
    _upsert_from_temp(conn, schema, table, tmp, cols, key_cols)

    if log:
        log(f"[upload] upsert -> {schema}.{table} rows={len(df_batch)} cols={len(cols)} keys={list(key_cols)}")


def copy_upsert_chunks(conn,
                       df: pd.DataFrame,
                       schema: str | None = None,
                       table: str | None = None,
                       key_cols: Sequence[str] = ("asset", "ts_utc"),
                       chunk_by: Optional[Sequence[str]] = None,
                       chunk_rows: Optional[int] = None,
                       log=print) -> None:
    """
    將 df 上傳到 schema.table：
      - 若提供 chunk_by，則依欄位分組上傳（例如 ["asset","year"]）
      - 否則若提供 chunk_rows，則按筆數切批
      - 否則整批一次上傳
    """
    if conn is None:
        conn = get_conn()

    if schema is None or table is None:
        # 允許以 "schema.table" 傳進 table
        schema, table = _split_schema_table(table or "public.features_1d")

    # 清除可能出現的重複欄位（pandas 允許同名欄；後續操作需唯一）
    if df.columns.duplicated().any():
        if log:
            dups = df.columns[df.columns.duplicated()].unique().tolist()
            log(f"[fix] 發現重複欄位: {dups} -> 進行合併（左優先）")
        df = df.loc[:, ~df.columns.duplicated()]

    # 排除 generated 欄位（就算 df 有帶上來也不上傳）
    meta = _get_target_columns(conn, schema, table)
    gen_cols = set(meta.loc[meta["is_generated"] == "ALWAYS", "column_name"].tolist())
    keep_cols = [c for c in df.columns if c not in gen_cols]
    df = df[keep_cols].copy()

    # 盡量確保 ts_utc 是 tz-aware；讓 COPY 能直接進 timestamptz
    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    # 依需求分批
    if chunk_by:
        groups = df.groupby(list(chunk_by), dropna=False)
        if log:
            log(f"[run] 開始上傳 {schema}.{table}，共 {groups.ngroups} 組（{ '×'.join(chunk_by) }）…")
        for _, g in groups:
            _copy_upsert_one_batch(conn, g, schema, table, key_cols, log=log)
        return

    if chunk_rows and len(df) > chunk_rows:
        if log:
            log(f"[run] 開始上傳 {schema}.{table}，分批大小 {chunk_rows} …")
        for i in range(0, len(df), int(chunk_rows)):
            g = df.iloc[i:i + int(chunk_rows)]
            _copy_upsert_one_batch(conn, g, schema, table, key_cols, log=log)
        return

    # 單批
    _copy_upsert_one_batch(conn, df, schema, table, key_cols, log=log)
