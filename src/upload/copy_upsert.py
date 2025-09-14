# -*- coding: utf-8 -*-
"""
copy_upsert.py
- 將 DataFrame 以 COPY → INSERT ... ON CONFLICT 方式上傳到 Postgres
- 會：
  1) 讀取目標表欄位型別與是否 GENERATED
  2) 自動排除 GENERATED / 不存在欄位 / updated_at
  3) 以 NULLIF(col,'')::<type> 精準 CAST（含 timestamptz）
  4) 依 (asset, ts_utc) 做 UPSERT
"""

from __future__ import annotations
import io
import math
from typing import Iterable, Dict, List, Tuple

import pandas as pd
import psycopg2
from psycopg2 import sql


# ---------- DB meta ----------

def _fetch_table_columns(conn, schema: str, table: str) -> pd.DataFrame:
    q = """
    SELECT
      column_name,
      data_type,
      is_generated
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    return pd.read_sql(q, conn, params=(schema, table))


def _pg_cast_for(datatype: str) -> str:
    dt = datatype.lower().strip()
    if dt == "timestamp with time zone":
        return "timestamptz"
    if dt in ("timestamp without time zone",):
        return "timestamp"
    if dt in ("double precision", "real", "numeric", "decimal"):
        return "double precision"
    if dt in ("integer", "bigint", "smallint"):
        return dt
    if dt in ("boolean",):
        return "boolean"
    if dt in ("jsonb", "json"):
        return "jsonb" if dt == "jsonb" else "json"
    if dt in ("date",):
        return "date"
    return "text"


# ---------- SQL helpers ----------

def _ident(name: str) -> sql.Identifier:
    return sql.Identifier(name)

def _join_ident(names: Iterable[str]) -> sql.SQL:
    return sql.SQL(", ").join([_ident(n) for n in names])

def _join_values_with_cast(cols: List[str], cast_map: Dict[str, str]) -> sql.SQL:
    """
    產生 SELECT 子句：NULLIF("col",'')::<cast> AS "col"
    """
    parts = []
    for c in cols:
        cast_to = cast_map[c]
        parts.append(
            sql.SQL("NULLIF({col}, '')::{cast} AS {col}").format(
                col=_ident(c),
                cast=sql.SQL(cast_to),
            )
        )
    return sql.SQL(", ").join(parts)


# ---------- Data helpers ----------

def _df_to_csv_buf(df: pd.DataFrame) -> io.StringIO:
    buf = io.StringIO()
    # NaN -> ''，COPY 時會被 NULLIF 處理成 NULL
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    return buf

def _split_by_asset_year(df: pd.DataFrame) -> List[pd.DataFrame]:
    """將 df 依 (asset, 年) 切小塊。"""
    if "asset" not in df.columns or "ts_utc" not in df.columns:
        return [df]
    x = df.copy()
    x["__year"] = pd.to_datetime(x["ts_utc"], utc=True, errors="coerce").dt.year
    groups = []
    for (_, _y), g in x.groupby(["asset", "__year"], dropna=False):
        g = g.drop(columns=["__year"])
        groups.append(g)
    return groups


# ---------- Public: COPY → UPSERT ----------

def copy_upsert_chunks(
    conn,
    schema: str,
    table: str,
    df: pd.DataFrame,
    pk: Tuple[str, str] = ("asset", "ts_utc"),
    chunk_rows: int = 200_000,
    log=print,
):
    """
    以批次（資產×年，再視需要切 row-chunks）上傳 df 到 schema.table，主鍵 pk UPSERT。
    """
    if df is None or len(df) == 0:
        log(f"[upload] 無資料可上傳：{schema}.{table}")
        return

    # 讀取目標表欄位資訊
    cols_meta = _fetch_table_columns(conn, schema, table)
    table_cols = cols_meta["column_name"].tolist()
    gen_cols = cols_meta.loc[cols_meta["is_generated"].str.upper() == "ALWAYS", "column_name"].tolist()
    exclude_cols = set(gen_cols + ["updated_at"])  # 讓 default/trigger 處理

    # 只取交集欄位
    usable_cols = [c for c in df.columns if c in table_cols and c not in exclude_cols]
    if not usable_cols:
        raise ValueError(f"[upload] 找不到可上傳欄位（df={list(df.columns)} 與表 {schema}.{table} 的交集為空）")

    # 準備 CAST 對應
    cast_map: Dict[str, str] = {}
    meta_map = {r["column_name"]: r["data_type"] for _, r in cols_meta.iterrows()}
    for c in usable_cols:
        cast_map[c] = _pg_cast_for(meta_map[c])

    # 先將 df 只留用得到的欄位
    df = df[usable_cols].copy()

    # 預處理：timestamptz 與整數欄位
    if "ts_utc" in df.columns:
        ts = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
        # 轉 ISO（含時區），%z 會是 +0000；轉成 +00:00 讓解析更直覺
        df["ts_utc"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
            r"(\+|\-)(\d{2})(\d{2})$", r"\1\2:\3", regex=True
        )

    # 將 integer 型別的欄位，避免出現 '123.0' 無法 CAST integer
    for c in usable_cols:
        if meta_map[c].lower() in ("integer", "bigint", "smallint"):
            s = pd.to_numeric(df[c], errors="coerce")
            df[c] = s.round(0).astype("Int64")  # 仍可輸出為 '' 代表 NULL

    groups = _split_by_asset_year(df)
    log(f"[run] 開始上傳 {schema}.{table}，共 {len(groups)} 組（資產×年）…")

    with conn:
        with conn.cursor() as cur:
            for gi, g in enumerate(groups, 1):
                if len(g) == 0:
                    continue

                total = len(g)
                n_chunks = max(1, math.ceil(total / max(1, chunk_rows)))
                for ci in range(n_chunks):
                    part = g.iloc[ci*chunk_rows : (ci+1)*chunk_rows]
                    if len(part) == 0:
                        continue

                    # 臨時表（全部 text）
                    tmp_name = f"tmp_copy_{table}_{gi}_{ci}"
                    col_defs = sql.SQL(", ").join([sql.SQL("{} text").format(_ident(c)) for c in usable_cols])
                    cur.execute(
                        sql.SQL("CREATE TEMP TABLE {tmp} ( {cols} ) ON COMMIT DROP")
                        .format(tmp=_ident(tmp_name), cols=col_defs)
                    )

                    # COPY 進臨時表
                    buf = _df_to_csv_buf(part)
                    copy_sql = sql.SQL("COPY {tmp} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')").format(
                        tmp=_ident(tmp_name),
                        cols=_join_ident(usable_cols),
                    )
                    cur.copy_expert(copy_sql.as_string(cur), buf)

                    # INSERT ... ON CONFLICT（精準 CAST）
                    insert_cols = usable_cols
                    key_cols = list(pk)
                    upd_cols = [c for c in insert_cols if c not in key_cols]

                    select_list = _join_values_with_cast(insert_cols, cast_map)
                    update_list = sql.SQL(", ").join([
                        sql.SQL("{col} = EXCLUDED.{col}").format(col=_ident(c)) for c in upd_cols
                    ])

                    insert_sql = sql.SQL("""
                        INSERT INTO {sch}.{tbl} ({cols})
                        SELECT {select_list}
                        FROM {tmp}
                        ON CONFLICT ({pkeys})
                        DO UPDATE SET
                          {updates}
                    """).format(
                        sch=_ident(schema),
                        tbl=_ident(table),
                        cols=_join_ident(insert_cols),
                        select_list=select_list,
                        tmp=_ident(tmp_name),
                        pkeys=_join_ident(key_cols),
                        updates=update_list
                    )

                    cur.execute(insert_sql)
                    log(f"[upload] group {gi}/{len(groups)} chunk {ci+1}/{n_chunks} rows={len(part)} ✅")

    log(f"[upload] 完成上傳 {schema}.{table}。")
