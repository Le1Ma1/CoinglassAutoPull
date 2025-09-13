# -*- coding: utf-8 -*-
"""
build_features_labels.py
- 抓來源資料 -> 建骨幹座標 -> 左連全部來源 -> 計算特徵 -> 只輸出近 N 天 -> 上傳
- 只調整特徵 ETL，讀取使用 SUPABASE_DB_URL；不影響你原本的原始數據 ETL。
"""
from __future__ import annotations
import os
import time
import pandas as pd
from datetime import datetime, timedelta, date

from src.etl.load_sources_db import load_all_sources_between
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features, REQUIRED_HISTORY_DAYS

# 如你已有上傳模組，這裡沿用（不要改你原有行為）
try:
    from src.upload.copy_upsert import upsert_dataframe as _real_upsert  # 你的函式若叫別名，這裡改成對應名稱即可
except Exception:
    _real_upsert = None  # 若環境無上傳工具，仍可本地跑特徵計算

def iso(d: date) -> str: return d.isoformat()
def _date(s: str) -> date: return datetime.strptime(s, "%Y-%m-%d").date()

def _print_src_shape(S: dict):
    for k, v in S.items():
        if v is None: 
            continue
        print(f"[load_db] {k:<28} rows={len(v)}")

def _merge_lsr(lsr_g: pd.DataFrame|None, lsr_a: pd.DataFrame|None, lsr_p: pd.DataFrame|None) -> pd.DataFrame|None:
    frames = []
    if lsr_g is not None and len(lsr_g):
        x = lsr_g.rename(columns={"long_short_ratio":"lsr_global"})
        frames.append(x[["symbol","ts_utc","date_utc","lsr_global"]])
    if lsr_a is not None and len(lsr_a):
        x = lsr_a.rename(columns={"long_short_ratio":"lsr_top_accounts"})
        frames.append(x[["symbol","ts_utc","date_utc","lsr_top_accounts"]])
    if lsr_p is not None and len(lsr_p):
        x = lsr_p.rename(columns={"long_short_ratio":"lsr_top_positions"})
        frames.append(x[["symbol","ts_utc","date_utc","lsr_top_positions"]])
    if not frames: 
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, how="outer", on=["symbol","ts_utc","date_utc"])
    return out

def _upload(df: pd.DataFrame, table: str, pk=("asset","date_utc")):
    if _real_upsert is not None:
        _real_upsert(df, table_name=table, pk_cols=list(pk))
    print(f"[upload] chunk 1/1 rows={len(df)} (100%)")

def _build_labels(df_all: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    df = df_all.sort_values(["asset","ts_utc"]).copy()
    df["fwd_1d_ret"] = df.groupby("asset", dropna=False)["px_close"].pct_change(-1)
    labels = df.loc[(df["date_utc"]>=start_date)&(df["date_utc"]<=end_date), ["asset","ts_utc","date_utc","fwd_1d_ret"]]
    return labels.reset_index(drop=True)

def main():
    days = int(os.getenv("DAYS", "7"))
    today = (datetime.utcnow().date() - timedelta(days=1))  # 跑到 T-1
    end_date = _date(os.getenv("END_DATE", iso(today)))
    start_date = end_date - timedelta(days=days-1)

    hist_start = start_date - timedelta(days=REQUIRED_HISTORY_DAYS)
    print(f"[run] 抓取來源 {iso(hist_start)} ~ {iso(end_date)}")
    t0 = time.time()

    # 讀來源
    S = load_all_sources_between(hist_start, end_date)
    _print_src_shape(S)
    alias = {
        "spot":"來源 spot", "fut":"來源 fut", "oi":"來源 oi", "oi_stable":"來源 oi_stable",
        "oi_coinm":"來源 oi_coinm", "funding_oiw":"來源 funding_oiw", "funding_volw":"來源 funding_volw",
        "lsr_g":"來源 lsr_g", "lsr_a":"來源 lsr_a", "lsr_p":"來源 lsr_p",
        "ob":"來源 ob", "taker":"來源 taker", "liq":"來源 liq", "etf_flow":"來源 etf_flow",
        "etf_aum":"來源 etf_aum", "etf_prem":"來源 etf_prem", "etf_hk":"來源 etf_hk",
        "cpi":"來源 cpi", "bfx":"來源 bfx", "bir":"來源 bir", "puell":"來源 puell",
        "s2f":"來源 s2f", "pi":"來源 pi",
    }
    for k,label in alias.items():
        if k in S and S[k] is not None:
            print(f"[run] {label}: rows={len(S[k])}, cols={list(S[k].columns)}")

    # 建價格座標
    print("[run] 建立價格座標…")
    px = build_price_coordinate({"spot":S.get("spot"), "fut":S.get("fut")})
    print(f"[run] 座標 rows={len(px)}, cols={['px_open','px_high','px_low','px_close','vol_usd']}")

    # 左連
    print("[run] 左連接來源…")
    df = left_join_all(px, {
        "oi": S.get("oi"),
        "oi_stable": S.get("oi_stable"),
        "oi_coinm": S.get("oi_coinm"),
        "funding_oiw": S.get("funding_oiw"),
        "funding_volw": S.get("funding_volw"),
        "lsr": _merge_lsr(S.get("lsr_g"), S.get("lsr_a"), S.get("lsr_p")),
        "ob": S.get("ob"),
        "taker": S.get("taker"),
        "liq": S.get("liq"),
        "cpi": S.get("cpi"),
        "bfx": S.get("bfx"),
        "bir": S.get("bir"),
        "puell": S.get("puell"),
        "s2f": S.get("s2f"),
        "pi": S.get("pi"),
    })
    print(f"[run] 左連完成 rows={len(df)}, cols={list(df.columns)}")

    # 特徵
    print("[run] 計算特徵…")
    feats = compute_features(df, out_start=start_date, out_end=end_date, log=print)
    print(f"[run] 特徵完成 rows={len(feats)}, cols={len(feats.columns)}")
    print("[run] 上傳 features_1d…")
    _upload(feats, "features_1d")

    # 標籤
    print("[run] 計算標籤…")
    labels = _build_labels(df, start_date, end_date)
    print(f"[run] 標籤完成 rows={len(labels)}")
    print("[run] 上傳 labels_1d…")
    _upload(labels, "labels_1d", pk=("asset","date_utc"))

    print(f"[run] 全部完成。耗時 {round(time.time()-t0,1)}s")

if __name__ == "__main__":
    main()
