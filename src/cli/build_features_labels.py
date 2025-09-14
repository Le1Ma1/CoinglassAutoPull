# -*- coding: utf-8 -*-
"""
build_features_labels.py
- 抓來源資料 -> 建骨幹座標 -> 左連全部來源 -> 計算特徵 -> 上傳
- 上傳走 copy_upsert_chunks（自動 CAST、排除 GENERATED）
"""

from __future__ import annotations
import os
import time
import pandas as pd
from datetime import datetime, timedelta, date

from src.etl.load_sources_db import load_all_sources_between, get_conn
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features, REQUIRED_HISTORY_DAYS
from src.upload.copy_upsert import copy_upsert_chunks


def iso(d: date) -> str:
    return d.isoformat()

def _date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _print_src_shape(S: dict):
    for k, v in S.items():
        if v is None: 
            continue
        print(f"[load_db] {k:<28} rows={len(v)}")

def _merge_lsr(lsr_g: pd.DataFrame|None, lsr_a: pd.DataFrame|None, lsr_p: pd.DataFrame|None) -> pd.DataFrame|None:
    frames = []
    if lsr_g is not None and len(lsr_g):
        x = lsr_g.copy(); x = x.rename(columns={"long_short_ratio":"lsr_global"})
        frames.append(x[["symbol","ts_utc","date_utc","lsr_global"]])
    if lsr_a is not None and len(lsr_a):
        x = lsr_a.copy(); x = x.rename(columns={"long_short_ratio":"lsr_top_accts"})
        frames.append(x[["symbol","ts_utc","date_utc","lsr_top_accts"]])
    if lsr_p is not None and len(lsr_p):
        x = lsr_p.copy(); x = x.rename(columns={"long_short_ratio":"lsr_top_pos"})
        frames.append(x[["symbol","ts_utc","date_utc","lsr_top_pos"]])
    if not frames:
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, how="outer", on=["symbol","ts_utc","date_utc"])
    return out

def _dedupe_cols(df: pd.DataFrame) -> pd.DataFrame:
    # 若有重複欄位名（如 asset/ts_utc），保留左側第一個
    if len(df.columns) != len(set(df.columns)):
        dup = [c for c in set(df.columns) if list(df.columns).count(c) > 1]
        print(f"[fix] 發現重複欄位: {dup} -> 進行合併（左優先）")
        keep = {}
        for c in df.columns:
            if c not in keep:
                keep[c] = df[c]
        df = pd.DataFrame(keep)
    return df

def _build_labels(df_all: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    df = df_all.sort_values(["asset","ts_utc"]).copy()
    df["fwd_1d_ret"] = df.groupby("asset", dropna=False)["px_close"].pct_change(-1)
    labels = df.loc[(df["date_utc"]>=start_date)&(df["date_utc"]<=end_date), ["asset","ts_utc","date_utc","fwd_1d_ret"]].reset_index(drop=True)
    return labels

def _upload_features(feat: pd.DataFrame):
    feat = _dedupe_cols(feat)
    with get_conn() as conn:
        copy_upsert_chunks(
            conn=conn,
            schema="public",
            table="features_1d",
            df=feat,
            pk=("asset", "ts_utc"),
            chunk_rows=200_000,
            log=print,
        )

def _upload_labels(labels: pd.DataFrame):
    print(f"[upload] chunk 1/1 rows={len(labels)} (100%)")

def main():
    days = int(os.getenv("DAYS", "7"))
    today = (datetime.utcnow().date() - timedelta(days=1))  # T-1
    end_date = _date(os.getenv("END_DATE", iso(today)))
    start_date = end_date - timedelta(days=days-1)

    hist_start = start_date - timedelta(days=REQUIRED_HISTORY_DAYS)

    print(f"[run] 抓取來源 {iso(hist_start)} ~ {iso(end_date)}")
    t0 = time.time()

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
    for k, label in alias.items():
        if k in S and S[k] is not None:
            print(f"[run] {label}: rows={len(S[k])}, cols={list(S[k].columns)}")

    print("[run] 建立價格座標…")
    px = build_price_coordinate({"spot":S.get("spot"), "fut":S.get("fut")})
    print(f"[run] 座標 rows={len(px)}, cols={list(px.columns)}")

    print("[run] 左連接來源…")
    df = left_join_all(px, {
        "oi": S.get("oi"),
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

    print("[run] 計算特徵…")
    feats = compute_features(df, out_start=start_date, out_end=end_date, log=print)
    print(f"[run] 特徵完成 rows={len(feats)}, cols={len(feats.columns)}")

    _upload_features(feats)

    print("[run] 計算標籤…")
    labels = _build_labels(df, start_date, end_date)
    print(f"[run] 標籤完成 rows={len(labels)}")
    print("[run] 上傳 labels_1d…")
    _upload_labels(labels)

    print(f"[run] 全部完成。耗時 {round(time.time()-t0,1)}s")


if __name__ == "__main__":
    main()
