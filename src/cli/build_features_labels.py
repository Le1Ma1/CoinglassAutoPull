# -*- coding: utf-8 -*-
"""
build_features_labels.py
- 抓來源資料 -> 建骨幹座標 -> 左連全部來源 -> 計算特徵 -> 只輸出近 N 天 -> 上傳
- 僅調整「特徵 ETL」，讀/寫都走 SUPABASE_DB_URL，不影響原始數據 ETL。
"""
from __future__ import annotations
import os
import time
from collections import Counter
import pandas as pd
from datetime import datetime, timedelta, date

from src.etl.load_sources_db import load_all_sources_between
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features, REQUIRED_HISTORY_DAYS
from src.upload.copy_upsert import copy_upsert_chunks  # 既有上傳工具

def iso(d: date) -> str: return d.isoformat()
def _date(s: str) -> date: return datetime.strptime(s, "%Y-%m-%d").date()

def _print_src_shape(S: dict):
    for k, v in S.items():
        if v is None:
            continue
        print(f"[load_db] {k:<27} rows={len(v)}")

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

def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """同名欄位合併（左到右優先），避免 df['ts_utc'] 取到 DataFrame 而非 Series。"""
    dup_names = [name for name, cnt in Counter(df.columns).items() if cnt > 1]
    if dup_names:
        print(f"[fix] 發現重複欄位: {dup_names} -> 進行合併（左優先）")
    for name in dup_names:
        sub = df.loc[:, df.columns == name]
        col = sub.iloc[:, 0]
        for j in range(1, sub.shape[1]):
            col = col.combine_first(sub.iloc[:, j])
        df = df.drop(columns=[name])
        df[name] = col  # 重新放回（置於尾端）
    # 重要欄位置前
    front = [c for c in ["asset", "ts_utc", "date_utc"] if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]

def _ensure_ts_utc_utc(series: pd.Series) -> pd.Series:
    """將 ts_utc 轉為 UTC tz-aware；若已 tz-aware 也統一轉為 UTC。"""
    s = pd.to_datetime(series, utc=True, errors="coerce")
    n_bad = int(s.isna().sum())
    if n_bad:
        sample = series[s.isna()].head(3).tolist()
        print(f"[warn] 轉換 ts_utc 有 {n_bad} 筆無法解析，樣本: {sample}")
    return s

def _upload_features(feat: pd.DataFrame):
    feat = _coalesce_duplicate_columns(feat.copy())
    # 時間戳處理
    feat["ts_utc"] = _ensure_ts_utc_utc(feat["ts_utc"])
    # 依資產×年份切塊，上傳到 public.features_1d（PK: asset, ts_utc）
    feat["year"] = feat["ts_utc"].dt.year
    groups = list(feat.groupby(["asset", "year"], sort=True))
    total = len(groups)
    print(f"[run] 開始上傳 features_1d，共 {total} 組（資產×年）…")
    done = 0
    for (a, y), g in groups:
        g = g.drop(columns=["year"])
        prefix = f" [{a} {y}]"
        copy_upsert_chunks(
            g,
            table="public.features_1d",
            chunk_rows=100_000,
            prefix=prefix
        )
        done += 1
        pct = int(done * 100 / total) if total else 100
        print(f"[upload] {done}/{total} ({pct}%) 完成 {a}-{y} rows={len(g)}")

def _build_labels(df_all: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    df = df_all.sort_values(["asset", "ts_utc"]).copy()
    df["fwd_1d_ret"] = df.groupby("asset", dropna=False)["px_close"].pct_change(-1)
    labels = df.loc[
        (df["date_utc"] >= start_date) & (df["date_utc"] <= end_date),
        ["asset", "ts_utc", "date_utc", "fwd_1d_ret"]
    ]
    return labels.reset_index(drop=True)

def _upload_labels(lbl: pd.DataFrame):
    if not len(lbl):
        print("[run] labels_1d 無資料可上傳，略過。")
        return
    try:
        lbl = _coalesce_duplicate_columns(lbl.copy())
        lbl["ts_utc"] = _ensure_ts_utc_utc(lbl["ts_utc"])
        lbl["year"] = lbl["ts_utc"].dt.year
        groups = list(lbl.groupby(["asset", "year"], sort=True))
        print(f"[run] 開始上傳 labels_1d，共 {len(groups)} 組（資產×年）…")
        for (a, y), g in groups:
            g = g.drop(columns=["year"])
            copy_upsert_chunks(
                g,
                table="public.labels_1d",
                chunk_rows=100_000,
                prefix=f" [LBL {a} {y}]"
            )
        print("[run] labels_1d 上傳完成。")
    except Exception as e:
        print(f"[warn] 上傳 labels_1d 失敗（不影響 features_1d）：{e}")

def main():
    days = int(os.getenv("DAYS", "7"))
    today = (datetime.utcnow().date() - timedelta(days=1))  # 跑到 T-1
    end_date = _date(os.getenv("END_DATE", iso(today)))
    start_date = end_date - timedelta(days=days - 1)

    hist_start = start_date - timedelta(days=REQUIRED_HISTORY_DAYS)
    print(f"[run] 抓取來源 {iso(hist_start)} ~ {iso(end_date)}")
    t0 = time.time()

    # 讀來源（走 SUPABASE_DB_URL）
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

    # 建價格座標
    print("[run] 建立價格座標…")
    px = build_price_coordinate({"spot": S.get("spot"), "fut": S.get("fut")})
    print(f"[run] 座標 rows={len(px)}, cols={list(px.columns)}")

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

    # 上傳 features_1d
    _upload_features(feats)

    # 標籤（可選）
    print("[run] 計算標籤…")
    labels = _build_labels(df, start_date, end_date)
    print(f"[run] 標籤完成 rows={len(labels)}")
    _upload_labels(labels)

    print(f"[run] 全部完成。耗時 {round(time.time()-t0,1)}s")

if __name__ == "__main__":
    main()
