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
from datetime import datetime, timedelta, date

import pandas as pd

from src.etl.load_sources_db import load_all_sources_between
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features, REQUIRED_HISTORY_DAYS
from src.upload.copy_upsert import copy_upsert_chunks  # 既有上傳工具

# ---- 目標表欄位白名單（依你提供的 schema.sql）----
FEATURES_ALLOWED_COLS = [
    "asset","ts_utc",
    "px_open","px_high","px_low","px_close","vol_usd",
    "oi_agg_close","oi_stable_close","oi_coinm_close",
    "funding_oiw_close","funding_volw_close",
    "lsr_global","lsr_top_accts","lsr_top_pos",
    "ob_bids_usd","ob_asks_usd","ob_bids_qty","ob_asks_qty","ob_imb","depth_ratio_q",
    "taker_buy_usd","taker_sell_usd","taker_imb",
    "liq_long_usd","liq_short_usd","liq_net",
    "etf_flow_usd","etf_aum_usd","etf_premdisc",
    "cpi_premium_rate",
    "bfx_long_qty","bfx_short_qty","borrow_ir",
    "puell","s2f_next_halving","pi_ma110","pi_ma350x2",
    "ret_1d","roc_3","roc_5","roc_10","roc_20","roc_60","roc_120","roc_252",
    "mom_3","mom_5","mom_10","mom_20","mom_60","mom_120","mom_252",
    "sma_10","sma_20","sma_60","sma_120","sma_252",
    "ema_12","ema_26","macd","macd_signal_9","macd_hist",
    "bb_mid_20","bb_up_20","bb_dn_20","atr_14",
    "rv_20","rv_60","rv_120","z_ret_20","z_ret_60","z_ret_120",
    "d_oi_1","oi_roc_5","oi_roc_20","oi_roc_60","oi_z_60",
    "d_funding_1","funding_ma_20","funding_ma_60","funding_z_60",
    "lsr_ma20_global","lsr_z60_global","lsr_ma20_top_accts","lsr_z60_top_accts","lsr_ma20_top_pos","lsr_z60_top_pos",
    "ob_imb_ma20","ob_imb_z60","depth_ratio_q_ma20","depth_ratio_q_z60",
    "taker_imb_ma20","taker_imb_z60","taker_buy_ma20","taker_sell_ma20","taker_buy_z60","taker_sell_z60",
    "liq_z60","etf_flow_z60","etf_aum_roc_5","etf_aum_roc_20",
    "premdisc_ma20","premdisc_z60","cpi_ma20","cpi_z60",
    "bfx_lr","bfx_lr_d1","borrow_ir_ma20",
    "puell_d1","s2f_d1","pi_ma110_d1","pi_ma350x2_d1",
    "xsec_ret_rank","xsec_mom_rank_20","xsec_vol_rank_60","rel_to_btc",
    # 不包含：feature_ver / updated_at / ext_features / date_utc(為 GENERATED)
]

LABELS_ALLOWED_COLS = ["asset", "ts_utc", "fwd_1d_ret"]  # date_utc 也是 GENERATED，COPY 時不可帶

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
    """同名欄位合併（左到右優先），避免 df['ts_utc'] 變成 DataFrame。"""
    dup_names = [name for name, cnt in Counter(df.columns).items() if cnt > 1]
    if dup_names:
        print(f"[fix] 發現重複欄位: {dup_names} -> 進行合併（左優先）")
    for name in dup_names:
        sub = df.loc[:, df.columns == name]
        col = sub.iloc[:, 0]
        for j in range(1, sub.shape[1]):
            col = col.combine_first(sub.iloc[:, j])
        df = df.drop(columns=[name])
        df[name] = col  # 置於尾端
    # 重要欄位置前
    front = [c for c in ["asset", "ts_utc", "date_utc"] if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]

def _ensure_ts_utc_utc(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, utc=True, errors="coerce")
    n_bad = int(s.isna().sum())
    if n_bad:
        sample = series[s.isna()].head(3).tolist()
        print(f"[warn] 轉換 ts_utc 有 {n_bad} 筆無法解析，樣本: {sample}")
    return s

def _sanitize_for_copy(df: pd.DataFrame, allowed_cols: list[str]) -> pd.DataFrame:
    """只保留表允許欄位，且移除 GENERATED 欄位（如 date_utc）。"""
    cols = [c for c in allowed_cols if c in df.columns]
    out = df[cols].copy()
    # 型別校正：Postgres 欄位 s2f_next_halving 是 integer，使用可 NA 的 Int64
    if "s2f_next_halving" in out.columns:
        out["s2f_next_halving"] = pd.to_numeric(out["s2f_next_halving"], errors="coerce").astype("Int64")
    return out

def _upload_features(feat: pd.DataFrame):
    feat = _coalesce_duplicate_columns(feat.copy())
    # 時間戳處理（tz-aware UTC）
    feat["ts_utc"] = _ensure_ts_utc_utc(feat["ts_utc"])
    # 只保留目標表欄位（自動排除 GENERATED 的 date_utc 與帶預設值的欄位）
    feat = _sanitize_for_copy(feat, FEATURES_ALLOWED_COLS)
    # 依資產×年份切塊，上傳到 public.features_1d（PK: asset, ts_utc）
    feat["year"] = feat["ts_utc"].dt.year
    groups = list(feat.groupby(["asset", "year"], sort=True))
    total = len(groups)
    print(f"[run] 開始上傳 features_1d，共 {total} 組（資產×年）…")
    done = 0
    for (a, y), g in groups:
        g = g.drop(columns=["year"])
        copy_upsert_chunks(
            g,
            table="public.features_1d",
            chunk_rows=100_000,
            prefix=f" [{a} {y}]"
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
        # 僅保留允許欄位（排除 GENERATED 的 date_utc）
        lbl = _sanitize_for_copy(lbl, LABELS_ALLOWED_COLS)
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
        "spot":"來源 spot","fut":"來源 fut","oi":"來源 oi","oi_stable":"來源 oi_stable",
        "oi_coinm":"來源 oi_coinm","funding_oiw":"來源 funding_oiw","funding_volw":"來源 funding_volw",
        "lsr_g":"來源 lsr_g","lsr_a":"來源 lsr_a","lsr_p":"來源 lsr_p",
        "ob":"來源 ob","taker":"來源 taker","liq":"來源 liq","etf_flow":"來源 etf_flow",
        "etf_aum":"來源 etf_aum","etf_prem":"來源 etf_prem","etf_hk":"來源 etf_hk",
        "cpi":"來源 cpi","bfx":"來源 bfx","bir":"來源 bir","puell":"來源 puell",
        "s2f":"來源 s2f","pi":"來源 pi",
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
