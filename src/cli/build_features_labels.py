# -*- coding: utf-8 -*-
"""
抓來源 -> 建座標 -> 左連全部來源 -> 計算特徵 -> 上傳 -> 計算標籤 -> 上傳
- 維持你原上傳模式（copy_upsert_chunks）
- 自動多抓 365 天歷史，確保長窗指標能算出
- 詳細 LOG（來源行數、合併欄位、特徵覆蓋率）
"""
import os, time
from datetime import date, timedelta

import pandas as pd
from src.etl.load_sources_db import load_sources_db
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features, REQUIRED_HISTORY_DAYS
from src.upload.copy_upsert import copy_upsert_chunks

def log(msg): print(f"[run] {msg}", flush=True)

def compute_labels(df: pd.DataFrame) -> pd.DataFrame:
    """最簡 D+1 報酬 / 方向 / 波動標籤；與你現流程相容"""
    out = df[["asset","ts_utc","date_utc","px_close"]].copy()
    out = out.sort_values(["asset","ts_utc"])
    out["y_ret_d1"] = out.groupby("asset")["px_close"].pct_change().shift(-1)
    out["y_dir_d1"] = (out["y_ret_d1"] > 0).astype("Int16")
    out["y_vol_d1"] = (out.groupby("asset")["y_ret_d1"].transform(
        lambda x: x.abs().rolling(20, min_periods=5).std()
    ) > 0.02).astype("Int16")
    return out.drop(columns=["px_close"])

def main(days=7):
    t0 = time.perf_counter()

    # 預設跑到 T-1（與你現有流程一致）
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days-1)

    # 多抓長歷史避免「十天前有、一日前沒有」
    hist_start = start - timedelta(days=REQUIRED_HISTORY_DAYS)

    log(f"抓取來源 {hist_start} ~ {end}")
    S = load_sources_db(hist_start, end)
    for k, v in S.items():
        if v is not None:
            log(f"來源 {k}: rows={len(v)}, cols={list(v.columns)}")

    log("建立價格座標…")
    px = build_price_coordinate(S.get("fut"), S.get("spot"))
    log(f"座標 rows={len(px)}, cols={list(px.columns)}")

    log("左連接來源…")
    df = left_join_all(px, S)
    log(f"左連完成 rows={len(df)}, cols={list(df.columns)}")

    log("計算特徵…")
    feat = compute_features(df, start_date=start, out_end=end, log=log).reset_index(drop=True)
    log(f"特徵完成 rows={len(feat)}, cols={len(feat.columns)}")

    log("上傳 features_1d…")
    copy_upsert_chunks(feat, table="public.features_1d")

    log("計算標籤…")
    labels = compute_labels(feat)
    log(f"標籤完成 rows={len(labels)}")

    log("上傳 labels_1d…")
    copy_upsert_chunks(labels, table="public.labels_1d")

    log(f"全部完成。耗時 {time.perf_counter()-t0:.1f}s")

if __name__ == "__main__":
    days = int(os.getenv("DAYS", "7"))
    main(days=days)
