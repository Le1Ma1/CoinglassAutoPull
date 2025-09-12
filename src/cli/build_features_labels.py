import os, time, pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv; load_dotenv()

from src.etl.load_sources_db import load_sources_db
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features
from src.upload.copy_upsert import copy_upsert_chunks

def log(msg): print(f"[run] {msg}", flush=True)

def compute_labels(df: pd.DataFrame) -> pd.DataFrame:
    """依 features_1d 計算標籤（D+1 報酬/方向/波動）"""
    out = df[["asset","ts_utc","px_close"]].copy()
    out = out.sort_values(["asset","ts_utc"])
    out["y_ret_d1"] = out.groupby("asset")["px_close"].pct_change().shift(-1)
    out["y_dir_d1"] = (out["y_ret_d1"] > 0).astype("Int16")
    out["y_vol_d1"] = (out.groupby("asset")["y_ret_d1"].transform(
        lambda x: x.abs().rolling(20,min_periods=5).std()
    ) > 0.02).astype("Int16")
    return out.drop(columns=["px_close"])

def main(days=7):
    t0 = time.perf_counter()
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days-1)

    HISTORY_DAYS = 365  # 額外往前抓一年的資料
    start_hist = start - timedelta(days=HISTORY_DAYS)

    log(f"抓取來源 {start_hist} ~ {end}")
    S = load_sources_db(start_hist, end)
    for k,v in S.items():
        if v is not None:
            log(f"來源 {k}: rows={len(v)}, cols={list(v.columns)}")

    log("建立價格座標…")
    px = build_price_coordinate(S.get("fut"), S.get("spot"))
    log(f"座標 rows={len(px)}, cols={list(px.columns)}")

    log("左連接來源…")
    df = left_join_all(px, S)
    log(f"左連完成 rows={len(df)}, cols={list(df.columns)}")

    log("計算特徵…")
    feat = compute_features(df, start_date=start).reset_index()
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
    days = int(os.getenv("DAYS","7"))
    main(days=days)
