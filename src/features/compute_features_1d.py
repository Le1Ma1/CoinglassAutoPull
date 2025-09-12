import pandas as pd, numpy as np

MAX_WINDOW = 252  # 最長需要 252 天

def log_feature_status(df: pd.DataFrame, name: str):
    if name not in df.columns: 
        print(f"[feature] {name}: not computed", flush=True)
        return
    total = len(df)
    non_null = df[name].notna().sum()
    nan_ratio = 1 - non_null / total if total > 0 else 0
    print(f"[feature] {name}: non-null={non_null}/{total} ({nan_ratio:.1%} NaN)", flush=True)

def compute_features(df: pd.DataFrame, start_date=None) -> pd.DataFrame:
    print(f"[compute_features] input rows={len(df)}, cols={list(df.columns)}", flush=True)

    out = df.copy()
    out = out.sort_index()

    # === 報酬 ===
    out["ret_1d"] = out.groupby("asset")["px_close"].pct_change()
    log_feature_status(out, "ret_1d")

    # === ROC / MOM ===
    for w in [3,5,10,20,60,120,252]:
        out[f"roc_{w}"] = out.groupby("asset")["px_close"].pct_change(w)
        out[f"mom_{w}"] = out.groupby("asset")["px_close"].diff(w)
        log_feature_status(out, f"roc_{w}")
        log_feature_status(out, f"mom_{w}")

    # === 移動平均 ===
    for w in [10,20,60,120,252]:
        out[f"sma_{w}"] = out.groupby("asset")["px_close"].transform(lambda x: x.rolling(w,min_periods=5).mean())
        log_feature_status(out, f"sma_{w}")

    # === EMA / MACD ===
    out["ema_12"] = out.groupby("asset")["px_close"].transform(lambda x: x.ewm(span=12,min_periods=5).mean())
    out["ema_26"] = out.groupby("asset")["px_close"].transform(lambda x: x.ewm(span=26,min_periods=5).mean())
    out["macd"] = out["ema_12"] - out["ema_26"]
    out["macd_signal_9"] = out.groupby("asset")["macd"].transform(lambda x: x.ewm(span=9,min_periods=5).mean())
    out["macd_hist"] = out["macd"] - out["macd_signal_9"]
    for f in ["ema_12","ema_26","macd","macd_signal_9","macd_hist"]:
        log_feature_status(out,f)

    # === 布林通道 ===
    mid = out.groupby("asset")["px_close"].transform(lambda x: x.rolling(20,min_periods=5).mean())
    std = out.groupby("asset")["px_close"].transform(lambda x: x.rolling(20,min_periods=5).std())
    out["bb_mid_20"] = mid
    out["bb_up_20"] = mid + 2*std
    out["bb_dn_20"] = mid - 2*std
    for f in ["bb_mid_20","bb_up_20","bb_dn_20"]:
        log_feature_status(out,f)

    # === ATR ===
    tr = (out["px_high"]-out["px_low"]).abs()
    out["atr_14"] = tr.rolling(14,min_periods=5).mean()
    log_feature_status(out,"atr_14")

    # === 波動率 ===
    for w in [20,60,120]:
        out[f"rv_{w}"] = out.groupby("asset")["ret_1d"].transform(lambda x: x.rolling(w,min_periods=5).std())
        log_feature_status(out,f"rv_{w}")

    # === Z-score ===
    for w in [20,60,120]:
        mean = out.groupby("asset")["ret_1d"].transform(lambda x: x.rolling(w,min_periods=5).mean())
        std = out.groupby("asset")["ret_1d"].transform(lambda x: x.rolling(w,min_periods=5).std())
        out[f"z_ret_{w}"] = (out["ret_1d"]-mean)/std
        log_feature_status(out,f"z_ret_{w}")

    # === 最後過濾，只保留 start_date 後的資料 ===
    if start_date is not None and "ts_utc" in out.index.names:
        mask = out.reset_index()["ts_utc"].dt.date >= start_date
        out = out.reset_index()[mask].set_index(["asset","ts_utc"]).sort_index()

    print(f"[compute_features] output rows={len(out)}, cols={len(out.columns)}", flush=True)
    return out
