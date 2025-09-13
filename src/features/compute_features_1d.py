# -*- coding: utf-8 -*-
"""
1d 特徵計算（需要長窗時會依 REQUIRED_HISTORY_DAYS 由 CLI 多抓歷史）
僅寫入 features_1d/labels_1d，不動原始表。
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Callable
from datetime import date

# 覆蓋 252 日等長窗指標
REQUIRED_HISTORY_DAYS = 400

def _cover_log(name: str, s: pd.Series, total: int, log: Callable[[str], None]):
    nn = int(s.notna().sum())
    miss = total - nn
    pct = (miss / total * 100.0) if total else 0.0
    log(f"[feature] {name}: non-null={nn}/{total} ({round(pct,1)}% NaN)")

def compute_features(df_all: pd.DataFrame, out_start: date, out_end: date, log: Callable[[str], None]=print) -> pd.DataFrame:
    df = df_all.sort_values(["asset","date_utc","ts_utc"]).copy()

    # 基礎價格系列
    px = df.groupby("asset", dropna=False).apply(lambda x: x.set_index("date_utc")["px_close"]).unstack(0)
    # 以每個 asset 的時間序列各自計算
    def by_asset(calc):
        return df.groupby("asset", dropna=False).apply(calc).reset_index(level=0, drop=True)

    # 動能 / 報酬
    df["ret_1d"] = by_asset(lambda x: x["px_close"].pct_change(1))
    for n in [3,5,10,20,60,120,252]:
        df[f"roc_{n}"] = by_asset(lambda x: x["px_close"].pct_change(n))
        df[f"mom_{n}"] = by_asset(lambda x: x["px_close"].diff(n))
    # 均線
    for n in [10,20,60,120,252]:
        df[f"sma_{n}"] = by_asset(lambda x: x["px_close"].rolling(n, min_periods=1).mean())
    # EMA & MACD(12,26,9)
    df["ema_12"] = by_asset(lambda x: x["px_close"].ewm(span=12, adjust=False).mean())
    df["ema_26"] = by_asset(lambda x: x["px_close"].ewm(span=26, adjust=False).mean())
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal_9"] = by_asset(lambda x: x["macd"].ewm(span=9, adjust=False).mean())
    df["macd_hist"] = df["macd"] - df["macd_signal_9"]
    # 布林
    mid = by_asset(lambda x: x["px_close"].rolling(20, min_periods=1).mean())
    std = by_asset(lambda x: x["px_close"].rolling(20, min_periods=1).std())
    df["bb_mid_20"] = mid
    df["bb_up_20"]  = mid + 2*std
    df["bb_dn_20"]  = mid - 2*std
    # ATR
    def _atr(x):
        tr = pd.concat([
            (x["px_high"]-x["px_low"]).rename("hl"),
            (x["px_high"]-x["px_close"].shift(1)).abs().rename("hc"),
            (x["px_low"]-x["px_close"].shift(1)).abs().rename("lc"),
        ], axis=1).max(axis=1)
        return tr.rolling(14, min_periods=1).mean()
    df["atr_14"] = by_asset(_atr)
    # 實現波動率（對數報酬平方的移動平均）
    for n in [20,60,120]:
        df[f"rv_{n}"] = by_asset(lambda x: np.log(x["px_close"]).diff().pow(2).rolling(n, min_periods=1).mean())

    # Z-score of returns
    for n in [20,60,120]:
        r = by_asset(lambda x: x["px_close"].pct_change(1))
        m = by_asset(lambda x: r.rolling(n, min_periods=1).mean())
        s = by_asset(lambda x: r.rolling(n, min_periods=1).std())
        df[f"z_ret_{n}"] = (r - m) / s.replace(0, np.nan)

    # === 覆蓋率 LOG（針對 out 窗口）===
    out = df[(df["date_utc"]>=out_start) & (df["date_utc"]<=out_end)].copy()
    total = len(out)
    for c in ["ret_1d"] + [f"roc_{n}" for n in [3,5,10,20,60,120,252]] + \
             [f"mom_{n}" for n in [3,5,10,20,60,120,252]] + \
             [f"sma_{n}" for n in [10,20,60,120,252]] + \
             ["ema_12","ema_26","macd","macd_signal_9","macd_hist",
              "bb_mid_20","bb_up_20","bb_dn_20","atr_14"] + \
             [f"rv_{n}" for n in [20,60,120]] + \
             [f"z_ret_{n}" for n in [20,60,120]]:
        _cover_log(c, out[c], total, log)

    # 僅輸出 out 窗口
    feats = out.drop(columns=[]).copy()
    keep_cols = ["asset","ts_utc","date_utc"] + [c for c in feats.columns if c not in ["asset","ts_utc","date_utc"]]
    feats = feats[keep_cols].reset_index(drop=True)
    return feats
