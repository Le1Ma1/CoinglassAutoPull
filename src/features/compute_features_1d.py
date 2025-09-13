# -*- coding: utf-8 -*-
import math
import pandas as pd
from datetime import date
from typing import Callable, Optional

# 為了 252 天等長窗指標，抓一年（含緩衝）
REQUIRED_HISTORY_DAYS = 365

def _log_cov(name: str, s: pd.Series, out_mask: pd.Series, log: Callable[[str], None]):
    total = len(s)
    nn_all = int(s.notna().sum())
    nn_out = int(s[out_mask].notna().sum())
    pct_all = 0 if total == 0 else (1 - (total - nn_all)/total) * 100
    pct_out = 0 if out_mask.sum() == 0 else nn_out / out_mask.sum() * 100
    log(f"[feature] {name}: non-null(all)={nn_all}/{total} ({pct_all:.1f}% non-null) | in-window={nn_out}/{out_mask.sum()} ({pct_out:.1f}% non-null)")

def _ema(x: pd.Series, span: int) -> pd.Series:
    return x.ewm(span=span, adjust=False, min_periods=span).mean()

def compute_features(df: pd.DataFrame, start_date: date, out_end: Optional[date]=None, log: Callable[[str], None]=print) -> pd.DataFrame:
    """
    df: 需包含 ['asset','ts_utc','date_utc','px_open','px_high','px_low','px_close','vol_usd'] 及左連後的原始來源欄位
    start_date: 僅輸出 >= start_date 的列
    out_end: 若指定，僅輸出 <= out_end 的列
    """
    x = df.sort_values(["asset","ts_utc"]).copy()
    out_mask = (x["date_utc"] >= start_date) & ((x["date_utc"] <= out_end) if out_end else True)

    # ===== 基礎報酬 =====
    x["ret_1d"] = x.groupby("asset")["px_close"].pct_change(1)

    # ===== ROC / Momentum =====
    for w in [3,5,10,20,60,120,252]:
        x[f"roc_{w}"] = x.groupby("asset")["px_close"].pct_change(w)
        x[f"mom_{w}"] = x.groupby("asset")["px_close"].transform(lambda s: s - s.shift(w))

    # ===== 移動平均（SMA） =====
    for w in [10,20,60,120,252]:
        x[f"sma_{w}"] = x.groupby("asset")["px_close"].transform(lambda s: s.rolling(w, min_periods=w).mean())

    # ===== EMA / MACD =====
    x["ema_12"] = x.groupby("asset")["px_close"].transform(lambda s: _ema(s, 12))
    x["ema_26"] = x.groupby("asset")["px_close"].transform(lambda s: _ema(s, 26))
    x["macd"] = x["ema_12"] - x["ema_26"]
    x["macd_signal_9"] = x.groupby("asset")["macd"].transform(lambda s: _ema(s, 9))
    x["macd_hist"] = x["macd"] - x["macd_signal_9"]

    # ===== 布林通道（20） =====
    def _bb(g: pd.Series, w=20):
        m = g.rolling(w, min_periods=w).mean()
        sd = g.rolling(w, min_periods=w).std()
        return m, m + 2*sd, m - 2*sd
    mid, up, dn = zip(*x.groupby("asset")["px_close"].apply(lambda s: pd.Series(_bb(s, 20))))
    # 以上 groupby 回傳的是三欄 series tuple，轉回對應索引
    # 為簡潔，重新計算一次並指派
    x["bb_mid_20"] = x.groupby("asset")["px_close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    bb_sd = x.groupby("asset")["px_close"].transform(lambda s: s.rolling(20, min_periods=20).std())
    x["bb_up_20"] = x["bb_mid_20"] + 2 * bb_sd
    x["bb_dn_20"] = x["bb_mid_20"] - 2 * bb_sd

    # ===== ATR(14) =====
    def _tr(df_g: pd.DataFrame) -> pd.Series:
        high = df_g["px_high"]; low = df_g["px_low"]; close = df_g["px_close"]
        prev_close = close.shift(1)
        tr = pd.concat([
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        return tr
    tr = x.groupby("asset", group_keys=False).apply(_tr)
    x["atr_14"] = tr.groupby(x["asset"]).transform(lambda s: s.rolling(14, min_periods=14).mean())

    # ===== Realized Vol (以對數報酬標準差近似) =====
    log_ret = (x["px_close"].groupby(x["asset"]).apply(lambda s: (s / s.shift(1)).apply(lambda v: math.log(v) if pd.notna(v) and v>0 else pd.NA))).reset_index(level=0, drop=True)
    x["rv_20"]  = log_ret.groupby(x["asset"]).transform(lambda s: s.rolling(20,  min_periods=20).std())
    x["rv_60"]  = log_ret.groupby(x["asset"]).transform(lambda s: s.rolling(60,  min_periods=60).std())
    x["rv_120"] = log_ret.groupby(x["asset"]).transform(lambda s: s.rolling(120, min_periods=120).std())

    # ===== z-score of returns =====
    for w in [20,60,120]:
        r = x["ret_1d"].groupby(x["asset"]).transform(lambda s: s.rolling(w, min_periods=w))
        mean = x["ret_1d"].groupby(x["asset"]).transform(lambda s: s.rolling(w, min_periods=w).mean())
        std  = x["ret_1d"].groupby(x["asset"]).transform(lambda s: s.rolling(w, min_periods=w).std())
        x[f"z_ret_{w}"] = (x["ret_1d"] - mean) / std

    # ===== 來源原欄位透出（避免來源「有值但特徵沒算出來」） =====
    passthrough_cols = [
        "oi_agg_close","oi_stable_close","oi_coinm_close",
        "funding_oiw_close","funding_volw_close",
        "lsr_global","lsr_top_accounts","lsr_top_positions",
        "ob_bids_usd","ob_asks_usd","ob_bids_qty","ob_asks_qty",
        "taker_buy_usd","taker_sell_usd",
        "liq_long_usd","liq_short_usd",
        "cpi_premium_rate",
        "bfx_long_qty","bfx_short_qty",
        "borrow_ir",
        "puell","s2f_next_halving","pi_ma110","pi_ma350x2",
        "vol_usd"
    ]
    # 確保不存在的欄位不會報錯
    passthrough_cols = [c for c in passthrough_cols if c in x.columns]

    # ===== 組輸出 =====
    cols_out = ["asset","ts_utc","date_utc","px_open","px_high","px_low","px_close"] + passthrough_cols + [
        "ret_1d",
        *[f"roc_{w}" for w in [3,5,10,20,60,120,252]],
        *[f"mom_{w}" for w in [3,5,10,20,60,120,252]],
        *[f"sma_{w}" for w in [10,20,60,120,252]],
        "ema_12","ema_26","macd","macd_signal_9","macd_hist",
        "bb_mid_20","bb_up_20","bb_dn_20",
        "atr_14","rv_20","rv_60","rv_120",
        "z_ret_20","z_ret_60","z_ret_120",
    ]
    x = x[cols_out]

    # ===== 覆蓋率 LOG（幫你快速定位哪個特徵在近窗掉值） =====
    for c in ["ret_1d","roc_3","mom_3","roc_5","mom_5","roc_10","mom_10","roc_20","mom_20",
              "roc_60","mom_60","roc_120","mom_120","roc_252","mom_252",
              "sma_10","sma_20","sma_60","sma_120","sma_252",
              "ema_12","ema_26","macd","macd_signal_9","macd_hist",
              "bb_mid_20","bb_up_20","bb_dn_20","atr_14",
              "rv_20","rv_60","rv_120","z_ret_20","z_ret_60","z_ret_120"]:
        _log_cov(c, x[c], out_mask, log)

    # 僅輸出近窗
    x = x.loc[(x["date_utc"] >= start_date) & ((x["date_utc"] <= out_end) if out_end else True)]
    return x.reset_index(drop=True)
