# -*- coding: utf-8 -*-
"""
compute_features_1d.py
- 日頻特徵計算；兼容來源欄位別名；缺失來源自動補空欄位
- 僅使用 groupby.transform / shift / rolling（避免 DataFrame->單欄指派錯誤）
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import date
from typing import Callable, Optional

# 需要的最長歷史窗（>252，含緩衝）
REQUIRED_HISTORY_DAYS = 400


# ---------- helpers ----------
def _ensure_col(df: pd.DataFrame, name: str, alts: list[str] | None = None) -> None:
    """若 name 不存在，嘗試以 alts 中第一個存在的欄位建立；否則建立 NaN 欄位。"""
    if name in df.columns:
        return
    if alts:
        for a in alts:
            if a in df.columns:
                df[name] = df[a]
                return
    df[name] = pd.Series(np.nan, index=df.index, dtype="float64")


def _roll_mean(g: pd.core.groupby.SeriesGroupBy, n: int, mp: Optional[int] = None) -> pd.Series:
    if mp is None:
        mp = max(2, n // 3)
    return g.transform(lambda s: s.rolling(n, min_periods=mp).mean())


def _roll_std(g: pd.core.groupby.SeriesGroupBy, n: int, mp: Optional[int] = None) -> pd.Series:
    if mp is None:
        mp = max(2, n // 3)
    return g.transform(lambda s: s.rolling(n, min_periods=mp).std())


def _pct_change(g: pd.core.groupby.SeriesGroupBy, n: int = 1) -> pd.Series:
    return g.pct_change(n)


def _zscore(series: pd.Series, g: pd.core.groupby.SeriesGroupBy, n: int) -> pd.Series:
    m = _roll_mean(g, n)
    s = _roll_std(g, n).replace(0, np.nan)
    return (series - m) / s


def _safe_ratio(a: pd.Series, b: pd.Series) -> pd.Series:
    return a / b.replace(0, np.nan)


def _pct_rank_within_day(df: pd.DataFrame, col: str) -> pd.Series:
    return df.groupby("date_utc")[col].rank(pct=True)


# ---------- main ----------
def compute_features(
    df_all: pd.DataFrame,
    out_start: date,
    out_end: date,
    log: Callable[[str], None] = print
) -> pd.DataFrame:

    df = df_all.sort_values(["asset", "ts_utc"]).copy()

    # 欄位別名對齊 schema
    # LSR
    if "lsr_top_accounts" in df.columns and "lsr_top_accts" not in df.columns:
        df = df.rename(columns={"lsr_top_accounts": "lsr_top_accts"})
    if "lsr_top_positions" in df.columns and "lsr_top_pos" not in df.columns:
        df = df.rename(columns={"lsr_top_positions": "lsr_top_pos"})

    # Orderbook 可能整段缺，補空欄
    _ensure_col(df, "ob_bids_usd", ["bids_usd"])
    _ensure_col(df, "ob_asks_usd", ["asks_usd"])
    _ensure_col(df, "ob_bids_qty", ["bids_qty"])
    _ensure_col(df, "ob_asks_qty", ["asks_qty"])

    # Taker / Liquidation 別名
    _ensure_col(df, "taker_buy_usd", ["taker_buy_vol_usd", "buy_vol_usd"])
    _ensure_col(df, "taker_sell_usd", ["taker_sell_vol_usd", "sell_vol_usd"])
    _ensure_col(df, "liq_long_usd", ["long_liq_usd", "liq_long_liq_usd"])
    _ensure_col(df, "liq_short_usd", ["short_liq_usd", "liq_short_liq_usd"])

    # ETF / Premium 別名
    _ensure_col(df, "etf_flow_usd", ["total_flow_usd"])
    _ensure_col(df, "etf_aum_usd", ["net_assets_usd"])
    _ensure_col(df, "etf_premdisc", ["premium_discount"])

    # 主要分組器
    g = df.groupby("asset", dropna=False, sort=False)

    # --- 微結構衍生 ---
    df["ob_imb"] = _safe_ratio(df["ob_bids_usd"], (df["ob_bids_usd"] + df["ob_asks_usd"]))
    df["depth_ratio_q"] = _safe_ratio(df["ob_bids_qty"], (df["ob_bids_qty"] + df["ob_asks_qty"]))
    df["taker_imb"] = _safe_ratio(df["taker_buy_usd"] - df["taker_sell_usd"],
                                  (df["taker_buy_usd"] + df["taker_sell_usd"]))
    df["liq_net"] = df["liq_long_usd"] - df["liq_short_usd"]

    # --- 價格動能 & 均線 ---
    df["ret_1d"] = _pct_change(g["px_close"], 1)

    for n in [3, 5, 10, 20, 60, 120, 252]:
        df[f"roc_{n}"] = _pct_change(g["px_close"], n)
        df[f"mom_{n}"] = df[f"roc_{n}"]

    for n in [10, 20, 60, 120, 252]:
        df[f"sma_{n}"] = _roll_mean(g["px_close"], n)

    # EMA / MACD
    df["ema_12"] = g["px_close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    df["ema_26"] = g["px_close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal_9"] = g["macd"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = df["macd"] - df["macd_signal_9"]

    # 布林
    bb_mid = df["sma_20"]
    bb_std = _roll_std(g["px_close"], 20)
    df["bb_mid_20"] = bb_mid
    df["bb_up_20"] = bb_mid + 2.0 * bb_std
    df["bb_dn_20"] = bb_mid - 2.0 * bb_std

    # ATR-14
    prev_close = g["px_close"].shift(1)
    tr1 = (df["px_high"] - df["px_low"]).abs()
    tr2 = (df["px_high"] - prev_close).abs()
    tr3 = (df["px_low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr_14"] = g["px_close"].transform(lambda s: 0.0)
    df["tr"] = tr
    df["atr_14"] = g["tr"].transform(lambda s: s.rolling(14, min_periods=2).mean())
    df.drop(columns=["tr"], inplace=True)

    # 已實現波動
    for n in [20, 60, 120]:
        df[f"rv_{n}"] = _roll_std(g["ret_1d"], n)

    # return z-scores
    for n in [20, 60, 120]:
        df[f"z_ret_{n}"] = _zscore(df["ret_1d"], g["ret_1d"], n)

    # --- OI / Funding / LSR ---
    if "oi_agg_close" in df.columns:
        df["d_oi_1"] = _pct_change(g["oi_agg_close"], 1)
        for n in [5, 20, 60]:
            df[f"oi_roc_{n}"] = _pct_change(g["oi_agg_close"], n)
        df["oi_z_60"] = _zscore(df["oi_agg_close"], g["oi_agg_close"], 60)

    if "funding_oiw_close" in df.columns:
        df["d_funding_1"] = _pct_change(g["funding_oiw_close"], 1)
        df["funding_ma_20"] = _roll_mean(g["funding_oiw_close"], 20)
        df["funding_ma_60"] = _roll_mean(g["funding_oiw_close"], 60)
        df["funding_z_60"]  = _zscore(df["funding_oiw_close"], g["funding_oiw_close"], 60)

    for col, ma_name, z_name in [
        ("lsr_global", "lsr_ma20_global", "lsr_z60_global"),
        ("lsr_top_accts", "lsr_ma20_top_accts", "lsr_z60_top_accts"),
        ("lsr_top_pos", "lsr_ma20_top_pos", "lsr_z60_top_pos"),
    ]:
        if col in df.columns:
            df[ma_name] = _roll_mean(g[col], 20)
            df[z_name]  = _zscore(df[col], g[col], 60)

    # Orderbook / Taker 延伸
    if "ob_imb" in df.columns:
        df["ob_imb_ma20"] = _roll_mean(g["ob_imb"], 20)
        df["ob_imb_z60"]  = _zscore(df["ob_imb"], g["ob_imb"], 60)

    if "depth_ratio_q" in df.columns:
        df["depth_ratio_q_ma20"] = _roll_mean(g["depth_ratio_q"], 20)
        df["depth_ratio_q_z60"]  = _zscore(df["depth_ratio_q"], g["depth_ratio_q"], 60)

    if "taker_imb" in df.columns:
        df["taker_imb_ma20"] = _roll_mean(g["taker_imb"], 20)
        df["taker_imb_z60"]  = _zscore(df["taker_imb"], g["taker_imb"], 60)

    if "taker_buy_usd" in df.columns:
        df["taker_buy_ma20"] = _roll_mean(g["taker_buy_usd"], 20)
        df["taker_buy_z60"]  = _zscore(df["taker_buy_usd"], g["taker_buy_usd"], 60)

    if "taker_sell_usd" in df.columns:
        df["taker_sell_ma20"] = _roll_mean(g["taker_sell_usd"], 20)
        df["taker_sell_z60"]  = _zscore(df["taker_sell_usd"], g["taker_sell_usd"], 60)

    if "liq_net" in df.columns:
        df["liq_z60"] = _zscore(df["liq_net"], g["liq_net"], 60)

    # --- ETF / 指數 ---
    if "etf_flow_usd" in df.columns:
        df["etf_flow_z60"] = _zscore(df["etf_flow_usd"], g["etf_flow_usd"], 60)

    if "etf_aum_usd" in df.columns:
        df["etf_aum_roc_5"]  = _pct_change(g["etf_aum_usd"], 5)
        df["etf_aum_roc_20"] = _pct_change(g["etf_aum_usd"], 20)

    if "etf_premdisc" in df.columns:
        df["premdisc_ma20"] = _roll_mean(g["etf_premdisc"], 20)
        df["premdisc_z60"]  = _zscore(df["etf_premdisc"], g["etf_premdisc"], 60)

    if "cpi_premium_rate" in df.columns:
        df["cpi_ma20"] = _roll_mean(g["cpi_premium_rate"], 20)
        df["cpi_z60"]  = _zscore(df["cpi_premium_rate"], g["cpi_premium_rate"], 60)

    if "bfx_long_qty" in df.columns and "bfx_short_qty" in df.columns:
        df["bfx_lr"] = _safe_ratio(df["bfx_long_qty"], df["bfx_short_qty"])
        df["bfx_lr_d1"] = _pct_change(g["bfx_lr"], 1)

    if "borrow_ir" in df.columns:
        df["borrow_ir_ma20"] = _roll_mean(g["borrow_ir"], 20)

    if "puell" in df.columns:
        df["puell_d1"] = _pct_change(g["puell"], 1)

    if "s2f_next_halving" in df.columns:
        df["s2f_d1"] = g["s2f_next_halving"].diff(1)

    if "pi_ma110" in df.columns:
        df["pi_ma110_d1"] = _pct_change(g["pi_ma110"], 1)
    if "pi_ma350x2" in df.columns:
        df["pi_ma350x2_d1"] = _pct_change(g["pi_ma350x2"], 1)

    # --- 橫截面 & 相對 BTC ---
    df["xsec_ret_rank"] = _pct_rank_within_day(df, "ret_1d")
    if "mom_20" in df.columns:
        df["xsec_mom_rank_20"] = _pct_rank_within_day(df, "mom_20")
    if "rv_60" in df.columns:
        df["xsec_vol_rank_60"] = _pct_rank_within_day(df, "rv_60")

    df["rel_to_btc"] = np.nan
    if "mom_20" in df.columns:
        btc_mask = df["asset"].astype(str).str.contains("BTC", case=False, na=False)
        if btc_mask.any():
            anchor = (
                df.loc[btc_mask, ["date_utc", "mom_20"]]
                  .dropna(subset=["mom_20"])
                  .groupby("date_utc", as_index=True)["mom_20"]
                  .first()
            )
            df["rel_to_btc"] = df["mom_20"] - df["date_utc"].map(anchor)

    # --- 視窗輸出 & 欄位順序 ---
    out = df[(df["date_utc"] >= out_start) & (df["date_utc"] <= out_end)].copy()

    schema_cols = [
        "asset","ts_utc","px_open","px_high","px_low","px_close","vol_usd",
        "oi_agg_close","oi_stable_close","oi_coinm_close",
        "funding_oiw_close","funding_volw_close",
        "lsr_global","lsr_top_accts","lsr_top_pos",
        "ob_bids_usd","ob_asks_usd","ob_bids_qty","ob_asks_qty",
        "ob_imb","depth_ratio_q",
        "taker_buy_usd","taker_sell_usd","taker_imb",
        "liq_long_usd","liq_short_usd","liq_net",
        "etf_flow_usd","etf_aum_usd","etf_premdisc",
        "cpi_premium_rate","bfx_long_qty","bfx_short_qty","borrow_ir",
        "puell","s2f_next_halving","pi_ma110","pi_ma350x2",
        "ret_1d",
        "roc_3","roc_5","roc_10","roc_20","roc_60","roc_120","roc_252",
        "mom_3","mom_5","mom_10","mom_20","mom_60","mom_120","mom_252",
        "sma_10","sma_20","sma_60","sma_120","sma_252",
        "ema_12","ema_26","macd","macd_signal_9","macd_hist",
        "bb_mid_20","bb_up_20","bb_dn_20",
        "atr_14","rv_20","rv_60","rv_120",
        "z_ret_20","z_ret_60","z_ret_120",
        "d_oi_1","oi_roc_5","oi_roc_20","oi_roc_60","oi_z_60",
        "d_funding_1","funding_ma_20","funding_ma_60","funding_z_60",
        "lsr_ma20_global","lsr_z60_global",
        "lsr_ma20_top_accts","lsr_z60_top_accts",
        "lsr_ma20_top_pos","lsr_z60_top_pos",
        "ob_imb_ma20","ob_imb_z60",
        "depth_ratio_q_ma20","depth_ratio_q_z60",
        "taker_imb_ma20","taker_imb_z60",
        "taker_buy_ma20","taker_sell_ma20",
        "taker_buy_z60","taker_sell_z60",
        "liq_z60","etf_flow_z60",
        "etf_aum_roc_5","etf_aum_roc_20",
        "premdisc_ma20","premdisc_z60",
        "cpi_ma20","cpi_z60",
        "bfx_lr","bfx_lr_d1",
        "borrow_ir_ma20","puell_d1","s2f_d1","pi_ma110_d1","pi_ma350x2_d1",
        "xsec_ret_rank","xsec_mom_rank_20","xsec_vol_rank_60",
        "rel_to_btc",
    ]
    present = ["asset","ts_utc","date_utc"] + [c for c in schema_cols if c in out.columns]
    log(f"[compute_features] input rows={len(df_all)}, cols={list(df_all.columns)}")
    tot = len(out)
    for c in present:
        if c in ("asset","ts_utc","date_utc"):
            continue
        nn = out[c].notna().sum()
        miss_pct = round(100.0 * (1 - nn / (tot or 1)), 1)
        log(f"[feature] {c}: non-null={nn}/{tot} ({miss_pct}% NaN)")

    return out[present].reset_index(drop=True)
