# -*- coding: utf-8 -*-
import pandas as pd
from typing import Dict, Optional, List

def _norm_sym(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.upper()
    out = out.str.replace(":USDT", "", regex=False)
    out = out.str.replace("USDT", "", regex=False)
    return out

def build_price_coordinate(fut: Optional[pd.DataFrame], spot: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    以期貨為主、現貨為輔建立價格座標（asset, ts_utc, date_utc）。
    不變更你的原始資料，只在欄位層面 rename 成 px_* 方便後續特徵計算。
    """
    frames: List[pd.DataFrame] = []

    if fut is not None and len(fut):
        f = fut.copy()
        f["asset"] = _norm_sym(f["symbol"])
        f = f[["asset","ts_utc","date_utc","open","high","low","close","volume_usd"]]
        f = f.rename(columns={
            "open":"px_open","high":"px_high","low":"px_low","close":"px_close","volume_usd":"vol_usd"
        })
        frames.append(f)

    if spot is not None and len(spot):
        s = spot.copy()
        s["asset"] = _norm_sym(s["symbol"])
        s = s[["asset","ts_utc","date_utc","open","high","low","close","volume_usd"]]
        s = s.rename(columns={
            "open":"px_open_s","high":"px_high_s","low":"px_low_s","close":"px_close_s","volume_usd":"vol_usd_s"
        })
        frames.append(s)

    if not frames:
        return pd.DataFrame(columns=["asset","ts_utc","date_utc","px_open","px_high","px_low","px_close","vol_usd"])

    base = frames[0]
    for f in frames[1:]:
        base = base.merge(f, how="outer", on=["asset","ts_utc","date_utc"])

    # 期貨優先，沒有再用現貨
    for k in ["px_open","px_high","px_low","px_close","vol_usd"]:
        sfx = "" if k != "vol_usd" else ""
        base[k] = base.get(k, pd.Series(index=base.index))
        alt = base.get(k + "_s")
        base[k] = base[k].where(base[k].notna(), alt)
        if k + "_s" in base:
            base = base.drop(columns=[k+"_s"])

    base = base.sort_values(["asset","ts_utc"]).reset_index(drop=True)
    return base[["asset","ts_utc","date_utc","px_open","px_high","px_low","px_close","vol_usd"]]

def _prep_symbol_df(df: pd.DataFrame, keep_cols: List[str]) -> pd.DataFrame:
    x = df.copy()
    if "symbol" in x.columns:
        x["asset"] = _norm_sym(x["symbol"])
    if "date_utc" not in x.columns:
        raise ValueError("來源缺少 date_utc 欄位")
    cols = [c for c in keep_cols if c in x.columns]
    cols = (["asset","date_utc"] + (["ts_utc"] if "ts_utc" in x.columns else []) + cols)
    x = x[cols].drop_duplicates(subset=["asset","date_utc"], keep="last")
    return x

def left_join_all(base: pd.DataFrame, S: Dict[str, Optional[pd.DataFrame]]) -> pd.DataFrame:
    out = base.copy()

    def j(df: Optional[pd.DataFrame], cols_map: Dict[str,str], label: str):
        nonlocal out
        if df is None or not len(df): 
            return
        keep = list(cols_map.keys())
        x = _prep_symbol_df(df, keep_cols=keep)
        x = x.rename(columns=cols_map)
        before_cols = set(out.columns)
        out = out.merge(x.drop(columns=[c for c in ["ts_utc"] if c in x.columns]), how="left", on=["asset","date_utc"])
        added = [c for c in out.columns if c not in before_cols]
        print(f"[merge_on_base] before join: df={len(base)}, other={len(x)}, cols={added}")
        print(f"[merge_on_base] after join: out={len(out)}, added_cols={added}")
        return out

    # ===== OI =====
    out = j(S.get("oi_agg"),    {"close":"oi_agg_close"},      "oi_agg") or out
    out = j(S.get("oi_stable"), {"close":"oi_stable_close"},   "oi_stable") or out
    out = j(S.get("oi_coinm"),  {"close":"oi_coinm_close"},    "oi_coinm") or out

    # ===== Funding =====
    out = j(S.get("funding_oiw"),   {"close":"funding_oiw_close"},  "funding_oiw") or out
    out = j(S.get("funding_volw"),  {"close":"funding_volw_close"}, "funding_volw") or out

    # ===== L/S Ratio =====
    out = j(S.get("lsr_g"), {"long_short_ratio":"lsr_global"}, "lsr_global") or out
    out = j(S.get("lsr_a"), {"long_short_ratio":"lsr_top_accounts"}, "lsr_top_accounts") or out
    out = j(S.get("lsr_p"), {"long_short_ratio":"lsr_top_positions"}, "lsr_top_positions") or out

    # ===== Orderbook / Taker / Liquidations =====
    out = j(S.get("ob"),    {"bids_usd":"ob_bids_usd","asks_usd":"ob_asks_usd","bids_qty":"ob_bids_qty","asks_qty":"ob_asks_qty"}, "ob") or out
    out = j(S.get("taker"), {"buy_vol_usd":"taker_buy_usd","sell_vol_usd":"taker_sell_usd"}, "taker") or out
    out = j(S.get("liq"),   {"long_liq_usd":"liq_long_usd","short_liq_usd":"liq_short_usd"}, "liq") or out

    # ===== Coinbase Premium：BTC 專屬（沒有 symbol），指派到 BTC =====
    if S.get("cpi") is not None and len(S["cpi"]):
        x = S["cpi"].copy()
        x["asset"] = "BTC"
        x = x[["asset","date_utc","premium_rate"]].rename(columns={"premium_rate":"cpi_premium_rate"})
        before_cols = set(out.columns)
        out = out.merge(x, how="left", on=["asset","date_utc"])
        added = [c for c in out.columns if c not in before_cols]
        print(f"[merge_on_base] before join: df={len(base)}, other={len(x)}, cols={added}")
        print(f"[merge_on_base] after join: out={len(out)}, added_cols={added}")

    # ===== Bitfinex Margin L/S =====
    out = j(S.get("bfx"), {"long_qty":"bfx_long_qty","short_qty":"bfx_short_qty"}, "bfx") or out

    # ===== Borrow interest rate =====
    out = j(S.get("bir"), {"interest_rate":"borrow_ir"}, "bir") or out

    # ===== Puell / S2F / PI：BTC 專屬 =====
    for key, cmap in [
        ("puell", {"puell_multiple":"puell"}),
        ("s2f",   {"next_halving":"s2f_next_halving"}),
        ("pi",    {"ma_110":"pi_ma110","ma_350_x2":"pi_ma350x2"}),
    ]:
        df = S.get(key)
        if df is not None and len(df):
            x = df.copy()
            x["asset"] = "BTC"
            keep_map = {k:v for k,v in cmap.items() if k in x.columns}
            cols = ["asset","date_utc"] + list(keep_map.keys())
            x = x[cols].rename(columns=keep_map)
            before_cols = set(out.columns)
            out = out.merge(x, how="left", on=["asset","date_utc"])
            added = [c for c in out.columns if c not in before_cols]
            print(f"[merge_on_base] before join: df={len(base)}, other={len(x)}, cols={added}")
            print(f"[merge_on_base] after join: out={len(out)}, added_cols={added}")

    return out
