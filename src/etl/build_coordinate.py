# -*- coding: utf-8 -*-
"""
build_coordinate.py
- 建立日頻價格座標（asset, ts_utc, date_utc）
- 依來源左連：自動偵測 keys（優先 ['asset','ts_utc','date_utc']；若來源沒有 ts_utc 或 asset，會退回以 date_utc 合併並廣播到所有資產）
- 內建 LSR 三表合併；ETF 類（flow/aum/prem）先按日聚合
"""

from __future__ import annotations
import pandas as pd
from typing import Dict, Optional, List

BASE_KEYS = ["asset", "ts_utc", "date_utc"]

def _ensure_cols(df: pd.DataFrame, must: List[str]) -> pd.DataFrame:
    missing = [c for c in must if c not in df.columns]
    if missing:
        raise KeyError(f"missing columns: {missing}")
    return df

def _prep_price_table(spot: Optional[pd.DataFrame], fut: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    以 spot 為主（若有），fallback 用 fut；輸出 (asset, ts_utc, date_utc, px_open/px_high/px_low/px_close, vol_usd)
    """
    pieces = []
    if spot is not None and len(spot):
        s = spot.copy()
        s = _ensure_cols(s, ["symbol", "ts_utc", "date_utc", "open", "high", "low", "close", "volume_usd"])
        s = s.rename(columns={
            "symbol": "asset",
            "open": "px_open",
            "high": "px_high",
            "low": "px_low",
            "close": "px_close",
            "volume_usd": "vol_usd",
        })
        pieces.append(s[["asset", "ts_utc", "date_utc", "px_open", "px_high", "px_low", "px_close", "vol_usd"]])

    if fut is not None and len(fut):
        f = fut.copy()
        f = _ensure_cols(f, ["symbol", "ts_utc", "date_utc", "open", "high", "low", "close", "volume_usd"])
        f = f.rename(columns={
            "symbol": "asset",
            "open": "px_open",
            "high": "px_high",
            "low": "px_low",
            "close": "px_close",
            "volume_usd": "vol_usd",
        })
        pieces.append(f[["asset", "ts_utc", "date_utc", "px_open", "px_high", "px_low", "px_close", "vol_usd"]])

    if not pieces:
        raise ValueError("spot / futures 皆為空，無法建立價格座標")

    # 先縱向併，再以 (asset, ts_utc) 去重，spot 先於 fut 放入 -> spot 優先
    px = pd.concat(pieces, ignore_index=True)
    px = px.sort_values(["asset", "ts_utc"])
    px = px.drop_duplicates(subset=["asset", "ts_utc"], keep="first").reset_index(drop=True)
    return px

def build_price_coordinate(sources: Dict[str, Optional[pd.DataFrame]]) -> pd.DataFrame:
    return _prep_price_table(sources.get("spot"), sources.get("fut"))

def _merge_on_base(
    base: pd.DataFrame,
    other: Optional[pd.DataFrame],
    on: List[str],
    add_prefix: Optional[str],
    cols_keep: List[str],
    how: str = "left",
    log=print,
) -> pd.DataFrame:
    """
    聰明合併：
    - 若 other 為 None 或空 -> 原樣返回
    - 先把 'symbol' 轉為 'asset'
    - 偵測 other 具備的 key：優先使用 ['asset','ts_utc','date_utc']；若缺 ts_utc 或 asset，會退化為 ['date_utc']（將數值廣播到所有 asset）
    - 僅保留 keep 的數值欄位；必要 keys 做去重
    """
    if other is None or not len(other):
        return base

    x = other.copy()
    if "symbol" in x.columns and "asset" not in x.columns:
        x = x.rename(columns={"symbol": "asset"})

    # 具體使用哪些 keys 來 join
    keys = [k for k in on if k in x.columns]
    if "ts_utc" not in keys and "date_utc" in x.columns:
        # 對於只有 date_utc 的「日頻指標」（ETF/puell/s2f/pi 等），用 date_utc 合併並廣播
        keys = ["date_utc"]

    # 精簡、去重
    keep_cols = [c for c in cols_keep if c in x.columns]
    x = x[keys + keep_cols].sort_values(keys).drop_duplicates(keys, keep="last")

    # 加前綴（若有）
    if add_prefix:
        x = x.rename(columns={c: f"{add_prefix}{c}" for c in keep_cols})
        keep_cols = [f"{add_prefix}{c}" for c in keep_cols]

    log(f"[merge_on_base] before join: df={len(base)}, other={len(x)}, cols={keep_cols}")
    out = base.merge(x[keys + keep_cols], how=how, on=keys)
    log(f"[merge_on_base] after join: out={len(out)}, added_cols={keep_cols}")
    return out

def _agg_mean_by_date(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """將含其它維度（如 ticker）的日表，彙總為 date_utc 單列（取平均/總和可自行換）"""
    g = (
        df.groupby("date_utc", as_index=False)[value_col]
        .mean()
        .rename(columns={value_col: value_col})
    )
    return g

def _merge_lsr(lsr_g: Optional[pd.DataFrame], lsr_a: Optional[pd.DataFrame], lsr_p: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    frames = []
    if lsr_g is not None and len(lsr_g):
        x = lsr_g.rename(columns={"symbol": "asset", "long_short_ratio": "lsr_global"})
        frames.append(x[["asset", "ts_utc", "date_utc", "lsr_global"]])
    if lsr_a is not None and len(lsr_a):
        x = lsr_a.rename(columns={"symbol": "asset", "long_short_ratio": "lsr_top_accounts"})
        frames.append(x[["asset", "ts_utc", "date_utc", "lsr_top_accounts"]])
    if lsr_p is not None and len(lsr_p):
        x = lsr_p.rename(columns={"symbol": "asset", "long_short_ratio": "lsr_top_positions"})
        frames.append(x[["asset", "ts_utc", "date_utc", "lsr_top_positions"]])

    if not frames:
        return None

    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, how="outer", on=["asset", "ts_utc", "date_utc"])
    return out

def left_join_all(base_px: pd.DataFrame, S: Dict[str, Optional[pd.DataFrame]], log=print) -> pd.DataFrame:
    """
    入參：
      base_px: 由 build_price_coordinate 輸出的價格骨幹
      S: 各來源資料 dict
    """
    df = base_px.copy()

    # ---- OI ----
    if S.get("oi") is not None:
        x = S["oi"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["close"], log=log)
        df = df.rename(columns={"close": "oi_agg_close"})

    # ---- OI - stable / coin-m ----
    if S.get("oi_stable") is not None:
        x = S["oi_stable"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["close"], log=log)
        df = df.rename(columns={"close": "oi_stable_close"})

    if S.get("oi_coinm") is not None:
        x = S["oi_coinm"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["close"], log=log)
        df = df.rename(columns={"close": "oi_coinm_close"})

    # ---- Funding (oi/vol 加權) ----
    if S.get("funding_oiw") is not None:
        x = S["funding_oiw"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["close"], log=log)
        df = df.rename(columns={"close": "funding_oiw_close"})

    if S.get("funding_volw") is not None:
        x = S["funding_volw"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["close"], log=log)
        df = df.rename(columns={"close": "funding_volw_close"})

    # ---- LSR 三張表合併 ----
    lsr_all = _merge_lsr(S.get("lsr_g"), S.get("lsr_a"), S.get("lsr_p"))
    if lsr_all is not None:
        df = _merge_on_base(df, lsr_all, BASE_KEYS, add_prefix=None,
                            cols_keep=["lsr_global", "lsr_top_accounts", "lsr_top_positions"], log=log)

    # ---- Orderbook / Taker / Liq ----
    if S.get("ob") is not None:
        x = S["ob"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="ob_",
                            cols_keep=["bids_usd", "asks_usd", "bids_qty", "asks_qty"], log=log)

    if S.get("taker") is not None:
        x = S["taker"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="taker_",
                            cols_keep=["buy_vol_usd", "sell_vol_usd"], log=log)

    if S.get("liq") is not None:
        x = S["liq"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="liq_",
                            cols_keep=["long_liq_usd", "short_liq_usd"], log=log)

    # ---- Coinbase Premium Index（可用 date_utc 也可用 ts_utc，這裡保守起見仍交給偵測）----
    if S.get("cpi") is not None:
        df = _merge_on_base(df, S["cpi"], BASE_KEYS, add_prefix="cpi_",
                            cols_keep=["premium_rate"], log=log)

    # ---- Bitfinex margin long/short ----
    if S.get("bfx") is not None:
        x = S["bfx"].rename(columns={"symbol": "asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="bfx_",
                            cols_keep=["long_qty", "short_qty"], log=log)

    # ---- Borrow interest rate（跨交易所 -> 先按日/資產平均）----
    if S.get("bir") is not None:
        x = S["bir"].copy()
        x = _ensure_cols(x, ["symbol", "ts_utc", "date_utc", "interest_rate"])
        x = x.rename(columns={"symbol": "asset"})
        # 同一 ts_utc 可能有多交易所，先平均
        x = x.groupby(["asset", "ts_utc", "date_utc"], as_index=False)["interest_rate"].mean()
        x = x.rename(columns={"interest_rate": "borrow_ir"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["borrow_ir"], log=log)

    # ---- Puell / S2F / PI（只有 date_utc -> 自動以 date_utc 併入，廣播到所有資產）----
    if S.get("puell") is not None:
        x = S["puell"].rename(columns={"puell_multiple": "puell"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["puell"], log=log)

    if S.get("s2f") is not None:
        x = S["s2f"].rename(columns={"next_halving": "s2f_next_halving"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["s2f_next_halving"], log=log)

    if S.get("pi") is not None:
        x = S["pi"].rename(columns={"ma_110": "pi_ma110", "ma_350_x2": "pi_ma350x2"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["pi_ma110", "pi_ma350x2"], log=log)

    # ---- ETF（只有 date_utc；prem 有 ticker -> 取當日平均）----
    if S.get("etf_flow") is not None and len(S["etf_flow"]):
        g = _agg_mean_by_date(S["etf_flow"].rename(columns={"total_flow_usd": "etf_flow_usd"}), "etf_flow_usd")
        df = _merge_on_base(df, g, BASE_KEYS, add_prefix=None, cols_keep=["etf_flow_usd"], log=log)

    if S.get("etf_aum") is not None and len(S["etf_aum"]):
        g = _agg_mean_by_date(S["etf_aum"].rename(columns={"net_assets_usd": "etf_aum_usd"}), "etf_aum_usd")
        df = _merge_on_base(df, g, BASE_KEYS, add_prefix=None, cols_keep=["etf_aum_usd"], log=log)

    if S.get("etf_prem") is not None and len(S["etf_prem"]):
        e = S["etf_prem"].rename(columns={"premium_discount": "etf_premdisc"})
        g = e.groupby("date_utc", as_index=False)["etf_premdisc"].mean()
        df = _merge_on_base(df, g, BASE_KEYS, add_prefix=None, cols_keep=["etf_premdisc"], log=log)

    if S.get("etf_hk") is not None and len(S["etf_hk"]):
        # 若後續要用，可在 compute_features 內引用；這裡僅先入欄（名為 hk_etf_flow_usd -> ext_features 也可）
        g = _agg_mean_by_date(S["etf_hk"].rename(columns={"total_flow_usd": "hk_etf_flow_usd"}), "hk_etf_flow_usd")
        df = _merge_on_base(df, g, BASE_KEYS, add_prefix=None, cols_keep=["hk_etf_flow_usd"], log=log)

    # 依舊保持 base 的欄位順序（價量在前）
    value_cols = [c for c in df.columns if c not in BASE_KEYS]
    return df[BASE_KEYS + value_cols]
