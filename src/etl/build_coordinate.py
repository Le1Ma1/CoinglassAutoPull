# -*- coding: utf-8 -*-
"""
建骨幹價格座標 + 左連各來源。
此檔只服務特徵 ETL，不改動任何原始表。
"""
from __future__ import annotations
import pandas as pd

BASE_KEYS = ["asset","ts_utc","date_utc"]

def _norm_price(df: pd.DataFrame, src_name: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=BASE_KEYS + ["px_open","px_high","px_low","px_close","vol_usd"])
    x = df.copy()
    x = x.rename(columns={"symbol":"asset","open":"px_open","high":"px_high","low":"px_low","close":"px_close","volume_usd":"vol_usd"})
    return x[["asset","ts_utc","date_utc","px_open","px_high","px_low","px_close","vol_usd"]]

def build_price_coordinate(S: dict[str,pd.DataFrame]) -> pd.DataFrame:
    spot = _norm_price(S.get("spot"), "spot")
    fut  = _norm_price(S.get("fut"), "fut")
    # 以 spot 為主、fut 補值
    if len(spot) == 0 and len(fut) == 0:
        return pd.DataFrame(columns=BASE_KEYS + ["px_open","px_high","px_low","px_close","vol_usd"])
    base = pd.concat([spot, fut], ignore_index=True)
    base = base.sort_values(["asset","date_utc","ts_utc"]).drop_duplicates(["asset","date_utc"], keep="last")
    base = base.sort_values(["asset","date_utc"]).reset_index(drop=True)
    return base

def _merge_on_base(base: pd.DataFrame, other: pd.DataFrame, on: list[str], how="left", add_prefix=None, cols_keep=None, log=print):
    if other is None or len(other) == 0:
        return base
    keep_cols = cols_keep if cols_keep is not None else [c for c in other.columns if c not in on]
    log(f"[merge_on_base] before join: df={len(base)}, other={len(other)}, cols={keep_cols}")
    out = base.merge(other[on + keep_cols], how=how, on=on)
    added = keep_cols
    if add_prefix:
        rename_map = {c: f"{add_prefix}{c}" for c in keep_cols}
        out = out.rename(columns=rename_map)
        added = list(rename_map.values())
    log(f"[merge_on_base] after join: out={len(out)}, added_cols={added}")
    return out

def left_join_all(base: pd.DataFrame, S: dict[str,pd.DataFrame], log=print) -> pd.DataFrame:
    df = base.copy()

    # OI 三類
    if S.get("oi") is not None:
        oi = S["oi"].rename(columns={"symbol":"asset"})
        oi = oi[["asset","ts_utc","date_utc","close"]]
        df = _merge_on_base(df, oi, BASE_KEYS, add_prefix="oi_agg_", cols_keep=["close"], log=log)
    if S.get("oi_stable") is not None:
        x = S["oi_stable"].rename(columns={"symbol":"asset"})
        x = x[["asset","ts_utc","date_utc","close"]]
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="oi_stable_", cols_keep=["close"], log=log)
    if S.get("oi_coinm") is not None:
        x = S["oi_coinm"].rename(columns={"symbol":"asset"})
        x = x[["asset","ts_utc","date_utc","close"]]
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="oi_coinm_", cols_keep=["close"], log=log)

    # funding
    if S.get("funding_oiw") is not None:
        x = S["funding_oiw"].rename(columns={"symbol":"asset"})
        x = x[["asset","ts_utc","date_utc","close"]]
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="funding_oiw_", cols_keep=["close"], log=log)

    if S.get("funding_volw") is not None:
        x = S["funding_volw"].rename(columns={"symbol":"asset"})
        x = x[["asset","ts_utc","date_utc","close"]]
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="funding_volw_", cols_keep=["close"], log=log)

    # long/short 三張合併後傳進來
    if S.get("lsr") is not None and len(S["lsr"]):
        x = S["lsr"].rename(columns={"symbol":"asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=[c for c in x.columns if c not in ["asset","ts_utc","date_utc"]], log=log)

    # orderbook
    if S.get("ob") is not None:
        x = S["ob"].rename(columns={"symbol":"asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="ob_", cols_keep=["bids_usd","asks_usd","bids_qty","asks_qty"], log=log)

    # taker
    if S.get("taker") is not None:
        x = S["taker"].rename(columns={"symbol":"asset"})
        x = x.rename(columns={"buy_vol_usd":"taker_buy_usd","sell_vol_usd":"taker_sell_usd"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["taker_buy_usd","taker_sell_usd"], log=log)

    # liquidation
    if S.get("liq") is not None:
        x = S["liq"].rename(columns={"symbol":"asset"})
        x = x.rename(columns={"long_liq_usd":"liq_long_usd","short_liq_usd":"liq_short_usd"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["liq_long_usd","liq_short_usd"], log=log)

    # CPI (coinbase premium index)
    if S.get("cpi") is not None:
        x = S["cpi"].copy()
        x["asset"] = "BTC"  # 該指標只對 BTC，有值就貼在 BTC 上
        x = x[["asset","ts_utc","date_utc","premium_rate"]]
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="cpi_", cols_keep=["premium_rate"], log=log)

    # Bitfinex margin L/S
    if S.get("bfx") is not None:
        x = S["bfx"].rename(columns={"symbol":"asset"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix="bfx_", cols_keep=["long_qty","short_qty"], log=log)

    # Borrow interest rate
    if S.get("bir") is not None:
        x = S["bir"].rename(columns={"symbol":"asset"})
        x = x.rename(columns={"interest_rate":"borrow_ir"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["borrow_ir"], log=log)

    # Macro/indices (BTC 專屬)
    if S.get("puell") is not None:
        x = S["puell"].copy()
        x["asset"] = "BTC"
        x = x.rename(columns={"puell_multiple":"puell"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["puell"], log=log)

    if S.get("s2f") is not None:
        x = S["s2f"].copy()
        x["asset"] = "BTC"
        x = x.rename(columns={"next_halving":"s2f_next_halving"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["s2f_next_halving"], log=log)

    if S.get("pi") is not None:
        x = S["pi"].copy()
        x["asset"] = "BTC"
        x = x.rename(columns={"ma_110":"pi_ma110","ma_350_x2":"pi_ma350x2"})
        df = _merge_on_base(df, x, BASE_KEYS, add_prefix=None, cols_keep=["pi_ma110","pi_ma350x2"], log=log)

    return df
