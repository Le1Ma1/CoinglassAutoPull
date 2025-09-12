import re
import numpy as np
import pandas as pd

# 規則與 loader 相同：去掉報價幣、分隔符；XBT→BTC
_TAIL_RE = re.compile(r'(USDT|USD|USDC|BUSD|TUSD|USDP|UST|PERP)$', re.I)
_SEP_RE  = re.compile(r'[-_/].*$', re.I)
def _asset_from_symbol(s: str) -> str:
    x = _SEP_RE.sub('', str(s).upper())
    x = _TAIL_RE.sub('', x)
    return 'BTC' if x == 'XBT' else x

def _pick_col(cols, *cands):
    cl = {c.lower(): c for c in cols}
    for k in cands:
        if k.lower() in cl:
            return cl[k.lower()]
    return None

def _ensure_asset_ts(df: pd.DataFrame) -> pd.DataFrame:
    """確保有 asset 與 ts_utc 欄，必要時從 symbol 或索引派生。"""
    if df is None or len(df) == 0:
        return df
    out = df.copy()

    # 若 asset/時間在索引，先展成欄位
    if isinstance(out.index, pd.MultiIndex):
        idx_names = [n.lower() if isinstance(n, str) else None for n in out.index.names]
        if ("asset" in idx_names) or ("ts_utc" in idx_names) or ("date_utc" in idx_names) \
           or ("timestamp" in idx_names) or ("time" in idx_names):
            out = out.reset_index()
    elif out.index.name and str(out.index.name).lower() in ("asset","ts_utc","date_utc","timestamp","time"):
        out = out.reset_index()

    # asset 欄
    if "asset" not in out.columns:
        sym = _pick_col(out.columns, "symbol")
        if sym is not None:
            out["asset"] = out[sym].map(_asset_from_symbol)
        else:
            # 單一全域指標（如 CPI/ETF/指標）
            out["asset"] = "BTC"

    # ts_utc 欄
    if "ts_utc" not in out.columns:
        tcol = _pick_col(out.columns, "ts_utc", "timestamp", "time", "date_utc")
        if tcol is None:
            # 再嘗試：若索引本身是 DatetimeIndex（前面 reset 失敗的保險）
            if isinstance(out.index, pd.DatetimeIndex):
                out["ts_utc"] = pd.to_datetime(out.index, utc=True)
            else:
                raise KeyError(f"source missing time column among {out.columns.tolist()}")
        else:
            if tcol.lower() == "date_utc":
                out["ts_utc"] = pd.to_datetime(out[tcol]).dt.tz_localize("UTC")
            else:
                out["ts_utc"] = pd.to_datetime(out[tcol], utc=True)

    return out

def _merge_on_index(df: pd.DataFrame, other: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """將 other 的指定欄位，依 (asset, ts_utc) 左合併到 df（df 已以此為 MultiIndex）。"""
    if other is None or len(other) == 0:
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
        return df

    other = _ensure_asset_ts(other)
    need = ["asset", "ts_utc", *cols]
    # 缺值欄補空
    for c in cols:
        if c not in other.columns:
            other[c] = np.nan
    other = other[need].drop_duplicates(subset=["asset", "ts_utc"], keep="last")

    return df.merge(
        other.set_index(["asset", "ts_utc"]),
        left_index=True, right_index=True, how="left"
    )

# 新增在檔內 _merge_on_index 旁
def _merge_on_base(df: pd.DataFrame, other: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """以『基礎資產』(去掉報價幣/分隔符，XBT->BTC) + ts_utc 左合併，並把來源值廣播到同基礎資產的所有交易對。"""
    import numpy as np
    if other is None or len(other) == 0:
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
        return df

    other = _ensure_asset_ts(other).copy()
    for c in cols:
        if c not in other.columns:
            other[c] = np.nan
    other["asset_base"] = other["asset"].map(_asset_from_symbol)
    other = other[["asset_base", "ts_utc", *cols]].drop_duplicates(["asset_base", "ts_utc"], keep="last")

    tmp = df.reset_index().copy()
    tmp["asset_base"] = tmp["asset"].map(_asset_from_symbol)

    out = tmp.merge(other, on=["asset_base", "ts_utc"], how="left").drop(columns=["asset_base"])
    return out.set_index(["asset","ts_utc"]).sort_index()

def build_price_coordinate(fut: pd.DataFrame | None, spot: pd.DataFrame | None) -> pd.DataFrame:
    """建立日價格骨幹座標（期貨+現貨 union 去重）。"""
    frames = []
    for k, d in (("fut", fut), ("spot", spot)):
        if d is None or len(d) == 0:
            continue
        tcol = _pick_col(d.columns, "ts_utc", "timestamp", "time", "date_utc")
        if tcol is None:
            continue
        tmp = d.copy()

        # 統一 asset 命名規則 → 必定輸出 XXXUSDT
        if "asset" not in tmp.columns:
            sym = _pick_col(tmp.columns, "symbol")
            if sym:
                tmp["asset"] = tmp[sym].map(_asset_from_symbol)  # e.g. BTC, ETH
            else:
                tmp["asset"] = "BTC"
        tmp["asset"] = tmp["asset"].astype(str).str.upper()
        # 統一保證帶 USDT
        tmp["asset"] = tmp["asset"].apply(lambda x: x if x.endswith("USDT") else x + "USDT")

        # 正規化時間
        if tcol.lower() == "date_utc":
            tmp["ts_utc"] = pd.to_datetime(tmp[tcol]).dt.tz_localize("UTC")
        else:
            tmp["ts_utc"] = pd.to_datetime(tmp[tcol], utc=True)

        # 對每資產×日選一筆代表（收盤那筆），同時帶上 OHLCV 欄位名相容
        cols_map = {
            _pick_col(tmp.columns, "open"):  "open",
            _pick_col(tmp.columns, "high"):  "high",
            _pick_col(tmp.columns, "low"):   "low",
            _pick_col(tmp.columns, "close"): "close",
            _pick_col(tmp.columns, "volume_usd", "vol_usd", "volume"): "volume_usd",
        }
        cols_map = {k: v for k, v in cols_map.items() if k is not None}
        keep = ["asset", "ts_utc", *cols_map.keys()]
        tmp = tmp[keep].rename(columns=cols_map)
        frames.append(tmp)

    if not frames:
        raise RuntimeError("no price inputs")
    px = pd.concat(frames, ignore_index=True)
    px = px.sort_values(["asset", "ts_utc"]).drop_duplicates(["asset", "ts_utc"], keep="last")
    # 設定索引，僅保留座標與最少欄位，後續左連其他來源
    px = px.set_index(["asset", "ts_utc"]).sort_index()
    px = px.rename(columns={
        "open": "px_open",
        "high": "px_high",
        "low": "px_low",
        "close": "px_close",
        "volume_usd": "vol_usd"
    })
    return px

def left_join_all(px: pd.DataFrame, S: dict) -> pd.DataFrame:
    """按骨幹座標把所有來源左連進來。"""
    df = px.copy()

    # OI
    if S.get("oi") is not None:
        oi = _ensure_asset_ts(S["oi"]).rename(columns={
            "oi_total_close": "oi_agg_close",
            "oi_stable_close": "oi_stable_close",
            "oi_coinm_close": "oi_coinm_close",
        })
        df = _merge_on_base(df, oi, ["oi_agg_close", "oi_stable_close", "oi_coinm_close"])

    # funding
    if S.get("funding_oiw") is not None:
        df = _merge_on_base(df, _ensure_asset_ts(S["funding_oiw"]).rename(columns={"funding_close":"funding_oiw_close"}), ["funding_oiw_close"])
    if S.get("funding_volw") is not None:
        df = _merge_on_base(df, _ensure_asset_ts(S["funding_volw"]).rename(columns={"funding_close":"funding_volw_close"}), ["funding_volw_close"])

    # long/short
    if S.get("lsr") is not None:
        lsr = _ensure_asset_ts(S["lsr"]).rename(columns={
            "lsr_global":"lsr_global",
            "lsr_top_accounts":"lsr_top_accts",
            "lsr_top_positions":"lsr_top_pos",
        })
        df = _merge_on_base(df, lsr, ["lsr_global","lsr_top_accts","lsr_top_pos"])

    # orderbook
    if S.get("ob") is not None:
        ob = _ensure_asset_ts(S["ob"]).query("range_pct==1 or range_pct==1.0")
        ob = ob.rename(columns={"bids_usd":"ob_bids_usd","asks_usd":"ob_asks_usd","bids_qty":"ob_bids_qty","asks_qty":"ob_asks_qty"})
        df = _merge_on_base(df, ob, ["ob_bids_usd","ob_asks_usd","ob_bids_qty","ob_asks_qty"])

    # taker
    if S.get("taker") is not None:
        tk = _ensure_asset_ts(S["taker"])
        df = _merge_on_base(df, tk, ["taker_buy_usd","taker_sell_usd"])

    # liquidation
    if S.get("liq") is not None:
        lq = _ensure_asset_ts(S["liq"])
        df = _merge_on_base(df, lq, ["liq_long_usd","liq_short_usd"])

    # ETF（BTC 全域）
    if S.get("etf") is not None:
        etf = _ensure_asset_ts(S["etf"])
        etf = etf.rename(columns={"flow_usd":"etf_flow_usd","aum_usd":"etf_aum_usd","premium_discount":"etf_premdisc"})
        df = _merge_on_base(df, etf, ["etf_flow_usd","etf_aum_usd","etf_premdisc"])

    # CPI（BTC 全域）
    if S.get("cpi") is not None:
        cpi = _ensure_asset_ts(S["cpi"]).rename(columns={"premium_rate":"cpi_premium_rate"})
        df = _merge_on_base(df, cpi, ["cpi_premium_rate"])

    # Bitfinex margin（BTC 全域或有 symbol）
    if S.get("bfx") is not None:
        bfx = _ensure_asset_ts(S["bfx"])
        df = _merge_on_base(df, bfx.rename(columns={"long_qty":"bfx_long_qty","short_qty":"bfx_short_qty"}),
                             ["bfx_long_qty","bfx_short_qty"])

    # Borrow IR
    if S.get("bir") is not None:
        bir = _ensure_asset_ts(S["bir"])
        df = _merge_on_base(df, bir, ["borrow_ir"])

    # 指標（BTC 全域）
    if S.get("puell") is not None:
        df = _merge_on_base(df, _ensure_asset_ts(S["puell"]), ["puell"])

    if S.get("s2f") is not None:
        s2f = _ensure_asset_ts(S["s2f"])
        # 與雲端 schema 對齊：統一用 s2f_next_halving
        if "s2f_next_halving" not in s2f.columns and "s2f" in s2f.columns:
            s2f = s2f.rename(columns={"s2f": "s2f_next_halving"})
        df = _merge_on_base(df, s2f, ["s2f_next_halving"])

    if S.get("pi") is not None:
        pi = _ensure_asset_ts(S["pi"])
        df = _merge_on_base(df, pi, ["pi_ma110","pi_ma350x2"])

    return df
