import os
import pandas as pd
from pathlib import Path

# 必要：期貨/現貨 K 線
REQ = {"futures_ohlcv_1d":"fut", "spot_ohlcv_1d":"spot"}
# 允許別名
ALIASES = {
  "futures_ohlcv_1d": ["futures_candles_1d","futures"],
  "spot_ohlcv_1d":    ["spot_candles_1d","spot"],
  "futures_oi_agg_1d":["oi_agg_1d"],
  "funding_oi_weight_1d":["funding_1d"],
  "funding_vol_weight_1d":["funding_1d"],
}

# 選擇性來源
OPT = {
  "futures_oi_agg_1d":"oi",
  "funding_oi_weight_1d":"funding_oiw",
  "funding_vol_weight_1d":"funding_volw",
  "long_short_all_1d":"lsr",
  "orderbook_agg_futures_1d":"ob",
  "taker_vol_agg_futures_1d":"taker",
  "liquidation_agg_1d":"liq",
  "etf_all_1d":"etf",
  "coinbase_premium_index_1d":"cpi",            # 無 symbol，BTC 全域
  "bitfinex_margin_long_short_1d":"bfx",
  "borrow_interest_rate_1d":"bir",              # 含 exchange
  "idx_puell_multiple_daily":"puell",           # 只有 date_utc，BTC 全域
  "idx_stock_to_flow_daily":"s2f",              # 只有 date_utc，BTC 全域
  "idx_pi_cycle_daily":"pi",                    # 只有 date_utc，BTC 全域
}

def _log(m): print(f"[load] {m}")

def _find(root:Path, base:str):
    for name in [base, *ALIASES.get(base, [])]:
        for suf in (".parquet",".csv"):
            hits = list(root.rglob(f"{name}{suf}"))
            if hits: return hits[0]
    return None

def _read(p:Path):
    return pd.read_parquet(p) if p.suffix.lower()==".parquet" else pd.read_csv(p)

def _pick_col(df, *cands):
    cols = {c.lower(): c for c in df.columns}
    for k in cands:
        if k.lower() in cols: return cols[k.lower()]
    return None

def _prep_keys(df, ts_col="ts_utc", sym_col="symbol"):
    df = df.copy()

    # asset
    if sym_col in df.columns:
        df["asset"] = df[sym_col].astype(str).str.upper()
    elif "asset" in df.columns:
        df["asset"] = df["asset"].astype(str).str.upper()
    else:
        raise KeyError(f"no symbol/asset column in {df.columns.tolist()}")

    # ts_utc
    if ts_col in df.columns:
        df["ts_utc"] = pd.to_datetime(df[ts_col], utc=True)
    elif "date_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["date_utc"]).dt.tz_localize("UTC")
    else:
        raise KeyError(f"no ts_utc/date_utc column in {df.columns.tolist()}")

    # 只丟來源欄，不要把新 ts_utc 丟掉
    drop_cols = []
    if sym_col in df.columns: drop_cols.append(sym_col)
    if ts_col in df.columns and ts_col != "ts_utc": drop_cols.append(ts_col)
    return df.drop(columns=drop_cols, errors="ignore")

def _btc_series_from_ts(df, ts_col, val_cols):
    ts_col = _pick_col(df, ts_col, "timestamp", "time", "date_utc")
    if ts_col is None:
        raise KeyError(f"no ts column in {df.columns.tolist()}")
    use_cols = [ts_col] + [c for c in val_cols if _pick_col(df, c)]
    out = df[[ _pick_col(df, c) for c in use_cols ]].copy()

    # 轉時間
    if ts_col.lower() == "date_utc":
        ts = pd.to_datetime(out[ts_col]).dt.tz_localize("UTC")
    else:
        ts = pd.to_datetime(out[ts_col], utc=True)
    out["ts_utc"] = ts
    if ts_col.lower() != "ts_utc":   # 只有在「原欄不是 ts_utc」時才刪
        out = out.drop(columns=[ts_col])

    out["asset"] = "BTC"
    return out

def _btc_series_from_date(df, date_col, value_cols):
    """來源只有 date_utc：轉 ts_utc=當日00:00:00Z，asset='BTC'"""
    out = df.copy()
    out["asset"] = "BTC"
    out["ts_utc"] = pd.to_datetime(out[date_col]).dt.tz_localize("UTC")
    keep = ["asset","ts_utc", *value_cols]
    return out[keep]

def load_sources():
    root = Path(os.getenv("DATA_DIR","./data_1d")).resolve()
    if not root.exists(): raise SystemExit(f"DATA_DIR 不存在：{root}")

    loaded = {}

    # 必要來源
    miss=[]
    for base,key in REQ.items():
        fp=_find(root, base)
        if not fp: miss.append(base); continue
        df=_read(fp); _log(f"載入 {key:>8s} ← {fp.name} rows={len(df)}")
        loaded[key]=_prep_keys(df)
    if miss:
        print("\n[ERROR] 缺少必要來源：", ", ".join(miss))
        raise SystemExit(2)

    # 選擇性來源（自適應欄位）
    for base,key in OPT.items():
        fp=_find(root, base)
        if not fp:
            _log(f"略過 {key:>8s}（未提供）"); continue
        df=_read(fp); _log(f"載入 {key:>8s} ← {fp.name} rows={len(df)}")

        if key == "oi":
            # 欄位別名歸一
            cols = {c.lower(): c for c in df.columns}
            def pick(*cands):
                for x in cands:
                    if x in cols: return cols[x]
                return None
            c_total  = pick("oi_total_close","oi_agg_close","oi_close","open_interest_close")
            c_stable = pick("oi_stable_close","oi_usdt_close","oi_stablecoin_close")
            c_coinm  = pick("oi_coinm_close","oi_coin_margin_close")
            unit_col = pick("unit","oi_unit","currency")
            if unit_col is None:
                df[ "unit" ] = "usd"
                unit_col = "unit"
            df = df.rename(columns={
                c_total:"oi_total_close",
                c_stable or "":"oi_stable_close",
                c_coinm  or "":"oi_coinm_close",
                unit_col:"unit"
            })
            df["unit"] = df["unit"].astype(str).str.lower()
            df = df[df["unit"].isin(["usd","usdt"])]
            loaded[key] = _prep_keys(df)

        elif key == "cpi":
            tscol = _pick_col(df, "ts_utc", "timestamp", "time", "date_utc")
            val   = _pick_col(df, "premium_rate", "premium", "premium_usd")
            if val is None:
                raise KeyError(f"coinbase_premium_index_1d: no premium column in {df.columns.tolist()}")
            loaded[key] = _btc_series_from_ts(df, tscol, [val]).rename(columns={val: "cpi_premium_rate"})

        elif key in ("puell","s2f","pi"):
            # 只有 date_utc，無 symbol/ts_utc
            cols = {c.lower(): c for c in df.columns}
            dcol = cols.get("date_utc","date_utc")
            if key=="puell":
                v = cols.get("puell","puell")
                loaded[key] = _btc_series_from_date(df, dcol, [v]).rename(columns={v:"puell"})
            elif key=="s2f":
                dcol = cols.get("date_utc","date_utc")
                if "s2f" in cols:
                    v = cols["s2f"]
                    loaded[key] = _btc_series_from_date(df, dcol, [v]).rename(columns={v:"s2f"})
                else:
                    v = cols.get("s2f_next_halving","s2f_next_halving")
                    loaded[key] = _btc_series_from_date(df, dcol, [v]).rename(columns={v:"s2f_next_halving"})            
            else:
                v1 = cols.get("pi_ma110","pi_ma110")
                v2 = cols.get("pi_ma350x2","pi_ma350x2")
                loaded[key] = _btc_series_from_date(df, dcol, [v1,v2]).rename(columns={v1:"pi_ma110", v2:"pi_ma350x2"})

        else:
            loaded[key]=_prep_keys(df)

    return loaded
