import numpy as np, pandas as pd

def _shift(s,k=1): return s.shift(k)
def rmean(s,w): return _shift(s).rolling(w, min_periods=w).mean()
def rstd(s,w):  return _shift(s).rolling(w, min_periods=w).std()
def rz(s,w):    return (s - rmean(s,w))/rstd(s,w)
def _ema(s,span): return _shift(s).ewm(span=span, adjust=False, min_periods=span).mean()

def _true_range(df):
    pc = _shift(df["px_close"])
    return pd.concat([df["px_high"]-df["px_low"], (df["px_high"]-pc).abs(), (df["px_low"]-pc).abs()],axis=1).max(axis=1)

def _beta60(ret, ret_btc):
    y = _shift(ret); x = _shift(ret_btc)
    cov = y.rolling(60, min_periods=60).cov(x)
    var = x.rolling(60, min_periods=60).var()
    return cov/var

def _imb(a, b):
    denom = a + b
    out = (a - b) / denom
    out = out.mask(denom == 0)
    return out

def compute_features(df):
    print(f"[compute_features] input rows={len(df)}, cols={list(df.columns)}", flush=True)
    def per_asset(g):
        g = g.sort_index()

        # 基礎：價格與波動
        g["ret_1d"] = g["px_close"].pct_change()
        for w in [3,5,10,20,60,120,252]:
            g[f"roc_{w}"] = g["px_close"].pct_change(w)
            g[f"mom_{w}"] = g["px_close"]/_shift(g["px_close"],w)-1
        for w in [10,20,60,120,252]:
            g[f"sma_{w}"] = rmean(g["px_close"],w)
        g["ema_12"], g["ema_26"] = _ema(g["px_close"],12), _ema(g["px_close"],26)
        g["macd"] = g["ema_12"] - g["ema_26"]
        g["macd_signal_9"] = _ema(g["macd"],9)
        g["macd_hist"] = g["macd"] - g["macd_signal_9"]
        mu20, sd20 = rmean(g["px_close"],20), rstd(g["px_close"],20)
        g["bb_mid_20"], g["bb_up_20"], g["bb_dn_20"] = mu20, mu20+2*sd20, mu20-2*sd20
        tr = _true_range(g.reset_index(level=0, drop=True).assign(asset=None))
        g["atr_14"] = _shift(tr).rolling(14, min_periods=14).mean()
        for w in [20,60,120]:
            g[f"rv_{w}"] = _shift(g["ret_1d"]).rolling(w, min_periods=w).std()
            g[f"z_ret_{w}"] = rz(g["ret_1d"],w)

        # OI（可缺）
        if "oi_agg_close" in g:
            g["d_oi_1"] = g["oi_agg_close"] - _shift(g["oi_agg_close"])
            for w in [5,20,60]:
                g[f"oi_roc_{w}"] = g["oi_agg_close"]/_shift(g["oi_agg_close"],w)-1
            g["oi_z_60"] = rz(g["oi_agg_close"],60)

        # Funding（可缺）
        if "funding_oiw_close" in g:
            g["d_funding_1"] = g["funding_oiw_close"] - _shift(g["funding_oiw_close"])
            g["funding_ma_20"], g["funding_ma_60"] = rmean(g["funding_oiw_close"],20), rmean(g["funding_oiw_close"],60)
            g["funding_z_60"] = rz(g["funding_oiw_close"],60)

        # LSR（可缺）
        for col in ["lsr_global","lsr_top_accts","lsr_top_pos"]:
            if col in g:
                suff = col.split('_',1)[1]
                g[f"lsr_ma20_{suff}"] = rmean(g[col],20)
                g[f"lsr_z60_{suff}"]  = rz(g[col],60)

        # Orderbook（可缺）
        if {"ob_bids_usd","ob_asks_usd"} <= set(g.columns):
            g["ob_imb"] = _imb(g["ob_bids_usd"], g["ob_asks_usd"])
            g["ob_imb_ma20"] = rmean(g["ob_imb"],20)
            g["ob_imb_z60"]  = rz(g["ob_imb"],60)
        if {"ob_bids_qty","ob_asks_qty"} <= set(g.columns):
            g["depth_ratio_q"] = _imb(g["ob_bids_qty"], g["ob_asks_qty"])
            g["depth_ratio_q_ma20"] = rmean(g["depth_ratio_q"],20)
            g["depth_ratio_q_z60"]  = rz(g["depth_ratio_q"],60)

        # Taker（可缺）
        if {"taker_buy_usd","taker_sell_usd"} <= set(g.columns):
            g["taker_imb"] = _imb(g["taker_buy_usd"], g["taker_sell_usd"])
            g["taker_imb_ma20"] = rmean(g["taker_imb"],20)
            g["taker_imb_z60"]  = rz(g["taker_imb"],60)
            g["taker_buy_ma20"], g["taker_sell_ma20"] = rmean(g["taker_buy_usd"],20), rmean(g["taker_sell_usd"],20)
            g["taker_buy_z60"],  g["taker_sell_z60"]  = rz(g["taker_buy_usd"],60),   rz(g["taker_sell_usd"],60)

        # 爆倉（可缺）
        if {"liq_long_usd","liq_short_usd"} <= set(g.columns):
            g["liq_net"] = g["liq_long_usd"] - g["liq_short_usd"]
            g["liq_z60"] = rz(g["liq_net"],60)

        # ETF / CPI（可缺）
        if "etf_flow_usd" in g:  g["etf_flow_z60"] = rz(g["etf_flow_usd"],60)
        if "etf_aum_usd" in g:
            g["etf_aum_roc_5"]  = g["etf_aum_usd"]/_shift(g["etf_aum_usd"],5)-1
            g["etf_aum_roc_20"] = g["etf_aum_usd"]/_shift(g["etf_aum_usd"],20)-1
        if "etf_premdisc" in g:
            g["premdisc_ma20"] = rmean(g["etf_premdisc"],20)
            g["premdisc_z60"]  = rz(g["etf_premdisc"],60)
        if "cpi_premium_rate" in g:
            g["cpi_ma20"], g["cpi_z60"] = rmean(g["cpi_premium_rate"],20), rz(g["cpi_premium_rate"],60)

        # BFX（可缺）
        if {"bfx_long_qty","bfx_short_qty"} <= set(g.columns):
            denom = g["bfx_short_qty"].replace(0, np.nan)
            g["bfx_lr"] = g["bfx_long_qty"]/denom
            g["bfx_lr_d1"] = g["bfx_lr"] - _shift(g["bfx_lr"])

        # 借幣利率 / 指標（可缺）
        if "borrow_ir" in g: g["borrow_ir_ma20"] = rmean(g["borrow_ir"],20)
        if "puell" in g:     g["puell_d1"] = g["puell"] - _shift(g["puell"])
        if "s2f" in g:
            g["s2f_d1"] = g["s2f"] - _shift(g["s2f"])
        elif "s2f_next_halving" in g:
            g["s2f_d1"] = g["s2f_next_halving"] - _shift(g["s2f_next_halving"])
        return g

    df = df.groupby(level=0, group_keys=False).apply(per_asset)

    # 相對 BTC
    close = df["px_close"].unstack(0); ret = close.pct_change()
    if "BTC" in ret.columns:
        ret_btc = ret["BTC"]
        beta = ret.apply(lambda s: _beta60(s, ret_btc))
        beta = beta.stack().rename("beta60"); beta.index.names=["ts_utc","asset"]
        beta = beta.reorder_levels(["asset","ts_utc"]).sort_index()
        df["rel_to_btc"] = df["ret_1d"] - beta.reindex(df.index)*ret_btc.reindex(df.index.get_level_values("ts_utc")).values

    # 橫截面排名
    by_day = df.index.get_level_values("ts_utc").date
    df["xsec_ret_rank"]    = df["ret_1d"].groupby(by_day).rank(pct=True)
    df["xsec_mom_rank_20"] = df["mom_20"].groupby(by_day).rank(pct=True)
    df["xsec_vol_rank_60"] = df["rv_60"].groupby(by_day).rank(pct=True)
    return df
