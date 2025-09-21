# -*- coding: utf-8 -*-
"""
build_ta5_scores_1d.py
從 public.spot_candles_1d 聚合現貨 OHLCV 到資產層，計算五維技術指標（Trend/Osc/Mom/Vol/Volume）
並上載至 public.features_1d（主鍵 asset, ts_utc）。

聚合規則：
- asset = 去除交易對尾綴（USDT/USD/USDC/BUSD/TUSD）的基礎幣別，轉大寫
- 價格：跨交易所同 asset、同 ts_utc 的 open/high/low/close 取平均
- 量：volume_usd 加總

環境變數（或改用參數）：
- SUPABASE_DB_URL or PGHOST/PGUSER/PGPASSWORD/PGDATABASE/PGPORT
- ASSETS（可選，逗號分隔，如 "BTC,ETH"）
- SINCE（可選，起算日 YYYY-MM-DD；程式自動回溫 400 日）
"""
import os
import sys
import argparse
import math
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

EPS = 1e-9

# ---------------- DB 連線 ----------------
def _conn_from_env():
    url = os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("環境變數 SUPABASE_DB_URL 未設定")
    # 直接使用完整 DSN，例如：
    # postgresql://user:password@host:port/dbname
    return psycopg2.connect(dsn=url, sslmode=os.getenv("PGSSLMODE", "require"))
    
# ---------------- 工具函式 ----------------
def normalize_asset(symbol: str) -> str:
    if symbol is None:
        return None
    s = symbol.upper()
    for suf in ("USDT", "USDC", "BUSD", "TUSD", "USD"):
        if s.endswith(suf):
            return s[: -len(suf)]
    return s

def pct_rank_rolling(s: pd.Series, window: int) -> pd.Series:
    # 以最後一筆在窗口內的分位：mean(arr <= arr[-1])
    def _fn(a: np.ndarray):
        if np.isnan(a[-1]):
            return np.nan
        b = a[~np.isnan(a)]
        if b.size == 0:
            return np.nan
        x = b[-1]
        return np.mean(b <= x)
    return s.rolling(window, min_periods=window).apply(_fn, raw=True)

def wilder_ema(s: pd.Series, n: int) -> pd.Series:
    # Wilder smoothing ≈ ewm(alpha=1/n)
    if n <= 1:
        return s
    return s.ewm(alpha=1.0 / float(n), adjust=False, min_periods=n).mean()

def rsi_wilder(close: pd.Series, n: int) -> pd.Series:
    d = close.diff()
    up = d.clip(lower=0.0)
    dn = (-d).clip(lower=0.0)
    avg_up = wilder_ema(up, n)
    avg_dn = wilder_ema(dn, n)
    rs = avg_up / (avg_dn + EPS)
    return 100.0 - 100.0 / (1.0 + rs)

def atr_wilder(high: pd.Series, low: pd.Series, close: pd.Series, n: int) -> pd.Series:
    tr1 = (high - low).abs()
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return wilder_ema(tr, n)

def adx_wilder(high: pd.Series, low: pd.Series, close: pd.Series, n: int) -> pd.Series:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    tr1 = (high - low).abs()
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = wilder_ema(tr, n)
    plus_di = 100.0 * wilder_ema(plus_dm, n) / (atr + EPS)
    minus_di = 100.0 * wilder_ema(minus_dm, n) / (atr + EPS)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + EPS)
    return wilder_ema(dx, n)

def rolling_min(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).min()

def rolling_max(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).max()

def rolling_sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()

def rolling_std(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).std(ddof=0)

def rolling_mean_abs_dev(s: pd.Series, n: int) -> pd.Series:
    # mean(|x - mean(x)|) over window n
    def _mad(a: np.ndarray):
        if np.isnan(a).all():
            return np.nan
        m = np.nanmean(a)
        return np.nanmean(np.abs(a - m))
    return s.rolling(n, min_periods=n).apply(_mad, raw=True)

def rolling_median_abs_dev(s: pd.Series, n: int) -> pd.Series:
    # median(|x - median(x)|)
    def _mad(a: np.ndarray):
        b = a[~np.isnan(a)]
        if b.size == 0:
            return np.nan
        med = np.median(b)
        return np.median(np.abs(b - med))
    return s.rolling(n, min_periods=n).apply(_mad, raw=True)

def rolling_ols_slope(y: pd.Series, n: int) -> pd.Series:
    # 對窗口 [0..n-1] 的 x 做最小二乘斜率（未標準化）
    x = np.arange(n, dtype=float)
    x_mean = x.mean()
    x_var = ((x - x_mean) ** 2).sum()
    def _slope(a: np.ndarray):
        if np.isnan(a).any():
            return np.nan
        y_mean = a.mean()
        return ((x - x_mean) * (a - y_mean)).sum() / (x_var + EPS)
    return y.rolling(n, min_periods=n).apply(_slope, raw=True)

def select_by_window(df_map: pd.DataFrame, w_series: pd.Series, w_min: int, w_max: int) -> pd.Series:
    # df_map: columns = [w_min..w_max]，index 與 w_series 對齊
    w = w_series.clip(lower=w_min, upper=w_max)
    cols = list(range(w_min, w_max + 1))
    arr = df_map.reindex(columns=cols).to_numpy()

    w_vals = w.to_numpy(dtype=float)
    n = arr.shape[0]
    idx_rows = np.arange(n)

    # 僅在 w 有限值時轉成整數索引；其他保持 NaN
    valid = np.isfinite(w_vals)
    idx_cols = np.full(n, -1, dtype=int)
    idx_cols[valid] = (w_vals[valid].astype(int) - w_min)

    out = np.full(n, np.nan, dtype=float)
    ok = valid & (idx_cols >= 0) & (idx_cols < arr.shape[1])
    out[ok] = arr[idx_rows[ok], idx_cols[ok]]
    return pd.Series(out, index=df_map.index)

# ---------------- 計算主流程（單資產） ----------------
def compute_ta5_for_asset(df: pd.DataFrame) -> pd.DataFrame:
    """
    輸入：同一資產的時間序列（欄位：ts_utc, px_open, px_high, px_low, px_close, vol_usd）
    回傳：新增欄位 score_trend/score_osc/score_mom/score_vol/score_volume
    """
    df = df.sort_values("ts_utc").copy()
    O, H, L, C, V = [df[c].astype(float) for c in ["px_open", "px_high", "px_low", "px_close", "vol_usd"]]

    # --- 基礎序列 ---
    r = np.log(C / C.shift(1))
    stdev20_r = r.rolling(20, min_periods=20).std(ddof=0)
    phi = pct_rank_rolling(stdev20_r, 252)  # 近端波動狀態 ∈ [0,1]

    # 自適應窗
    w_f = (5 + np.rint(15 * phi)).clip(5, 20)            # 5..20
    w_s = (2 * w_f + 10).clip(20, 120)                   # 20..120
    w_adx = (10 + np.rint(20 * phi)).clip(10, 30)        # 10..30
    n_osc = (10 + np.rint(20 * (1 - phi))).clip(10, 30)  # 10..30
    w_m  = (5 + np.rint(25 * phi)).clip(5, 30)           # 5..30
    n_atr = (10 + np.rint(20 * phi)).clip(10, 30)        # 10..30
    n_v = (10 + np.rint(20 * (1 - phi))).clip(10, 30)    # 10..30

    # 共同指標
    atr14 = atr_wilder(H, L, C, 14)
    # BB 20, 2σ
    bb_mid = rolling_sma(C, 20)
    bb_std = rolling_std(C, 20)
    bb_up = bb_mid + 2.0 * bb_std
    bb_dn = bb_mid - 2.0 * bb_std
    bw20 = (bb_up - bb_dn) / (bb_mid.replace(0, np.nan).abs() + EPS)
    omega_range = 1.0 - pct_rank_rolling(bw20, 252)

    # --- Trend ---
    # EMA 矩陣預先計（5..120）
    ema_map = {w: C.ewm(span=int(w), adjust=False, min_periods=int(w)).mean() for w in range(5, 121)}
    ema_df = pd.DataFrame(ema_map)
    ema_fast = select_by_window(ema_df.loc[:, 5:20], w_f, 5, 20)
    ema_slow = select_by_window(ema_df.loc[:, 20:120], w_s, 20, 120)
    cross = (ema_fast - ema_slow) / (atr14 + EPS)
    cross_n = np.tanh(cross / 1.5)

    # ADX 矩陣（10..30）
    adx_map = {w: adx_wilder(H, L, C, int(w)) for w in range(10, 31)}
    adx_df = pd.DataFrame(adx_map)
    adx_sel = select_by_window(adx_df, w_adx, 10, 30)
    q = ((adx_sel - 20.0) / (50.0 - 20.0)).clip(lower=0.0, upper=1.0)

    score_trend = 100.0 * q * cross_n

    # --- Oscillator ---
    # RSI(n_osc)
    rsi_map = {w: rsi_wilder(C, int(w)) for w in range(10, 31)}
    rsi_df = pd.DataFrame(rsi_map)
    rsi_sel = select_by_window(rsi_df, n_osc, 10, 30)
    rsi_c = (rsi_sel - 50.0) / 50.0  # [-1,1]

    # Stochastic %K(n_osc)
    low_map = {w: rolling_min(L, int(w)) for w in range(10, 31)}
    high_map = {w: rolling_max(H, int(w)) for w in range(10, 31)}
    low_df = pd.DataFrame(low_map)
    high_df = pd.DataFrame(high_map)
    low_sel = select_by_window(low_df, n_osc, 10, 30)
    high_sel = select_by_window(high_df, n_osc, 10, 30)
    k = (C - low_sel) / (high_sel - low_sel + EPS)
    stoc = 2.0 * k - 1.0  # [-1,1]

    # CCI(n_osc)，使用 mean absolute deviation
    TP = (H + L + C) / 3.0
    sma_map = {w: rolling_sma(TP, int(w)) for w in range(10, 31)}
    mad_map = {w: rolling_mean_abs_dev(TP, int(w)) for w in range(10, 31)}
    sma_df = pd.DataFrame(sma_map)
    mad_df = pd.DataFrame(mad_map)
    sma_sel = select_by_window(sma_df, n_osc, 10, 30)
    mad_sel = select_by_window(mad_df, n_osc, 10, 30)
    cci = (TP - sma_sel) / (0.015 * (mad_sel + EPS))
    cci_c = np.tanh(cci / 200.0)

    score_osc = 100.0 * omega_range * (0.5 * rsi_c + 0.3 * stoc + 0.2 * cci_c)

    # --- Momentum ---
    # ROC(w_m) 與其 252 天標準差
    roc_map = {w: (C / C.shift(int(w)) - 1.0) for w in range(5, 31)}
    roc_df = pd.DataFrame(roc_map)
    roc_sel = select_by_window(roc_df, w_m, 5, 30)
    sroc_map = {w: roc_df[w].rolling(252, min_periods=100).std(ddof=0) for w in range(5, 31)}
    sroc_df = pd.DataFrame(sroc_map)
    sroc_sel = select_by_window(sroc_df, w_m, 5, 30)
    roc_n = np.tanh(roc_sel / (3.0 * (sroc_sel + EPS)))

    # MACD 柱（12,26,9）
    ema12 = C.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = C.ewm(span=26, adjust=False, min_periods=26).mean()
    macd_line = ema12 - ema26
    signal = macd_line.ewm(span=9, adjust=False, min_periods=9).mean()
    mh = macd_line - signal
    mh_n = np.tanh(mh / (1.5 * (atr14 + EPS)))

    score_mom = 100.0 * (0.7 * roc_n + 0.3 * mh_n)

    # --- Volatility ---
    atr_map = {w: atr_wilder(H, L, C, int(w)) for w in range(10, 31)}
    atr_df = pd.DataFrame(atr_map)
    atr_sel = select_by_window(atr_df, n_atr, 10, 30)
    x1 = atr_sel / (C.replace(0, np.nan).abs() + EPS)
    x2 = bw20
    p1 = pct_rank_rolling(x1, 252)
    p2 = pct_rank_rolling(x2, 252)
    score_vol = 100.0 * 0.5 * (p1 + p2)

    # --- Volume ---
    vr = V / (rolling_sma(V, 20) + EPS)
    vr_n = 2.0 * pct_rank_rolling(vr, 252) - 1.0  # [-1,1]

    dC = C.diff()
    sign_dir = np.sign(dC).fillna(0.0)
    obv = (sign_dir * V).fillna(0.0).cumsum()

    # s_obv 斜率（n_v）
    slope_map = {w: rolling_ols_slope(obv, int(w)) for w in range(10, 31)}
    slope_df = pd.DataFrame(slope_map)
    s_obv = select_by_window(slope_df, n_v, 10, 30)

    # MAD_252(ΔOBV)
    d_obv = obv.diff()
    mad252 = rolling_median_abs_dev(d_obv, 252)
    alpha = (s_obv.abs() / (mad252 + EPS)).clip(0.0, 1.0)

    sigma = np.sign((dC) * (s_obv)).fillna(0.0)  # ∈ {-1,0,1}
    score_volume = 100.0 * vr_n * sigma * alpha

    out = df[["ts_utc", "px_open", "px_high", "px_low", "px_close", "vol_usd"]].copy()
    out["score_trend"] = score_trend.astype(float)
    out["score_osc"] = score_osc.astype(float)
    out["score_mom"] = score_mom.astype(float)
    out["score_vol"] = score_vol.astype(float)
    out["score_volume"] = score_volume.astype(float)
    return out

# ---------------- 資料擷取（聚合到資產層） ----------------
def load_spot_ohlcv_aggregated(conn, since: datetime | None, assets: list[str] | None):
    params = []
    where = []
    if since is not None:
        warmup = since - timedelta(days=400)  # 回溫 400 日以覆蓋 252 + 慢窗 120
        where.append("ts_utc >= %s")
        params.append(warmup.astimezone(timezone.utc))
    sql = f"""
    with base as (
      select
        ts_utc,
        upper(symbol) as symbol,
        (regexp_replace(upper(symbol), '(USDT|USDC|BUSD|TUSD|USD)$', '')) as asset,
        open::double precision as open,
        high::double precision as high,
        low::double precision  as low,
        close::double precision as close,
        volume_usd::double precision as volume_usd
      from public.spot_candles_1d
      {"where " + " and ".join(where) if where else ""}
    )
    select
      asset,
      ts_utc,
      avg(open)  as px_open,
      avg(high)  as px_high,
      avg(low)   as px_low,
      avg(close) as px_close,
      sum(volume_usd) as vol_usd
    from base
    { "where asset = any(%s)" if assets else "" }
    group by asset, ts_utc
    order by asset, ts_utc
    """
    if assets:
        params.append(assets)
    df = pd.read_sql(sql, conn, params=params)
    return df

# ---------------- 上載（UPSERT） ----------------
def upsert_features(conn, asset: str, df_scored: pd.DataFrame, score_ver: int = 1):
    if df_scored.empty:
        return 0
    rows = []
    for _, row in df_scored.iterrows():
        rows.append((
            asset,
            row["ts_utc"],
            float(row["px_open"]) if pd.notna(row["px_open"]) else None,
            float(row["px_high"]) if pd.notna(row["px_high"]) else None,
            float(row["px_low"]) if pd.notna(row["px_low"]) else None,
            float(row["px_close"]) if pd.notna(row["px_close"]) else None,
            float(row["vol_usd"]) if pd.notna(row["vol_usd"]) else None,
            float(row["score_trend"]) if pd.notna(row["score_trend"]) else None,
            float(row["score_osc"]) if pd.notna(row["score_osc"]) else None,
            float(row["score_mom"]) if pd.notna(row["score_mom"]) else None,
            float(row["score_vol"]) if pd.notna(row["score_vol"]) else None,
            float(row["score_volume"]) if pd.notna(row["score_volume"]) else None,
            score_ver,
            json.dumps({})  # 讓 INSERT 帶空物件，但不會清掉既有鍵
        ))
    sql = """
    insert into public.features_1d
      (asset, ts_utc, px_open, px_high, px_low, px_close, vol_usd,
       score_trend, score_osc, score_mom, score_vol, score_volume,
       score_ver, ext_features)
    values %s
    on conflict (asset, ts_utc) do update set
       px_open = excluded.px_open,
       px_high = excluded.px_high,
       px_low  = excluded.px_low,
       px_close = excluded.px_close,
       vol_usd = excluded.vol_usd,
       score_trend = excluded.score_trend,
       score_osc   = excluded.score_osc,
       score_mom   = excluded.score_mom,
       score_vol   = excluded.score_vol,
       score_volume = excluded.score_volume,
       score_ver = excluded.score_ver,
       -- 關鍵：只合併，不覆蓋其他 JSON 鍵
       ext_features = coalesce(public.features_1d.ext_features,'{}'::jsonb)
                      || coalesce(excluded.ext_features,'{}'::jsonb),
       updated_at = now();
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


# ---------------- 主程式 ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=str, default=os.getenv("SINCE", None),
                    help="YYYY-MM-DD；自動回溫 400 日")
    ap.add_argument("--assets", type=str, default=os.getenv("ASSETS", None),
                    help="逗號分隔資產，如 BTC,ETH")
    ap.add_argument("--score_ver", type=int, default=1)
    args = ap.parse_args()

    since = None
    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    assets = None
    if args.assets:
        assets = [a.strip().upper() for a in args.assets.split(",") if a.strip()]

    with _conn_from_env() as conn:
        print(f"[{datetime.now(timezone.utc).isoformat()}] 讀取 spot_candles_1d → 聚合到資產層…")
        df = load_spot_ohlcv_aggregated(conn, since, assets)
        if df.empty:
            print("無資料")
            return
        print(f"資產數={df['asset'].nunique()}, 期間={df['ts_utc'].min()}→{df['ts_utc'].max()}")
        # 依資產分組計算與上載
        n_total = 0
        for asset, g in df.groupby("asset", sort=True):
            g = g.sort_values("ts_utc").reset_index(drop=True)

            scored = compute_ta5_for_asset(g)
            scored.replace([np.inf, -np.inf], np.nan, inplace=True)

            if since is not None:
                scored = scored[scored["ts_utc"] >= since]

            if scored.empty:
                print(f"  {asset}: 無需更新")
                continue

            n = upsert_features(conn, asset, scored, score_ver=args.score_ver)
            n_total += n
            print(f"  {asset}: upsert {n} rows")

        print(f"完成，上載 {n_total} 列。")

if __name__ == "__main__":
    main()
