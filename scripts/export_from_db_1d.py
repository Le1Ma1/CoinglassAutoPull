import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import os, csv, re, pandas as pd, datetime as dt
from dotenv import load_dotenv; load_dotenv()
from src.common.db import connect

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR","./data_1d")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
def log(*a): print("[export]", *a)

# ---------- 資產鍵正規化 ----------
TAIL_RE = re.compile(r'(USDT|USD|USDC|BUSD|TUSD|USDP|UST|PERP)$', re.I)
SEP_RE  = re.compile(r'[-_/].*$', re.I)

def base_asset_py(sym:str)->str:
    s = SEP_RE.sub('', str(sym).upper())
    s = TAIL_RE.sub('', s)
    if s == 'XBT': s = 'BTC'
    return s

def base_assets_from_local():
    aset=set()
    for name in ("futures_ohlcv_1d.csv","spot_ohlcv_1d.csv"):
        p=DATA_DIR/name
        if p.exists():
            df=pd.read_csv(p,usecols=["symbol"])
            aset |= { base_asset_py(x) for x in df["symbol"].astype(str) }
    return sorted(aset)

def asset_expr_sql(col="symbol"):
    # upper(regexp_replace(regexp_replace(symbol,'[-_/].*$',''),'(...tail...)$',''))
    expr = f"upper(regexp_replace(regexp_replace({col}, '[-_/].*$', ''), '(USDT|USD|USDC|BUSD|TUSD|USDP|UST|PERP)$', '', 'i'))"
    return f"(case when {expr}='XBT' then 'BTC' else {expr} end)"

def arr_literal_str(strs): return "ARRAY[" + ",".join("'" + s.replace("'", "''") + "'" for s in strs) + "]"

# ---------- 共用查詢 ----------
def table_has_col(cur, table, col):
    cur.execute("""select exists (
      select 1 from information_schema.columns
      where table_schema='public' and table_name=%s and column_name=%s)""",(table,col))
    return cur.fetchone()[0]

def year_span_db(table:str, where_extra:str|None=None, use_symbol_filter:bool=True):
    with connect() as c, c.cursor() as cur:
        has_date = table_has_col(cur, table, "date_utc")
        has_ts   = table_has_col(cur, table, "ts_utc")
        if not (has_date or has_ts): return (None,None)
        date_expr = "date_utc" if has_date else "(ts_utc::date)"
        whs=[]
        if where_extra: whs.append(where_extra)
        if use_symbol_filter and table_has_col(cur, table, "symbol"):
            aset = base_assets_from_local()
            if aset:
                whs.append(f"{asset_expr_sql('symbol')} = ANY({arr_literal_str(aset)})")
        where = (" where " + " and ".join(whs)) if whs else ""
        cur.execute(f"select min({date_expr})::date, max({date_expr})::date from public.{table}{where}")
        lo, hi = cur.fetchone()
        if not lo or not hi: return (None,None)
        return (int(lo.year), int(hi.year))

def copy_sql(table, sel_cols, where=None, order=None):
    sel = f"select {', '.join(sel_cols)} from public.{table}"
    if where: sel += f" where {where}"
    if order: sel += f" order by {', '.join(order)}"
    return f"copy ({sel}) to stdout with (format csv, header false)"

def copy_by_year(table, sel_cols, out_name, where_base="", order=("symbol","ts_utc"), symbol_filter=True):
    aset = base_assets_from_local()
    y0,y1 = year_span_db(table, where_extra=where_base or None, use_symbol_filter=symbol_filter)
    if y0 is None:
        log(f"skip {out_name}: no data window"); return
    out = DATA_DIR/f"{out_name}.csv"
    with connect() as c, c.cursor() as cur, open(out,"w",newline="",encoding="utf-8") as f:
        cur.execute("set local statement_timeout to '30min'")
        writer=csv.writer(f); writer.writerow(sel_cols if all(' as ' not in x.lower() for x in sel_cols)
                                              else [x.split(' as ')[-1] if ' as ' in x.lower() else x for x in sel_cols])
        for y in range(y0,y1+1):
            whs=[]
            if where_base: whs.append(where_base)
            if table_has_col(cur, table, "date_utc"):
                whs.append(f"date_utc >= date '{y}-01-01' and date_utc < date '{y+1}-01-01'")
            else:
                whs.append(f"(ts_utc::date) >= date '{y}-01-01' and (ts_utc::date) < date '{y+1}-01-01'")
            if symbol_filter and table_has_col(cur, table, "symbol") and aset:
                whs.append(f"{asset_expr_sql('symbol')} = ANY({arr_literal_str(aset)})")
            sql=copy_sql(table, sel_cols, " and ".join(whs), order)
            log(f"{out_name} {y} …")
            try:
                cur.copy_expert(sql, f)
            except Exception as e:
                log(f"skip {out_name} {y}: {e}")
    log(f"{out_name:>28s} -> {out}")

# ---------- 匯出 ----------

# 1) 現貨/期貨 K 線（K 線本身用原始 symbol，不變）
copy_by_year("spot_candles_1d",
    ["symbol","ts_utc","open","high","low","close","volume_usd"],
    "spot_ohlcv_1d", order=("symbol","ts_utc"), symbol_filter=False)
# 如需重做期貨 K 線，解除註解：
# copy_by_year("futures_candles_1d",
#     ["symbol","ts_utc","open","high","low","close","volume_usd"],
#     "futures_ohlcv_1d", order=("symbol","ts_utc"), symbol_filter=False)

# 2) OI：total（agg）、stable、coinm 三表 → 合併
def export_oi_merged():
    aset = base_assets_from_local()
    y0,y1=None,None
    for tbl in ("futures_oi_agg_1d","futures_oi_stablecoin_1d","futures_oi_coin_margin_1d"):
        a,b = year_span_db(tbl, use_symbol_filter=True)
        if a is not None:
            y0 = min(y0,a) if y0 is not None else a
            y1 = max(y1,b) if y1 is not None else b
    if y0 is None:
        log("skip OI: no data window"); return
    out=DATA_DIR/"futures_oi_agg_1d.csv"
    aset_arr = arr_literal_str(aset) if aset else None
    with connect() as c, c.cursor() as cur, open(out,"w",newline="",encoding="utf-8") as f:
        cur.execute("set local statement_timeout to '30min'")
        writer=csv.writer(f); writer.writerow(["symbol","ts_utc","oi_total_close","oi_stable_close","oi_coinm_close","unit"])
        for y in range(y0,y1+1):
            base_filter = f"{asset_expr_sql('symbol')} = ANY({aset_arr})" if aset_arr else "true"
            wh = f"date_utc >= date '{y}-01-01' and date_utc < date '{y+1}-01-01' and {base_filter}"
            sql=f"""
            copy (
              with
              t as (
                select {asset_expr_sql('symbol')} as symbol, ts_utc, close as oi_total_close, lower(unit) as unit
                from public.futures_oi_agg_1d
                where {wh} and lower(unit) in ('usd','usdt')
              ),
              s as (
                select {asset_expr_sql('symbol')} as symbol, ts_utc, sum(close) as oi_stable_close
                from public.futures_oi_stablecoin_1d
                where {wh} group by 1,2
              ),
              c as (
                select {asset_expr_sql('symbol')} as symbol, ts_utc, sum(close) as oi_coinm_close
                from public.futures_oi_coin_margin_1d
                where {wh} group by 1,2
              )
              select coalesce(t.symbol,s.symbol,c.symbol) as symbol,
                     coalesce(t.ts_utc,s.ts_utc,c.ts_utc) as ts_utc,
                     t.oi_total_close, s.oi_stable_close, c.oi_coinm_close, t.unit
              from t full join s using(symbol, ts_utc)
                     full join c using(symbol, ts_utc)
              order by 1,2
            ) to stdout with (format csv, header false)
            """
            log(f"futures_oi_agg_1d {y} …"); cur.copy_expert(sql, f)
    log(f"{'futures_oi_agg_1d':>28s} -> {out}")
export_oi_merged()

# 3) Funding（close→funding_close；用資產鍵過濾）
copy_by_year("funding_oi_weight_1d",
    [f"{asset_expr_sql('symbol')} as symbol","ts_utc","close as funding_close"],
    "funding_oi_weight_1d", order=("symbol","ts_utc"), symbol_filter=True)
copy_by_year("funding_vol_weight_1d",
    [f"{asset_expr_sql('symbol')} as symbol","ts_utc","close as funding_close"],
    "funding_vol_weight_1d", order=("symbol","ts_utc"), symbol_filter=True)

# 4) Orderbook / Taker / Liq（資產鍵過濾 + 欄位改名）
copy_by_year("orderbook_agg_futures_1d",
    [f"{asset_expr_sql('symbol')} as symbol","ts_utc","range_pct","bids_usd","asks_usd","bids_qty","asks_qty"],
    "orderbook_agg_futures_1d", where_base="range_pct in (1,1.0)", order=("symbol","ts_utc"), symbol_filter=True)

copy_by_year("taker_vol_agg_futures_1d",
    [f"{asset_expr_sql('symbol')} as symbol","ts_utc","buy_vol_usd as taker_buy_usd","sell_vol_usd as taker_sell_usd"],
    "taker_vol_agg_futures_1d", order=("symbol","ts_utc"), symbol_filter=True)

copy_by_year("liquidation_agg_1d",
    [f"{asset_expr_sql('symbol')} as symbol","ts_utc","long_liq_usd as liq_long_usd","short_liq_usd as liq_short_usd"],
    "liquidation_agg_1d", order=("symbol","ts_utc"), symbol_filter=True)

# 5) ETF（US+HK+AUM+Premium → BTC 全域）
def export_etf_all():
    out = DATA_DIR/"etf_all_1d.csv"
    with connect() as c, c.cursor() as cur, open(out,"w",newline="",encoding="utf-8") as f:
        cur.execute("set local statement_timeout to '10min'")
        writer=csv.writer(f); writer.writerow(["symbol","date_utc","flow_usd","aum_usd","premium_discount"])
        sql = """
        copy (
          with
          us_flow as (select date_utc, total_flow_usd from public.etf_bitcoin_flow_1d),
          hk_flow as (select date_utc, total_flow_usd from public.hk_etf_flow_1d),
          aum as     (select date_utc, net_assets_usd from public.etf_bitcoin_net_assets_1d),
          prem as    (select date_utc, avg(premium_discount) as premium_discount from public.etf_premium_discount_1d group by 1),
          d as (
            select generate_series(
              least( (select min(date_utc) from us_flow),
                     (select min(date_utc) from hk_flow),
                     (select min(date_utc) from aum),
                     (select min(date_utc) from prem) ),
              greatest( (select max(date_utc) from us_flow),
                        (select max(date_utc) from hk_flow),
                        (select max(date_utc) from aum),
                        (select max(date_utc) from prem) ),
              interval '1 day')::date as date_utc
          )
          select 'BTC'::text as symbol, d.date_utc,
                 coalesce(us.total_flow_usd,0)+coalesce(hk.total_flow_usd,0) as flow_usd,
                 a.net_assets_usd as aum_usd, p.premium_discount
          from d
          left join us_flow us on us.date_utc=d.date_utc
          left join hk_flow hk on hk.date_utc=d.date_utc
          left join aum a on a.date_utc=d.date_utc
          left join prem p on p.date_utc=d.date_utc
          order by 2
        ) to stdout with (format csv, header false)
        """
        cur.copy_expert(sql, f)
    log(f"{'etf_all_1d':>28s} -> {out}")
export_etf_all()

# 6) CPI / BFX / Borrow IR / LSR / 指標
with connect() as c, c.cursor() as cur, open(DATA_DIR/"coinbase_premium_index_1d.csv","w",newline="",encoding="utf-8") as f:
    cur.execute("set local statement_timeout to '10min'")
    cur.copy_expert("copy (select ts_utc, premium_rate from public.coinbase_premium_index_1d order by ts_utc) to stdout with (format csv, header false)", f)
log("coinbase_premium_index_1d ->", DATA_DIR/"coinbase_premium_index_1d.csv")

copy_by_year("bitfinex_margin_long_short_1d",
    [f"{asset_expr_sql('symbol')} as symbol","ts_utc","long_qty","short_qty"],
    "bitfinex_margin_long_short_1d", order=("symbol","ts_utc"), symbol_filter=True)

def export_borrow_ir():
    y0,y1 = year_span_db("borrow_interest_rate_1d", use_symbol_filter=True)
    if y0 is None: log("skip borrow_interest_rate_1d: no data window"); return
    aset = base_assets_from_local()
    aset_arr = arr_literal_str(aset) if aset else None
    out = DATA_DIR/"borrow_interest_rate_1d.csv"
    with connect() as c, c.cursor() as cur, open(out,"w",newline="",encoding="utf-8") as f:
        cur.execute("set local statement_timeout to '30min'")
        writer=csv.writer(f); writer.writerow(["symbol","ts_utc","borrow_ir"])
        for y in range(y0,y1+1):
            base_filter = f"{asset_expr_sql('symbol')} = ANY({aset_arr})" if aset_arr else "true"
            wh = f"date_utc >= date '{y}-01-01' and date_utc < date '{y+1}-01-01' and {base_filter}"
            sql = f"""
            copy (
              select {asset_expr_sql('symbol')} as symbol,
                     ts_utc,
                     avg(interest_rate) as borrow_ir
              from public.borrow_interest_rate_1d
              where {wh}
              group by 1,2
              order by 1,2
            ) to stdout with (format csv, header false)
            """
            log(f"borrow_interest_rate_1d {y} …"); cur.copy_expert(sql, f)
    log(f"{'borrow_interest_rate_1d':>28s} -> {out}")
export_borrow_ir()

def export_lsr_all():
    # 三表時間窗聯集
    ys=[]
    for tbl in ("long_short_global_1d","long_short_top_accounts_1d","long_short_top_positions_1d"):
        a,b = year_span_db(tbl, use_symbol_filter=True)
        if a is not None: ys += [a,b]
    if not ys: log("skip long_short_all_1d: no data window"); return
    y0,y1=min(ys),max(ys)
    aset = base_assets_from_local()
    aset_arr = arr_literal_str(aset) if aset else None
    out = DATA_DIR/"long_short_all_1d.csv"
    with connect() as c, c.cursor() as cur, open(out,"w",newline="",encoding="utf-8") as f:
        cur.execute("set local statement_timeout to '30min'")
        writer=csv.writer(f); writer.writerow(["symbol","ts_utc","lsr_global","lsr_top_accounts","lsr_top_positions"])
        for y in range(y0,y1+1):
            base_filter = f"{asset_expr_sql('symbol')} = ANY({aset_arr})" if aset_arr else "true"
            wh = f"date_utc >= date '{y}-01-01' and date_utc < date '{y+1}-01-01' and {base_filter}"
            sql=f"""
            copy (
              with
              g as (select {asset_expr_sql('symbol')} as symbol, ts_utc, avg(long_short_ratio) as lsr_global
                    from public.long_short_global_1d where {wh} group by 1,2),
              a as (select {asset_expr_sql('symbol')} as symbol, ts_utc, avg(long_short_ratio) as lsr_top_accounts
                    from public.long_short_top_accounts_1d where {wh} group by 1,2),
              p as (select {asset_expr_sql('symbol')} as symbol, ts_utc, avg(long_short_ratio) as lsr_top_positions
                    from public.long_short_top_positions_1d where {wh} group by 1,2)
              select coalesce(g.symbol,a.symbol,p.symbol) as symbol,
                     coalesce(g.ts_utc,a.ts_utc,p.ts_utc) as ts_utc,
                     g.lsr_global, a.lsr_top_accounts, p.lsr_top_positions
              from g full join a using(symbol, ts_utc)
                      full join p using(symbol, ts_utc)
              order by 1,2
            ) to stdout with (format csv, header false)
            """
            log(f"long_short_all_1d {y} …"); cur.copy_expert(sql, f)
    log(f"{'long_short_all_1d':>28s} -> {out}")
export_lsr_all()

# 7) 指標（整表，量小）
with connect() as c, c.cursor() as cur:
    for tbl, sel, out in [
        ("idx_puell_multiple_daily", ["date_utc","puell_multiple as puell"], "idx_puell_multiple_daily"),
        ("idx_stock_to_flow_daily", ["date_utc","next_halving as s2f_next_halving"], "idx_stock_to_flow_daily"),
        ("idx_pi_cycle_daily", ["date_utc","ma_110 as pi_ma110","ma_350_x2 as pi_ma350x2"], "idx_pi_cycle_daily"),
    ]:
        p = DATA_DIR/f"{out}.csv"
        with open(p,"w",newline="",encoding="utf-8") as f:
            cur.execute("set local statement_timeout to '10min'")
            cur.copy_expert(copy_sql(tbl, sel, order=["date_utc"]), f)
        log(f"{out:>28s} -> {p}")

log("done")
