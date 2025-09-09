#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coinglass 日線歷史全量 -> Supabase(Postgres)
- API 分頁：每請求 <= 4500（v4 限制）；多頁累積；入庫每批 <= 20000
- 嚴格限流：預設 80 調用/分鐘（CG_QPM 可覆蓋）
- 時間統一：ts_utc 為 UTC；date_utc 由 DB 生成欄位
- 首頁不帶時間只帶 limit 拿最近一頁，再以最老 time 作 end_time 游標往前翻
"""
import os, time, json
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional
import requests
import psycopg2
from psycopg2.extras import execute_values
import socket
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# -------- .env --------
try:
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv() or load_dotenv(Path(__file__).with_name(".env")) \
        or load_dotenv(Path(__file__).parent.parent.joinpath(".env"))
except Exception:
    pass

def getenv_any(names, default=None):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

# -------- 基本設定 --------
BASE    = getenv_any(["CG_BASE","COINGLASS_BASE"], "https://open-api-v4.coinglass.com")
API_KEY = getenv_any(["CG_API_KEY","COINGLASS_API_KEY"])
DB_URL  = getenv_any(["DATABASE_URL","SUPABASE_DB_URL"])

QPM   = int(getenv_any(["CG_QPM"], "80"))               # 調用/分鐘，最大 80
SLEEP = 60.0 / max(min(QPM, 80), 1)

API_PAGE_LIMIT = int(getenv_any(["CG_API_LIMIT"], "4500"))    # v4 單請求上限
HTTP_TIMEOUT   = float(getenv_any(["CG_TIMEOUT"], "60"))      # 允許外部調整 HTTP 逾時
MAX_INSERT     = int(getenv_any(["DB_BATCH_LIMIT"], "20000")) # 單批入庫上限

START_DATE = getenv_any(["START_DATE"], "2015-01-01")
END_DATE   = getenv_any(["END_DATE"],   None)

EXCHANGES   = [x for x in getenv_any(["CG_EXCHANGES"], "Binance").split(",") if x]
# 支援「多組 exchange_list」以分號分隔；每組內用逗號（例：Binance,OKX,Bybit;Bybit,Deribit）
EXLISTS_RAW = getenv_any(["CG_EXLISTS"], "Binance,OKX,Bybit")
EXLISTS     = [s.strip() for s in EXLISTS_RAW.split(";") if s.strip()]

COINS = [x for x in getenv_any(["CG_COINS"], "BTC,ETH,XRP,BNB,SOL,DOGE,ADA").split(",") if x]
def _pairs_env(key: str):
    v = getenv_any([key], "")
    if v:
        return [x for x in v.split(",") if x]
    # default: 由 COINS 自動衍生 *USDT 對
    return [f"{c}USDT" for c in COINS]
FUT_PAIRS = _pairs_env("CG_FUT_PAIRS")
SPOT_PAIRS= _pairs_env("CG_SPOT_PAIRS")

# -------- 日誌 --------
def log(msg:str):
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)

# -------- HTTP + 限流 --------
SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({
        "accept": "application/json",
        "User-Agent": "coinglass-supabase-ingestor/1.3",
        "CG-API-KEY": API_KEY,        # v4 header
        "coinglassSecret": API_KEY    # 容錯
    })

_NEXT_AT = 0.0
def _throttle():
    global _NEXT_AT
    now = time.time()
    if now < _NEXT_AT:
        time.sleep(_NEXT_AT - now)
    _NEXT_AT = max(now, _NEXT_AT) + SLEEP

def must_env():
    if not API_KEY or not DB_URL:
        raise SystemExit("缺少環境變數：COINGLASS_API_KEY/CG_API_KEY 或 SUPABASE_DB_URL/DATABASE_URL")

# -------- 工具 --------
# --- 新增工具函式：把 DB URL 加上 hostaddr=IPv4，並保留 host 作為 SNI ---
def _dsn_force_ipv4(dsn: str) -> str:
    try:
        u = urlparse(dsn.replace("postgres://", "postgresql://"))
        if u.scheme not in ("postgresql", "postgres"):
            return dsn
        host = u.hostname
        port = u.port or 5432
        if not host:
            return dsn
        # 解析第一個 IPv4
        ipv4 = next((ai[4][0] for ai in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)), None)
        if not ipv4:
            return dsn
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        # 連線走 IPv4，SNI/證書仍用原 host
        q.setdefault("hostaddr", ipv4)
        q.setdefault("sslmode", "require")
        new = u._replace(query=urlencode(q))
        return urlunparse(new)
    except Exception:
        return dsn

def to_utc_ts(x) -> Optional[dt.datetime]:
    if x is None: return None
    if isinstance(x, (int, float)):
        v = int(x)
        if v > 10**12: v //= 1000
        return dt.datetime.fromtimestamp(v/1000 if v>=10**11 else v, tz=dt.timezone.utc)
    if isinstance(x, str):
        try:
            d = dt.datetime.fromisoformat(x.replace("Z","+00:00"))
            if d.tzinfo is None: d = d.replace(tzinfo=dt.timezone.utc)
            return d.astimezone(dt.timezone.utc)
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d"):
                try:
                    d = dt.datetime.strptime(x, fmt)
                    return d.replace(tzinfo=dt.timezone.utc)
                except ValueError:
                    continue
            return None
    if isinstance(x, dt.datetime):
        return x if x.tzinfo else x.replace(tzinfo=dt.timezone.utc)
    return None

def daterange_utc() -> Tuple[int, int]:
    s = dt.datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    if END_DATE:
        e = dt.datetime.strptime(END_DATE, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    else:
        today = dt.datetime.now(dt.timezone.utc).date()
        e = dt.datetime(today.year, today.month, today.day, tzinfo=dt.timezone.utc) - dt.timedelta(days=1)
    return int(s.timestamp()*1000), int(e.timestamp()*1000)

def as_list(obj: Any) -> List[Dict[str, Any]]:
    if obj is None: return []
    if isinstance(obj, list): return obj
    if isinstance(obj, dict):
        for k in ("data","list","rows","records","values","items","points","candles","klines","details","result"):
            v = obj.get(k)
            if isinstance(v, list): return v
        v = obj.get("data")
        if isinstance(v, dict):
            for k in ("data","list","rows","records","values","items","points","candles","klines","details","result"):
                vv = v.get(k)
                if isinstance(vv, list): return vv
    return []

def first(obj: Dict[str,Any], *keys):
    for k in keys:
        if isinstance(obj, dict) and k in obj and obj[k] not in (None,""):
            return obj[k]
    return None

def fnum(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

class ApiError(RuntimeError):
    pass

def req(path: str, params: Dict[str,Any]) -> Any:
    url = BASE.rstrip("/") + path
    _throttle()
    try:
        r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
    except Exception as e:
        raise ApiError(f"NETWORK {path} {params} -> {e}")
    txt = r.text[:300].replace("\n"," ")
    if r.status_code != 200:
        raise ApiError(f"HTTP {r.status_code} {path} {params} -> {txt}")
    try:
        obj = r.json()
    except Exception:
        raise ApiError(f"NONJSON {path} {params} -> {txt}")
    if isinstance(obj, dict):
        code = str(obj.get("code","0"))
        if code != "0":
            msg = obj.get("msg")
            log(f"[req] {path} code={code} msg={msg}")
            raise ApiError(f"CODE {code} {msg}")
        return obj.get("data", obj)
    return obj

def pull_range(path: str, base_params: Dict[str,Any], start_ms: int, end_ms: int, tkey: str="time") -> List[Dict[str,Any]]:
    """首頁不帶時間，limit 設定可由 base_params['limit'] 覆蓋；以最老 time 作 end_time 往前翻。
       自動偵測時間欄位：time / timestamp / ts / t / date。
    """
    def _aug(p: Dict[str,Any]) -> Dict[str,Any]:
        q = dict(p)
        if "/futures/" in path:
            if "symbol" in q and "pair" not in q: q["pair"] = q["symbol"]
            q.setdefault("market","futures"); q.setdefault("marketType","futures")
            q.setdefault("type","futures");   q.setdefault("category","futures")
        if "/spot/" in path:
            if "symbol" in q and "pair" not in q: q["pair"] = q["symbol"]
            q.setdefault("market","spot"); q.setdefault("marketType","spot")
            q.setdefault("type","spot");   q.setdefault("category","spot")
        return q

    def _to_ms(val) -> Optional[int]:
        if val is None: return None
        if isinstance(val, (int, float)):
            v = int(val)
            if v >= 10**12 or v >= 10**10:  # ms
                return v
            return v * 1000                  # sec -> ms
        dtv = to_utc_ts(val)
        return int(dtv.timestamp()*1000) if dtv else None

    def _detect_tkey(sample: Dict[str,Any], prefer: str) -> Optional[str]:
        if prefer in sample and sample[prefer] is not None:
            return prefer
        for k in ("time","timestamp","ts","t","date"):
            if k in sample and sample[k] is not None:
                return k
        return None

    all_rows: List[Dict[str,Any]] = []
    cursor: Optional[int] = None
    tried_aug = False
    tk: Optional[str] = None
    log(f"[pull_range] 分頁抓取 {path} base={base_params}")

    # 允許每次呼叫自訂 limit；否則用全域
    base_limit = base_params.get("limit", API_PAGE_LIMIT)

    while True:
        p = dict(base_params)
        p["limit"] = base_limit
        if cursor is not None:
            p["end_time"] = cursor

        try:
            d = req(path, p)
            lst = as_list(d)
            got = len(lst)
            log(f"[pull_range] got={got} cursor={cursor if cursor is not None else 'latest'}")
        except ApiError as e:
            if "limit" in str(e).lower() and base_limit > 4500:
                p["limit"] = 4500
                d = req(path, p)
                lst = as_list(d)
                got = len(lst)
                log(f"[pull_range] retry got={got} with limit=4500")
            else:
                log(f"[pull_range] error: {e}")
                lst=[]; got=0

        # 若第一頁沒資料，嘗試補齊 futures/spot 類別參數
        if got == 0 and cursor is None and not tried_aug:
            tried_aug = True
            p2 = _aug({k:v for k,v in base_params.items()})
            p2["limit"] = base_limit
            try:
                d2 = req(path, p2)
                lst2 = as_list(d2)
            except ApiError as e:
                log(f"[pull_range] aug error: {e}")
                lst2=[]
            log(f"[pull_range] augmented got={len(lst2)} with params={p2}")
            lst = lst2

        if not lst:
            break

        # 偵測時間欄位
        if tk is None:
            tk = _detect_tkey(lst[0], tkey)
            if tk is None:
                log("[pull_range] 無時間欄位可辨識，跳過頁面")
                break

        # 篩選在區間內
        page_ms = []
        for it in lst:
            msv = _to_ms(it.get(tk))
            if msv is None:
                continue
            page_ms.append(msv)
            if start_ms <= msv <= end_ms:
                all_rows.append(it)

        if not page_ms:
            break

        oldest = min(page_ms)
        if oldest <= start_ms:
            break
        cursor = oldest - 1

    if not all_rows:
        return []
    out, seen = [], set()
    for it in sorted(all_rows, key=lambda x: (_to_ms(x.get(tk)) or 0)):
        msv = _to_ms(it.get(tk))
        if msv is None or msv in seen:
            continue
        seen.add(msv)
        out.append(it)
    return out

# -------- DB --------
# --- 修改 pg()：預設啟用 IPv4；失敗時再重試一次 ---
def pg():
    dsn = DB_URL
    if getenv_any(["DB_FORCE_IPV4"], "1") == "1":
        dsn = _dsn_force_ipv4(dsn)
    try:
        return psycopg2.connect(dsn)
    except psycopg2.OperationalError as e:
        # 若仍遇到網路/解析問題再強制一次
        if "Network is unreachable" in str(e) or "could not translate host name" in str(e):
            return psycopg2.connect(_dsn_force_ipv4(DB_URL))
        raise

def upsert(conn, sql: str, rows: List[Tuple], table_label: str):
    if not rows:
        log(f"[{table_label}] 無資料可寫入")
        return
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), MAX_INSERT):
            part = rows[i:i+MAX_INSERT]
            execute_values(cur, sql, part, page_size=min(MAX_INSERT, 10000))
            total += len(part)
    conn.commit()
    log(f"[{table_label}] upsert rows = {total}")

def db_ping(conn):
    with conn.cursor() as cur:
        cur.execute("select current_database(), current_user, current_schema(), inet_server_addr(), inet_server_port();")
        db, usr, sch, ip, port = cur.fetchone()
        log(f"DB 連線 OK → db={db} user={usr} schema={sch} host={ip}:{port}")

# -------- Ingests --------
def ingest_futures_candles_1d(conn, exchanges=EXCHANGES, pairs=FUT_PAIRS):
    table="futures_candles_1d"
    sql = """
    insert into futures_candles_1d (exchange, symbol, ts_utc, open, high, low, close, volume_usd)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume_usd=excluded.volume_usd;
    """
    s_ms, e_ms = daterange_utc()
    for ex in exchanges:
        for sym in pairs:
            s_date = dt.datetime.fromtimestamp(s_ms/1000, tz=dt.timezone.utc).date()
            e_date = dt.datetime.fromtimestamp(e_ms/1000, tz=dt.timezone.utc).date()
            log(f"[{table}] {ex} {sym} 拉取 {s_date}~{e_date}")
            lst = pull_range("/api/futures/price/history",
                             {"exchange":ex, "symbol":sym, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{table}] {ex} {sym} 得 {len(lst)} 行")
            rows=[]
            for it in lst:
                rows.append((ex, sym, to_utc_ts(it.get("time")),
                             fnum(first(it,"open")), fnum(first(it,"high")),
                             fnum(first(it,"low")),  fnum(first(it,"close")),
                             fnum(first(it,"volume_usd","volume"))))
            upsert(conn, sql, rows, table)

def ingest_spot_candles_1d(conn, exchanges=EXCHANGES, pairs=SPOT_PAIRS):
    table="spot_candles_1d"
    sql = """
    insert into spot_candles_1d (exchange, symbol, ts_utc, open, high, low, close, volume_usd)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume_usd=excluded.volume_usd;
    """
    s_ms, e_ms = daterange_utc()
    for ex in exchanges:
        for sym in pairs:
            s_date = dt.datetime.fromtimestamp(s_ms/1000, tz=dt.timezone.utc).date()
            e_date = dt.datetime.fromtimestamp(e_ms/1000, tz=dt.timezone.utc).date()
            log(f"[{table}] {ex} {sym} 拉取 {s_date}~{e_date}")
            lst = pull_range("/api/spot/price/history",
                             {"exchange":ex, "symbol":sym, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{table}] {ex} {sym} 得 {len(lst)} 行")
            rows=[]
            for it in lst:
                rows.append((ex, sym, to_utc_ts(it.get("time")),
                             fnum(first(it,"open")), fnum(first(it,"high")),
                             fnum(first(it,"low")),  fnum(first(it,"close")),
                             fnum(first(it,"volume_usd","volume"))))
            upsert(conn, sql, rows, table)

def ingest_oi_agg_1d(conn, coins=COINS):
    table="futures_oi_agg_1d"
    sql = """
    insert into futures_oi_agg_1d (symbol, ts_utc, open, high, low, close, unit)
    values %s
    on conflict (symbol, ts_utc, unit)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for c in coins:
        lst = pull_range("/api/futures/open-interest/aggregated-history",
                         {"symbol":c, "interval":"1d", "unit":"usd"}, s_ms, e_ms, "time")
        log(f"[{table}] {c} 得 {len(lst)} 行")
        for it in lst:
            rows.append((c, to_utc_ts(it.get("time")),
                         fnum(it.get("open")), fnum(it.get("high")),
                         fnum(it.get("low")),  fnum(it.get("close")), "usd"))
    upsert(conn, sql, rows, table)

def ingest_oi_stable_1d(conn, coins=COINS, exlists=EXLISTS):
    table="futures_oi_stablecoin_1d"
    sql = """
    insert into futures_oi_stablecoin_1d (exchange_list, symbol, ts_utc, open, high, low, close)
    values %s
    on conflict (exchange_list, symbol, ts_utc)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for el in exlists:
        for c in coins:
            base = {"exchange_list":el, "symbol":c, "interval":"1d"}
            # 僅對 BTC 降低單請求上限，降低超時風險
            if c == "BTC" and API_PAGE_LIMIT > 3000:
                base["limit"] = 3000
            lst = pull_range("/api/futures/open-interest/aggregated-stablecoin-history",
                             base, s_ms, e_ms, "time")
            log(f"[{table}] {el}|{c} 得 {len(lst)} 行")
            for it in lst:
                rows.append((el, c, to_utc_ts(it.get("time")),
                             fnum(it.get("open")), fnum(it.get("high")),
                             fnum(it.get("low")),  fnum(it.get("close"))))
    upsert(conn, sql, rows, table)

def ingest_oi_coinm_1d(conn, coins=COINS, exlists=EXLISTS):
    table="futures_oi_coin_margin_1d"
    sql = """
    insert into futures_oi_coin_margin_1d (exchange_list, symbol, ts_utc, open, high, low, close)
    values %s
    on conflict (exchange_list, symbol, ts_utc)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for el in exlists:
        for c in coins:
            lst = pull_range("/api/futures/open-interest/aggregated-coin-margin-history",
                             {"exchange_list":el, "symbol":c, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{table}] {el}|{c} 得 {len(lst)} 行")
            for it in lst:
                rows.append((el, c, to_utc_ts(it.get("time")),
                             fnum(it.get("open")), fnum(it.get("high")),
                             fnum(it.get("low")),  fnum(it.get("close"))))
    upsert(conn, sql, rows, table)

def ingest_funding_1d(conn, coins=COINS):
    t1, t2 = "funding_oi_weight_1d","funding_vol_weight_1d"
    sql_oi = """
    insert into funding_oi_weight_1d (symbol, ts_utc, open, high, low, close)
    values %s
    on conflict (symbol, ts_utc)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close;
    """
    sql_vol = """
    insert into funding_vol_weight_1d (symbol, ts_utc, open, high, low, close)
    values %s
    on conflict (symbol, ts_utc)
    do update set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close;
    """
    s_ms, e_ms = daterange_utc()
    rows_oi, rows_vol = [], []
    for c in coins:
        l1 = pull_range("/api/futures/funding-rate/oi-weight-history",
                        {"symbol":c, "interval":"1d"}, s_ms, e_ms, "time")
        log(f"[{t1}] {c} 得 {len(l1)} 行")
        for it in l1:
            rows_oi.append((c, to_utc_ts(it.get("time")),
                            fnum(it.get("open")), fnum(it.get("high")),
                            fnum(it.get("low")),  fnum(it.get("close"))))
        l2 = pull_range("/api/futures/funding-rate/vol-weight-history",
                        {"symbol":c, "interval":"1d"}, s_ms, e_ms, "time")
        log(f"[{t2}] {c} 得 {len(l2)} 行")
        for it in l2:
            rows_vol.append((c, to_utc_ts(it.get("time")),
                             fnum(it.get("open")), fnum(it.get("high")),
                             fnum(it.get("low")),  fnum(it.get("close"))))
    upsert(conn, sql_oi, rows_oi, t1)
    upsert(conn, sql_vol, rows_vol, t2)

def ingest_long_short_1d(conn, exchanges=EXCHANGES, pairs=FUT_PAIRS):
    t1,t2,t3 = "long_short_global_1d","long_short_top_accounts_1d","long_short_top_positions_1d"
    sql1 = """
    insert into long_short_global_1d (exchange, symbol, ts_utc, long_percent, short_percent, long_short_ratio)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set long_percent=excluded.long_percent, short_percent=excluded.short_percent, long_short_ratio=excluded.long_short_ratio;
    """
    sql2 = """
    insert into long_short_top_accounts_1d (exchange, symbol, ts_utc, long_percent, short_percent, long_short_ratio)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set long_percent=excluded.long_percent, short_percent=excluded.short_percent, long_short_ratio=excluded.long_short_ratio;
    """
    sql3 = """
    insert into long_short_top_positions_1d (exchange, symbol, ts_utc, long_percent, short_percent, long_short_ratio)
    values %s
    on conflict (exchange, symbol, ts_utc)
    do update set long_percent=excluded.long_percent, short_percent=excluded.short_percent, long_short_ratio=excluded.long_short_ratio;
    """
    s_ms, e_ms = daterange_utc()
    rows1, rows2, rows3 = [], [], []
    for ex in exchanges:
        for sym in pairs:
            l1 = pull_range("/api/futures/global-long-short-account-ratio/history",
                            {"exchange":ex, "symbol":sym, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{t1}] {ex}|{sym} 得 {len(l1)} 行")
            for it in l1:
                rows1.append((ex, sym, to_utc_ts(it.get("time")),
                              fnum(first(it,"global_account_long_percent")),
                              fnum(first(it,"global_account_short_percent")),
                              fnum(first(it,"global_account_long_short_ratio"))))
            l2 = pull_range("/api/futures/top-long-short-account-ratio/history",
                            {"exchange":ex, "symbol":sym, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{t2}] {ex}|{sym} 得 {len(l2)} 行")
            for it in l2:
                rows2.append((ex, sym, to_utc_ts(it.get("time")),
                              fnum(first(it,"top_account_long_percent")),
                              fnum(first(it,"top_account_short_percent")),
                              fnum(first(it,"top_account_long_short_ratio"))))
            l3 = pull_range("/api/futures/top-long-short-position-ratio/history",
                            {"exchange":ex, "symbol":sym, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{t3}] {ex}|{sym} 得 {len(l3)} 行")
            for it in l3:
                rows3.append((ex, sym, to_utc_ts(it.get("time")),
                              fnum(first(it,"top_position_long_percent")),
                              fnum(first(it,"top_position_short_percent")),
                              fnum(first(it,"top_position_long_short_ratio"))))
    upsert(conn, sql1, rows1, t1)
    upsert(conn, sql2, rows2, t2)
    upsert(conn, sql3, rows3, t3)

def ingest_liquidation_1d(conn, coins=COINS, exlists=EXLISTS):
    table="liquidation_agg_1d"
    sql = """
    insert into liquidation_agg_1d (exchange_list, symbol, ts_utc, long_liq_usd, short_liq_usd)
    values %s
    on conflict (exchange_list, symbol, ts_utc)
    do update set long_liq_usd=excluded.long_liq_usd, short_liq_usd=excluded.short_liq_usd;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for el in exlists:
        for c in coins:
            l = pull_range("/api/futures/liquidation/aggregated-history",
                           {"exchange_list":el, "symbol":c, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{table}] {el}|{c} 得 {len(l)} 行")
            for it in l:
                rows.append((el, c, to_utc_ts(it.get("time")),
                             fnum(first(it,"aggregated_long_liquidation_usd","long_liq_usd","long_liquidation_usd")),
                             fnum(first(it,"aggregated_short_liquidation_usd","short_liq_usd","short_liquidation_usd"))))
    upsert(conn, sql, rows, table)

def ingest_orderbook_agg_futures_1d(conn, coins=COINS, exlists=EXLISTS, range_pct="1"):
    table="orderbook_agg_futures_1d"
    sql = """
    insert into orderbook_agg_futures_1d (exchange_list, symbol, ts_utc, bids_usd, bids_qty, asks_usd, asks_qty, range_pct)
    values %s
    on conflict (exchange_list, symbol, ts_utc, range_pct)
    do update set bids_usd=excluded.bids_usd, bids_qty=excluded.bids_qty, asks_usd=excluded.asks_usd, asks_qty=excluded.asks_qty;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for el in exlists:
        for c in coins:
            l = pull_range("/api/futures/orderbook/aggregated-ask-bids-history",
                           {"exchange_list":el, "symbol":c, "interval":"1d", "range":range_pct}, s_ms, e_ms, "time")
            log(f"[{table}] {el}|{c} 得 {len(l)} 行")
            for it in l:
                rows.append((el, c, to_utc_ts(it.get("time")),
                             fnum(first(it,"aggregated_bids_usd","bids_usd")),
                             fnum(first(it,"aggregated_bids_quantity","bids_qty")),
                             fnum(first(it,"aggregated_asks_usd","asks_usd")),
                             fnum(first(it,"aggregated_asks_quantity","asks_qty")),
                             fnum(range_pct)))
    upsert(conn, sql, rows, table)

def ingest_taker_vol_futures_1d(conn, coins=COINS, exlists=EXLISTS):
    table="taker_vol_agg_futures_1d"
    sql = """
    insert into taker_vol_agg_futures_1d (exchange_list, symbol, ts_utc, buy_vol_usd, sell_vol_usd)
    values %s
    on conflict (exchange_list, symbol, ts_utc)
    do update set buy_vol_usd=excluded.buy_vol_usd, sell_vol_usd=excluded.sell_vol_usd;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for el in exlists:
        for c in coins:
            l = pull_range("/api/futures/aggregated-taker-buy-sell-volume/history",
                           {"exchange_list":el, "symbol":c, "interval":"1d", "unit":"usd"}, s_ms, e_ms, "time")
            log(f"[{table}] {el}|{c} 得 {len(l)} 行")
            for it in l:
                rows.append((el, c, to_utc_ts(it.get("time")),
                             fnum(first(it,"aggregated_buy_volume_usd","buy_vol_usd","buy_volume_usd")),
                             fnum(first(it,"aggregated_sell_volume_usd","sell_vol_usd","sell_volume_usd"))))
    upsert(conn, sql, rows, table)

def ingest_etf_bitcoin_flow_and_aum(conn):
    t_flow, t_aum = "etf_bitcoin_flow_1d", "etf_bitcoin_net_assets_1d"
    sql_flow = """
    insert into etf_bitcoin_flow_1d (date_utc, total_flow_usd, price_usd, details)
    values %s
    on conflict (date_utc) do update set total_flow_usd=excluded.total_flow_usd, price_usd=excluded.price_usd, details=excluded.details;
    """
    sql_aum = """
    insert into etf_bitcoin_net_assets_1d (date_utc, net_assets_usd, change_usd, price_usd)
    values %s
    on conflict (date_utc) do update set net_assets_usd=excluded.net_assets_usd, change_usd=excluded.change_usd, price_usd=excluded.price_usd;
    """
    rows_flow, rows_aum = [], []
    d = req("/api/etf/bitcoin/flow-history", {})
    lst = as_list(d); log(f"[{t_flow}] 取得 {len(lst)} 天")
    for it in lst:
        date_utc = dt.datetime.fromtimestamp(int(first(it,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        flow = fnum(first(it,"flow_usd","total_flow_usd","net_flow_usd","flow"))
        price = fnum(first(it,"price_usd","price","btc_price_usd","btc_price"))
        details = first(it,"etf_flows","details","list") or []
        rows_flow.append((date_utc, flow, price, json.dumps(details)))
    upsert(conn, sql_flow, rows_flow, t_flow)

    d = req("/api/etf/bitcoin/net-assets/history", {})
    lst = as_list(d); log(f"[{t_aum}] 取得 {len(lst)} 天")
    for it in lst:
        date_utc = dt.datetime.fromtimestamp(int(first(it,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        rows_aum.append((date_utc,
                         fnum(first(it,"net_assets_usd","aum_usd")),
                         fnum(first(it,"change_usd","delta_usd","net_change_usd")),
                         fnum(first(it,"price_usd","price","btc_price_usd","btc_price"))))
    upsert(conn, sql_aum, rows_aum, t_aum)

def ingest_etf_premium_discount(conn, tickers: List[str]=None):
    table="etf_premium_discount_1d"
    sql = """
    insert into etf_premium_discount_1d (date_utc, ticker, nav_usd, market_price_usd, premium_discount)
    values %s
    on conflict (date_utc, ticker) do update set nav_usd=excluded.nav_usd, market_price_usd=excluded.market_price_usd, premium_discount=excluded.premium_discount;
    """
    rows=[]
    d = req("/api/etf/bitcoin/premium-discount/history", {})
    outer = as_list(d); log(f"[{table}] 天數={len(outer)}")
    for day in outer:
        date_utc = dt.datetime.fromtimestamp(int(first(day,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        inner = day.get("list") if isinstance(day, dict) else None
        for item in as_list(inner if inner is not None else day):
            t = item.get("ticker")
            if (not tickers) or (t in tickers):
                rows.append((date_utc,
                             t,
                             fnum(first(item,"nav_usd","nav")),
                             fnum(first(item,"market_price_usd","price_usd","price")),
                             fnum(first(item,"premium_discount","premium_discount_rate","discount_rate"))))
    upsert(conn, sql, rows, table)

def ingest_hk_etf_flow(conn):
    table="hk_etf_flow_1d"
    sql = """
    insert into hk_etf_flow_1d (date_utc, total_flow_usd, price_usd, details)
    values %s
    on conflict (date_utc) do update set total_flow_usd=excluded.total_flow_usd, price_usd=excluded.price_usd, details=excluded.details;
    """
    rows=[]
    d = req("/api/hk-etf/bitcoin/flow-history", {})
    lst = as_list(d); log(f"[{table}] 取得 {len(lst)} 天")
    for it in lst:
        date_utc = dt.datetime.fromtimestamp(int(first(it,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        flow = fnum(first(it,"flow_usd","total_flow_usd","net_flow_usd","flow"))
        price = fnum(first(it,"price_usd","price","btc_price_usd","btc_price"))
        details = first(it,"etf_flows","details","list") or []
        rows.append((date_utc, flow, price, json.dumps(details)))
    upsert(conn, sql, rows, table)

def ingest_coinbase_premium_index_1d(conn):
    table="coinbase_premium_index_1d"
    sql = """
    insert into coinbase_premium_index_1d (ts_utc, premium_usd, premium_rate)
    values %s
    on conflict (ts_utc) do update set premium_usd=excluded.premium_usd, premium_rate=excluded.premium_rate;
    """
    s_ms, e_ms = daterange_utc()
    lst = pull_range("/api/coinbase-premium-index",
                     {"interval":"1d"}, s_ms, e_ms, "time")
    log(f"[{table}] 得 {len(lst)} 行")
    rows=[]
    for it in lst:
        rows.append((to_utc_ts(first(it,"time","timestamp")),
                     fnum(first(it,"premium","premium_usd")),
                     fnum(first(it,"premium_rate","rate"))))
    upsert(conn, sql, rows, table)

def ingest_bitfinex_margin_ls_1d(conn, coins=COINS):
    table="bitfinex_margin_long_short_1d"
    sql = """
    insert into bitfinex_margin_long_short_1d (symbol, ts_utc, long_qty, short_qty)
    values %s
    on conflict (symbol, ts_utc) do update set long_qty=excluded.long_qty, short_qty=excluded.short_qty;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for c in coins:
        lst = pull_range("/api/bitfinex-margin-long-short",
                         {"symbol":c, "interval":"1d"}, s_ms, e_ms, "time")
        log(f"[{table}] {c} 得 {len(lst)} 行")
        for it in lst:
            rows.append((c, to_utc_ts(first(it,"time","timestamp")),
                         fnum(first(it,"long_quantity","long_qty")),
                         fnum(first(it,"short_quantity","short_qty"))))
    upsert(conn, sql, rows, table)

def ingest_borrow_ir_1d(conn, exchanges=EXCHANGES, coins=COINS):
    table="borrow_interest_rate_1d"
    sql = """
    insert into borrow_interest_rate_1d (exchange, symbol, ts_utc, interest_rate)
    values %s
    on conflict (exchange, symbol, ts_utc) do update set interest_rate=excluded.interest_rate;
    """
    s_ms, e_ms = daterange_utc()
    rows=[]
    for ex in exchanges:
        for c in coins:
            lst = pull_range("/api/borrow-interest-rate/history",
                             {"exchange":ex, "symbol":c, "interval":"1d"}, s_ms, e_ms, "time")
            log(f"[{table}] {ex}|{c} 得 {len(lst)} 行")
            for it in lst:
                rows.append((ex, c, to_utc_ts(first(it,"time","timestamp")),
                             fnum(first(it,"interest_rate","rate"))))
    upsert(conn, sql, rows, table)

def ingest_indices_daily(conn):
    t1,t2,t3 = "idx_puell_multiple_daily","idx_stock_to_flow_daily","idx_pi_cycle_daily"
    sql_puell = "insert into idx_puell_multiple_daily (date_utc, price, puell_multiple) values %s on conflict (date_utc) do update set price=excluded.price, puell_multiple=excluded.puell_multiple;"
    sql_s2f   = "insert into idx_stock_to_flow_daily (date_utc, price, next_halving) values %s on conflict (date_utc) do update set price=excluded.price, next_halving=excluded.next_halving;"
    sql_pi    = "insert into idx_pi_cycle_daily (date_utc, price, ma_110, ma_350_x2) values %s on conflict (date_utc) do update set price=excluded.price, ma_110=excluded.ma_110, ma_350_x2=excluded.ma_350_x2;"

    rows=[]
    d = req("/api/index/puell-multiple", {}); lst = as_list(d); log(f"[{t1}] 天數={len(lst)}")
    for it in lst:
        date_utc = dt.datetime.fromtimestamp(int(first(it,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        rows.append((date_utc, fnum(first(it,"price","price_usd")), fnum(first(it,"puell_multiple","puell"))))
    upsert(conn, sql_puell, rows, t1); rows.clear()

    d = req("/api/index/stock-flow", {}); lst = as_list(d); log(f"[{t2}] 天數={len(lst)}")
    for it in lst:
        date_utc = dt.datetime.fromtimestamp(int(first(it,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        rows.append((date_utc, fnum(first(it,"price","price_usd")), int(first(it,"next_halving","next_halving_epoch") or 0)))
    upsert(conn, sql_s2f, rows, t2); rows.clear()

    d = req("/api/index/pi-cycle-indicator", {}); lst = as_list(d); log(f"[{t3}] 天數={len(lst)}")
    for it in lst:
        date_utc = dt.datetime.fromtimestamp(int(first(it,"timestamp","time"))/1000.0, tz=dt.timezone.utc).date()
        ma350x2 = first(it,"ma_350_mu_2","ma_350_x2")
        rows.append((date_utc, fnum(first(it,"price","price_usd")), fnum(first(it,"ma_110")), fnum(ma350x2)))
    upsert(conn, sql_pi, rows, t3)

# -------- 入口 --------
TASKS = [x.strip() for x in getenv_any(["CG_TASKS","TASKS"], "").split(",") if x.strip()]

def run_all():
    must_env()
    log(f"啟動，限流 {min(QPM,80)} req/min，BASE={BASE}")
    conn = pg()
    db_ping(conn)

    pipeline = [
        ("futures_candles_1d",             lambda: ingest_futures_candles_1d(conn)),
        ("spot_candles_1d",                lambda: ingest_spot_candles_1d(conn)),
        ("oi_agg_1d",                      lambda: ingest_oi_agg_1d(conn)),
        ("oi_stable_1d",                   lambda: ingest_oi_stable_1d(conn)),
        ("oi_coinm_1d",                    lambda: ingest_oi_coinm_1d(conn)),
        ("funding_1d",                     lambda: ingest_funding_1d(conn)),
        ("long_short_1d",                  lambda: ingest_long_short_1d(conn)),
        ("liquidation_1d",                 lambda: ingest_liquidation_1d(conn)),
        ("orderbook_agg_futures_1d",       lambda: ingest_orderbook_agg_futures_1d(conn)),
        ("taker_vol_agg_futures_1d",       lambda: ingest_taker_vol_futures_1d(conn)),
        ("etf_bitcoin_flow_aum",           lambda: ingest_etf_bitcoin_flow_and_aum(conn)),
        ("etf_premium_discount_1d",        lambda: ingest_etf_premium_discount(conn, tickers=None)),
        ("hk_etf_flow_1d",                 lambda: ingest_hk_etf_flow(conn)),
        ("coinbase_premium_index_1d",      lambda: ingest_coinbase_premium_index_1d(conn)),
        ("bitfinex_margin_long_short_1d",  lambda: ingest_bitfinex_margin_ls_1d(conn)),  # 預設用 COINS
        ("borrow_interest_rate_1d",        lambda: ingest_borrow_ir_1d(conn)),
        ("indices_daily",                  lambda: ingest_indices_daily(conn)),
    ]

    for name, fn in pipeline:
        if TASKS and name not in TASKS:
            continue
        fn()

    conn.close()
    log("完成")

if __name__ == "__main__":
    try:
        run_all()
    except Exception as e:
        log(f"致命錯誤：{e}")
        raise
