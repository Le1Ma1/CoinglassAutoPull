import json, datetime as dt
from collections import deque
from common.db import connect
from common.utils import log, json_dumps, winsor

TASK = dict(
    name="feat_cpi",
    kind="feature",
    provides=["cpi_*"],
    depends_on=["coinbase_premium_index_1d"],
    default_days_back=400,
)

SCORE_VER = "cpi_v1"
NS = "cpi"

def _calc_series(rates):
    z60, ewz20, rank252, spike2, spike3, streak = [], [], [], [], [], []
    w60, w20, w252 = deque(), deque(), deque()
    last_sign, cur_streak = 0, 0
    for r in rates:
        # 60D z
        w60.append(r)
        if len(w60) > 60: w60.popleft()
        arr60 = [x for x in w60 if x is not None]
        mu = (sum(arr60) / len(arr60)) if arr60 else None
        sd = None
        if arr60 and len(arr60) > 1:
            var = sum((x - mu) ** 2 for x in arr60) / (len(arr60) - 1)
            sd = var ** 0.5
        z = (r - mu) / sd if (r is not None and mu is not None and sd and sd > 0) else None
        z60.append(z)
        # 20D 平滑
        w20.append(0.0 if z is None else z)
        if len(w20) > 20: w20.popleft()
        ewz20.append(sum(w20) / len(w20))
        # 252D rank
        w252.append(r)
        if len(w252) > 252: w252.popleft()
        arr252 = [x for x in w252 if x is not None]
        rk = None if (r is None or not arr252) else sum(1 for x in arr252 if x <= r) / len(arr252)
        rank252.append(rk)
        # spikes + streak
        s2 = 1 if (z is not None and z >= 2) else (-1 if (z is not None and z <= -2) else 0)
        s3 = 1 if (z is not None and z >= 3) else (-1 if (z is not None and z <= -3) else 0)
        spike2.append(s2); spike3.append(s3)
        sg = 0 if z is None else (1 if z > 0 else (-1 if z < 0 else 0))
        cur_streak = (cur_streak + 1) if (sg != 0 and sg == last_sign) else (1 if sg != 0 else 0)
        last_sign = sg if sg != 0 else last_sign
        streak.append(min(cur_streak, 10))
    return z60, ewz20, rank252, spike2, spike3, streak

def run(conn, since=None, until=None, days_back=None):
    cur = conn.cursor()

    # 修正：用子查詢聚合，與 bounds CROSS JOIN
    cur.execute("""
    with src as (
      select premium_rate::float8 as r
      from public.coinbase_premium_index_1d
      where premium_rate is not null
    ),
    bounds as (
      select min(date_utc) as min_d, max(date_utc) as max_d
      from public.coinbase_premium_index_1d
    ),
    aggs as (
      select
        percentile_cont(0.01) within group (order by r) as p01,
        percentile_cont(0.99) within group (order by r) as p99
      from src
    )
    select a.p01, a.p99, b.min_d, b.max_d
    from aggs a cross join bounds b;
    """)
    p01, p99, _, max_d = cur.fetchone()

    if until is None: until = max_d
    if since is None: since = until - dt.timedelta(days=(days_back or TASK["default_days_back"]))

    # 取近窗
    cur.execute("""
      select date_utc::date, premium_rate::float8
      from public.coinbase_premium_index_1d
      where date_utc between %s and %s
      order by date_utc
    """, (since, until))
    recs = cur.fetchall()
    if not recs:
        cur.close()
        return {"updated_rows": 0, "start": since, "end": until, "ver": SCORE_VER}

    dates = [d for d, _ in recs]
    rates = [winsor(r, p01, p99) for _, r in recs]
    z60, ewz20, rank252, s2, s3, streak = _calc_series(rates)

    # 批次合併更新（只改 cpi_* 鍵）
    vals = []
    for d, r, z, ez, rk, a, b, st in zip(dates, rates, z60, ewz20, rank252, s2, s3, streak):
        feats = json_dumps({
            f"{NS}_rate": r, f"{NS}_z60": z, f"{NS}_ewz20": ez,
            f"{NS}_rank252": rk, f"{NS}_spike2": a, f"{NS}_spike3": b,
            f"{NS}_streak": st, f"{NS}_na": False
        })
        vals.append((d, feats))

    n_btc = 0
    if vals:
        values_sql = b",".join(cur.mogrify("(%s,%s::jsonb)", v) for v in vals).decode()
        cur.execute(f"""
          update public.features_1d f
             set ext_features = coalesce(f.ext_features,'{{}}'::jsonb) || v.feats,
                 score_ver    = %s
            from (values {values_sql}) as v(date_utc, feats)
           where f.asset='BTC' and f.date_utc=v.date_utc;
        """, (SCORE_VER,))
        n_btc = cur.rowcount

    # ETH 遮罩（僅一次）
    cur.execute("""
      update public.features_1d
         set ext_features = coalesce(ext_features,'{}'::jsonb) || jsonb_build_object('cpi_na', true)
       where asset='ETH' and (ext_features->>'cpi_na') is null;
    """)
    n_eth_mask = cur.rowcount

    conn.commit()
    cur.close()
    return {"updated_rows": int(n_btc), "mask_updates": int(n_eth_mask),
            "start": since, "end": until, "ver": SCORE_VER}

if __name__ == "__main__":
    conn = connect()
    log("DB 連線 OK")
    print(run(conn))
    conn.close()
