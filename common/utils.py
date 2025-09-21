# common/utils.py
import json, datetime as dt
import numpy as np

def log(msg: str):
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)

def json_dumps(obj) -> str:
    def _default(o):
        if isinstance(o, (np.float32, np.float64)): return float(o)
        if isinstance(o, (np.int32, np.int64)): return int(o)
        if isinstance(o, (dt.datetime, dt.date)): return o.isoformat()
        return str(o)
    return json.dumps(obj, default=_default, ensure_ascii=False)

def winsor(x, lo=None, hi=None):
    if x is None: return None
    if lo is not None and x < lo: x = lo
    if hi is not None and x > hi: x = hi
    return x
