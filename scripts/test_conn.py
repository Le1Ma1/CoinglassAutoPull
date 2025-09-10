import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from src.common.db import connect

with connect() as c, c.cursor() as cur:
    cur.execute("select version()")
    print(cur.fetchone()[0])
