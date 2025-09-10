import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from src.common.db import connect

if len(sys.argv) != 2:
    raise SystemExit("Usage: python scripts/apply_sql.py <path/to/sql>")
sql_path = pathlib.Path(sys.argv[1]).resolve()
sql = sql_path.read_text(encoding="utf-8")

with connect() as c, c.cursor() as cur:
    cur.execute(sql)
    c.commit()
print(f"Applied: {sql_path}")
