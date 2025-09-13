# scripts/test_conn.py
# -*- coding: utf-8 -*-
import os
import psycopg2

dsn = (
    os.getenv("SUPABASE_DB_URL")      # 你的正式 DSN（含 6543）
    or os.getenv("DATABASE_URL")       # 兼容舊名稱
    or os.getenv("PG_DSN")
)
if not dsn:
    raise SystemExit("❌ Missing DSN: please set SUPABASE_DB_URL (repository secret).")

conn = psycopg2.connect(dsn, sslmode=os.getenv("PGSSLMODE", "require"))
with conn.cursor() as cur:
    cur.execute("select 1")
    print(f"[test_conn] ok, select 1 -> {cur.fetchone()[0]}")
conn.close()
