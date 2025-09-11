# src/common/db.py
import os, psycopg2, urllib.parse
from dotenv import load_dotenv
load_dotenv()

def connect():
    sslmode = os.getenv("PGSSLMODE", "require")

    # ✅ 統一使用 SUPABASE_DB_URL
    dsn = os.getenv("SUPABASE_DB_URL")
    if dsn:
        return psycopg2.connect(dsn, sslmode=sslmode)

    # 備用：PGHOST/PGUSER/PGPASSWORD
    host = os.getenv("PGHOST")
    if host:
        return psycopg2.connect(
            host=host,
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE", "postgres"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            sslmode=sslmode
        )

    raise RuntimeError("No Postgres connection info found.")
