# src/common/db.py
import os, psycopg2, urllib.parse
from dotenv import load_dotenv
load_dotenv()

def connect():
    sslmode = os.getenv("PGSSLMODE", "require")

    # 1. 優先使用 SUPABASE_DB_URL / DATABASE_URL
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn, sslmode=sslmode)

    # 2. 傳統 PGHOST / PGUSER / PGPASSWORD
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

    # 3. PG_DSN fallback
    dsn = os.getenv("PG_DSN", "").strip()
    if dsn:
        u = urllib.parse.urlparse(dsn)
        return psycopg2.connect(
            host=u.hostname, port=u.port or 5432,
            dbname=(u.path or "/").lstrip("/"),
            user=urllib.parse.unquote(u.username or ""),
            password=urllib.parse.unquote(u.password or ""),
            sslmode=sslmode
        )

    raise RuntimeError("No Postgres connection info found.")
