import os, psycopg2, urllib.parse
from dotenv import load_dotenv
load_dotenv()

def connect():
    # 優先使用分欄參數，避免殘留 PG_DSN 造成誤連
    host = os.getenv("PGHOST")
    sslmode = os.getenv("PGSSLMODE", "require")
    if host:
        return psycopg2.connect(
            host=host,
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE", "postgres"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            sslmode=sslmode
        )
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
