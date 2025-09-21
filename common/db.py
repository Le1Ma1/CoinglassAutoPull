# common/db.py
import os, socket, time
import psycopg2
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _getenv(*names, default=None):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

def _dsn_force_ipv4(dsn: str) -> str:
    try:
        u = urlparse(dsn.replace("postgres://", "postgresql://"))
        if u.scheme not in ("postgresql", "postgres"):
            return dsn
        host, port = u.hostname, u.port or 5432
        if not host:
            return dsn
        ipv4 = next((ai[4][0] for ai in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)), None)
        if not ipv4:
            return dsn
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        q.setdefault("hostaddr", ipv4)
        q.setdefault("sslmode", "require")
        return urlunparse(u._replace(query=urlencode(q)))
    except Exception:
        return dsn

def connect(max_retries: int = 2, backoff: float = 1.5):
    dsn = _getenv("SUPABASE_DB_URL", "DATABASE_URL")
    if not dsn:
        raise RuntimeError("缺少 DATABASE_URL 或 SUPABASE_DB_URL")
    if _getenv("DB_FORCE_IPV4", default="1") == "1":
        dsn = _dsn_force_ipv4(dsn)

    last_err = None
    for i in range(max_retries + 1):
        try:
            conn = psycopg2.connect(dsn, connect_timeout=int(_getenv("PG_CONNECT_TIMEOUT", default="10") or 10))
            with conn.cursor() as cur:
                cur.execute("set timezone='UTC'; set statement_timeout = %s;", (int(_getenv("PG_STMT_TIMEOUT", default="30000")),))
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            if i == max_retries:
                break
            time.sleep(backoff ** i)
    raise last_err

def ping(conn):
    with conn.cursor() as cur:
        cur.execute("select current_database(), current_user, inet_server_addr(), inet_server_port();")
        return cur.fetchone()

def exec_values(conn, sql: str, rows, page_size: int = 1000):
    from psycopg2.extras import execute_values as _ev
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), page_size):
            part = rows[i:i+page_size]
            _ev(cur, sql, part, page_size=page_size)
            total += len(part)
    conn.commit()
    return total
