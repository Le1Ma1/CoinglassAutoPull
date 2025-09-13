# -*- coding: utf-8 -*-
"""
共用資料庫連線：
優先讀 SUPABASE_DB_URL；相容舊程式也接受 DATABASE_URL / PG_DSN。
提供 get_conn() 與 connect() 兩個名稱（後者給 legacy 腳本用）。
"""
import os
import psycopg2

def _dsn() -> str:
    url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or os.getenv("PG_DSN")
    if not url:
        raise RuntimeError("Missing SUPABASE_DB_URL / DATABASE_URL / PG_DSN")
    return url

def get_conn():
    return psycopg2.connect(_dsn())

# legacy alias
def connect():
    return get_conn()

__all__ = ["get_conn", "connect"]
