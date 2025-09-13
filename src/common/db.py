# src/common/db.py
# -*- coding: utf-8 -*-
import os
import psycopg2

def get_conn():
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL/DATABASE_URL 未設定，無法連線資料庫。")
    if "sslmode=" not in dsn:
        dsn = dsn + ("?sslmode=require" if "?" not in dsn else "&sslmode=require")
    return psycopg2.connect(dsn)
