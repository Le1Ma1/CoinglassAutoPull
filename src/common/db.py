# -*- coding: utf-8 -*-
"""
共用資料庫連線：
優先讀 SUPABASE_DB_URL；若未設定則退回 DATABASE_URL。
此模組僅提供讀寫連線；實際上傳 features/labels 仍由你既有 upload 模組負責。
"""
import os
import psycopg2

def _dsn() -> str:
    url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL).")
    return url

def get_conn():
    return psycopg2.connect(_dsn())
