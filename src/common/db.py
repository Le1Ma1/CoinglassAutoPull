# -*- coding: utf-8 -*-
"""
共用資料庫連線：
優先讀 SUPABASE_DB_URL；若未設定則退回 DATABASE_URL（或 PG_DSN 以相容舊腳本）。
同時提供 get_conn() 與 connect()（給舊腳本用）。
"""
from __future__ import annotations
import os
import psycopg2


def _dsn() -> str:
    url = (
        os.getenv("SUPABASE_DB_URL")
    )
    if not url:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL / PG_DSN).")
    return url


def get_conn():
    """主要入口：取得 psycopg2 連線"""
    return psycopg2.connect(_dsn())


# 兼容舊腳本（例如 scripts/test_conn.py 會 from src.common.db import connect）
def connect():
    return get_conn()
