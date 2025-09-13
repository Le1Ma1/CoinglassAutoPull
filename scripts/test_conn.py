# -*- coding: utf-8 -*-
"""
簡單驗證 DB 連線是否可用（在 CI 上跑）
"""
from src.common.db import connect

with connect() as conn:
    with conn.cursor() as cur:
        cur.execute("select 1")
        one = cur.fetchone()[0]
        print(f"[test_conn] ok, select 1 -> {one}")
