```
CoinglassAutoPull/
├─ data_1d/                              # 匯出用的臨時 CSV 目錄
├─ scripts/                              # 排程腳本與工具
│  ├─ apply_sql.py                       # 連 DB 套用 SQL
│  ├─ export_from_db_1d.py               # 從 DB 匯出 1d 原始資料到 data_1d/
│  ├─ run_fast.sh                        # 近幾天快速回補（呼叫 Dataupsert.py）
│  ├─ run_full.sh                        # 全量修補（呼叫 Dataupsert.py）
│  ├─ run_features_1d.sh                 # 匯出→算特徵→套 SQL 的一鍵流程
│  └─ test_conn.py                       # DB 連線測試
├─ sql/                                  # 結構與特徵 SQL
│  ├─ schema.sql                         # 主要表結構
│  └─ features_1d.sql                    # features_1d 視圖/物化或相關 SQL
├─ src/                                  # 程式主模組
│  ├─ cli/
│  │  ├─ __init__.py
│  │  ├─ auto_features.py                # 檢查近7天完整性並觸發特徵流程
│  │  └─ build_and_upload_features_1d.py # 建特徵並上傳
│  ├─ common/
│  │  ├─ __init__.py
│  │  └─ db.py                           # 共用 DB 連線工具
│  ├─ etl/
│  │  ├─ build_coordinate.py             # ETL 對應/座標整理
│  │  └─ load_sources.py                 # 各來源載入
│  ├─ features/
│  │  └─ compute_features_1d.py          # 1d 特徵計算
│  └─ upload/
│     ├─ __init__.py
│     └─ copy_upsert.py                  # 目標表 upsert/搬運
├─ .env                                  # 環境變數（本機）
├─ .env.example
├─ .gitattributes
├─ .gitignore
├─ Dataupsert.py                         # 既有 ETL 主程式（被 run_fast/full 呼叫）
├─ render.yaml                           # Render Blueprint（所有 cron/worker 定義）
├─ requirements.txt                      # 依賴（pandas/psycopg2-binary 等）
└─ runtime.txt                           # Python 版本鎖定（3.11.9）
```