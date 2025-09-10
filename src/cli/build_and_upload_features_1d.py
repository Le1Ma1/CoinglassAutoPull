import os, time
from dotenv import load_dotenv; load_dotenv()

from src.etl.load_sources import load_sources
from src.etl.build_coordinate import build_price_coordinate, left_join_all
from src.features.compute_features_1d import compute_features
from src.upload.copy_upsert import copy_upsert_chunks

def log(msg): print(f"[run] {msg}")

def main():
    t0 = time.perf_counter()
    # 讀來源
    S = load_sources()
    log("建立價格座標…")
    px = build_price_coordinate(S["fut"], S["spot"])
    log(f"座標 rows={len(px)}")

    log("左連接來源…")
    df = left_join_all(px, S)
    log(f"左連完成 rows={len(df)}")

    log("計算特徵（因果 ≤t）…")
    feat = compute_features(df).reset_index()
    log(f"特徵完成 rows={len(feat)}, cols={len(feat.columns)}")

    # 可選資產過濾
    assets = os.getenv("ASSETS")
    if assets:
        keep = {x.strip().upper() for x in assets.split(",")}
        feat = feat[feat["asset"].isin(keep)]
        log(f"僅上傳資產 {sorted(keep)} → rows={len(feat)}")

    # 分組為「資產×年份」
    feat["year"] = feat["ts_utc"].dt.year
    groups = list(feat.groupby(["asset","year"], sort=True))
    total = len(groups)
    done = 0

    log(f"開始上傳，共 {total} 組（資產×年）…")
    for (a,y), g in groups:
        g = g.drop(columns=["year"])
        prefix = f" [{a} {y}]"
        copy_upsert_chunks(g, table="public.features_1d", chunk_rows=100_000, prefix=prefix)
        done += 1
        pct = int(done*100/total)
        log(f"進度 {done}/{total} ({pct}%) 完成 {a}-{y} rows={len(g)}")

    log(f"全部完成。耗時 {time.perf_counter()-t0:.1f}s")

if __name__ == "__main__":
    main()
