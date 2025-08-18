import os
import re
import json
import glob

def get_schema_cols(path: str):
    # Try pyarrow schema first (fast, no full read). Fallback to pandas.
    try:
        import pyarrow.parquet as pq  # type: ignore
        return pq.ParquetFile(path).schema.names
    except Exception:
        try:
            import pandas as pd  # type: ignore
            return list(pd.read_parquet(path).columns)
        except Exception as e2:
            return [f"<schema_error: {e2}>"]

def derive_date_from_path(path: str):
    m = re.search(r"partition_0=(\d{4}).*partition_1=(\d{1,2}).*partition_2=(\d{1,2})", path.replace("\\", "/"))
    if not m:
        return None
    try:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mth:02d}-{d:02d}"
    except Exception:
        return None

def summarize_partition(pattern: str, limit: int = 3):
    files = glob.glob(pattern, recursive=True)
    dates = []
    for f in files:
        d = derive_date_from_path(f)
        if d:
            dates.append(d)
    dates_sorted = sorted(dates)
    # Collect sample schemas
    samples = []
    for f in files[:limit]:
        samples.append({
            "file": f,
            "date": derive_date_from_path(f),
            "cols": get_schema_cols(f)
        })
    return {
        "pattern": pattern,
        "count_files": len(files),
        "min_date": dates_sorted[0] if dates_sorted else None,
        "max_date": dates_sorted[-1] if dates_sorted else None,
        "samples": samples
    }

def main():
    out = {}
    out["eventos"] = summarize_partition("src/feature_step/datasets/eventos/**/part-*.parquet", limit=3)
    out["curva_horaria"] = summarize_partition("src/feature_step/datasets/curva_horaria/**/part-*.parquet", limit=3)
    # Grid contadores: list first file columns
    grid_files = glob.glob("src/feature_step/datasets/grid_contadores/*.parquet")
    grid_info = {"count_files": len(grid_files), "sample": None}
    if grid_files:
        gf = grid_files[0]
        grid_info["sample"] = {"file": gf, "cols": get_schema_cols(gf)}
    out["grid_contadores"] = grid_info

    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
