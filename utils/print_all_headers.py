import os
import glob
import json
import pandas as pd

def csv_cols(path: str):
    try:
        return pd.read_csv(path, nrows=0).columns.tolist()
    except Exception as e:
        return [f"error: {e}"]

def parquet_cols(path: str):
    try:
        import pyarrow.parquet as pq
        return pq.ParquetFile(path).schema.names
    except Exception:
        try:
            return list(pd.read_parquet(path).columns)
        except Exception as e:
            return [f"error: {e}"]

def sample_first(pattern: str):
    files = glob.glob(pattern, recursive=True)
    return files[0] if files else None, len(files)

def build_summary():
    out = {}

    # 1) Eventos (CSV)
    ev_csv = "data/datos_eventos_enero_a_julio_2025.csv"
    out["eventos_csv"] = {
        "path": ev_csv,
        "exists": os.path.exists(ev_csv),
        "columns": csv_cols(ev_csv) if os.path.exists(ev_csv) else [],
    }

    # 2) Eventos (Parquet particionado)
    ev_sample, ev_count = sample_first("src/feature_step/datasets/eventos/**/part-*.parquet")
    out["eventos_parquet"] = {
        "count_files": ev_count,
        "sample": ev_sample,
        "columns": parquet_cols(ev_sample) if ev_sample else [],
    }

    # 3) Curva horaria (Parquet particionado)
    cv_sample, cv_count = sample_first("src/feature_step/datasets/curva_horaria/**/part-*.parquet")
    out["curva_horaria_parquet"] = {
        "count_files": cv_count,
        "sample": cv_sample,
        "columns": parquet_cols(cv_sample) if cv_sample else [],
    }

    # 4) Grid contadores (Parquet)
    grid_sample, grid_count = sample_first("src/feature_step/datasets/grid_contadores/*.parquet")
    out["grid_contadores"] = {
        "count_files": grid_count,
        "sample": grid_sample,
        "columns": parquet_cols(grid_sample) if grid_sample else [],
    }

    # 5) Expedientes (CSV)
    exp_csv = "data/Expedientes.csv"
    out["expedientes_csv"] = {
        "path": exp_csv,
        "exists": os.path.exists(exp_csv),
        "columns": csv_cols(exp_csv) if os.path.exists(exp_csv) else [],
    }

    # 6) Grupo eventos (CSV maestro)
    grp_csv = "data/grupo_eventos_codigos.csv"
    out["grupo_eventos_codigos_csv"] = {
        "path": grp_csv,
        "exists": os.path.exists(grp_csv),
        "columns": csv_cols(grp_csv) if os.path.exists(grp_csv) else [],
    }

    # 7) Históricos de consumos horarios (CSV)
    hist_sample, hist_count = sample_first("data/historicos de consumos horarios/*.csv")
    out["historicos_consumos_csv"] = {
        "count_files": hist_count,
        "sample": hist_sample,
        "columns": csv_cols(hist_sample) if hist_sample else [],
    }

    # 8) Bucketizada (CSV)
    buck_files = glob.glob("data/*_bucketized.csv")
    buck_sample = max(buck_files, key=os.path.getmtime) if buck_files else None
    out["bucketized_csv"] = {
        "path": buck_sample,
        "exists": bool(buck_sample),
        "columns": csv_cols(buck_sample) if buck_sample else [],
    }

    # 9) Consumos estáticos (Parquet)
    cons_parquet = "features/datos_contador_clean.parquet"
    out["datos_contador_clean_parquet"] = {
        "path": cons_parquet,
        "exists": os.path.exists(cons_parquet),
        "columns": parquet_cols(cons_parquet) if os.path.exists(cons_parquet) else [],
    }

    # 10) Snapshots de features/labels (Parquet)
    for name in ["dataset_cups_snapshot_2024-11-05.parquet",
                 "features_cups_snapshot_2024-11-05.parquet",
                 "labels_cups_snapshot_2024-11-05.parquet"]:
        p = os.path.join("features", name)
        out[f"features/{name}"] = {
            "path": p,
            "exists": os.path.exists(p),
            "columns": parquet_cols(p) if os.path.exists(p) else [],
        }

    # 11) Denuncias (Parquet)
    den_sample, den_count = sample_first("src/feature_step/datasets/denuncias/*.parquet")
    out["denuncias_parquet"] = {
        "count_files": den_count,
        "sample": den_sample,
        "columns": parquet_cols(den_sample) if den_sample else [],
    }

    # 12) Expedientes (Parquet particionado)
    exp_parq_sample, exp_parq_count = sample_first("src/feature_step/datasets/expedientes/*.parquet")
    out["expedientes_parquet"] = {
        "count_files": exp_parq_count,
        "sample": exp_parq_sample,
        "columns": parquet_cols(exp_parq_sample) if exp_parq_sample else [],
    }

    # 13) Grupo eventos (Parquet maestro)
    grp_parq_sample, grp_parq_count = sample_first("src/feature_step/datasets/grupo_eventos_codigos/*.parquet")
    out["grupo_eventos_codigos_parquet"] = {
        "count_files": grp_parq_count,
        "sample": grp_parq_sample,
        "columns": parquet_cols(grp_parq_sample) if grp_parq_sample else [],
    }

    return out

if __name__ == "__main__":
    summary = build_summary()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
