import os
import re
import json
from glob import glob

import pandas as pd

BASE = "src/feature_step/datasets"

def _first_parquet(pattern: str) -> str | None:
    files = sorted(glob(pattern, recursive=True))
    return files[0] if files else None

def _read_head(path: str, n: int = 5) -> dict:
    try:
        df = pd.read_parquet(path)
        head = df.head(n).copy()
        # Asegurar serialización JSON (fechas y tipos numpy)
        for c in head.columns:
            if pd.api.types.is_datetime64_any_dtype(head[c]):
                head[c] = head[c].astype(str)
            else:
                head[c] = head[c].apply(lambda x: x.item() if hasattr(x, "item") else x)
        out = {
            "path": path,
            "rows": int(len(df)),
            "cols": list(df.columns),
            "dtypes": {c: str(t) for c, t in df.dtypes.items()},
            "head": head.to_dict(orient="records"),
        }
        return out
    except Exception as e:
        return {"path": path, "error": str(e)}

def _find_any(dir_glob: str) -> list[str]:
    return sorted(glob(dir_glob, recursive=True))

def main():
    report = {}

    # Eventos (log)
    ev_parquet = _first_parquet(os.path.join(BASE, "eventos", "**", "*.parquet"))
    if ev_parquet:
        report["eventos"] = _read_head(ev_parquet)
    else:
        report["eventos"] = {"error": "No parquet found"}

    # Maestro: grupo_eventos_codigos
    maestro_parquet = _first_parquet(os.path.join(BASE, "grupo_eventos_codigos", "*.parquet"))
    if maestro_parquet:
        report["grupo_eventos_codigos"] = _read_head(maestro_parquet)
    else:
        report["grupo_eventos_codigos"] = {"error": "No parquet found"}

    # Curva horaria
    curva_parquet = _first_parquet(os.path.join(BASE, "curva_horaria", "**", "*.parquet"))
    if curva_parquet:
        report["curva_horaria"] = _read_head(curva_parquet)
    else:
        report["curva_horaria"] = {"error": "No parquet found"}

    # Grid contadores (posible mapping cnt_id -> cups/nis)
    grid_parquet = _first_parquet(os.path.join(BASE, "grid_contadores", "*.parquet"))
    if grid_parquet:
        report["grid_contadores"] = _read_head(grid_parquet)
    else:
        report["grid_contadores"] = {"error": "No parquet found"}

    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
