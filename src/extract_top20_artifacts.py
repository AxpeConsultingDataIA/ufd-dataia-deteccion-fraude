import argparse
import os
import glob
from typing import List, Optional, Tuple, Set

import pandas as pd
import numpy as np

def find_latest(path_glob: str) -> Optional[str]:
    files = sorted(glob.glob(path_glob))
    return files[-1] if files else None

def load_scoring(scoring_path: Optional[str]) -> pd.DataFrame:
    if scoring_path is None:
        scoring_path = find_latest("scoring/scoring_full_*.parquet")
        if scoring_path is None:
            raise FileNotFoundError("No se encontró scoring_full_*.parquet en 'scoring/'.")
    df = pd.read_parquet(scoring_path)
    need = ["cups_sgc", "prob_fraude"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"Falta columna requerida en scoring: {c}")
    df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    return df

def load_grid() -> pd.DataFrame:
    path = find_latest("src/feature_step/datasets/grid_contadores/*.parquet")
    if path is None:
        raise FileNotFoundError("No se encontró parquet en src/feature_step/datasets/grid_contadores/")
    df = pd.read_parquet(path)
    # Normalizar
    for c in ["cups_sgc", "cnc_s05", "cnt_sgc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()
    return df

def load_expedientes() -> pd.DataFrame:
    path = find_latest("src/feature_step/datasets/expedientes/*.parquet")
    if path is None:
        raise FileNotFoundError("No se encontró parquet de expedientes en src/feature_step/datasets/expedientes/")
    df = pd.read_parquet(path)
    # Normalizar posibles columnas de id
    if "cups_sgc" not in df.columns:
        for cand in ["cups", "CUPS"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "cups_sgc"})
                break
    if "cups_sgc" in df.columns:
        df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    return df

def load_datos_contador(path: str = "features/datos_contador_clean.parquet") -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró {path}")
    df = pd.read_parquet(path)
    if "cups_sgc" in df.columns:
        df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    return df

def filter_expedientes_csv(cups_set: Set[str], csv_path: str = "data/Expedientes.csv") -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    import pandas as pd
    chunks = []
    id_col = None
    # detectar id en cabecera
    sample = pd.read_csv(csv_path, nrows=1)
    for cand in ["cups_sgc", "cups", "CUPS", "nis", "NIS", "nis_rad"]:
        if cand in sample.columns:
            id_col = cand
            break
    if id_col is None:
        # fallback a primera col que contenga 'cup'/'nis'
        for c in sample.columns:
            lc = c.lower()
            if "cup" in lc or "nis" in lc:
                id_col = c
                break
    if id_col is None:
        return pd.DataFrame()
    for chunk in pd.read_csv(csv_path, chunksize=200_000, low_memory=False):
        if id_col not in chunk.columns:
            continue
        ids = chunk[id_col].astype(str).str.upper().str.strip()
        sub = chunk.loc[ids.isin(cups_set)].copy()
        if not sub.empty:
            if id_col != "cups_sgc":
                sub = sub.rename(columns={id_col: "cups_sgc"})
            sub["cups_sgc"] = sub["cups_sgc"].astype(str).str.upper().str.strip()
            chunks.append(sub)
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

def load_consumos_historicos_for_cups(cups_set: Set[str], base_dir: str = "data/historicos de consumos horarios") -> pd.DataFrame:
    # Lectura streaming con csv.DictReader para evitar OOM y acelerar filtrando por CUPS
    import glob, csv
    files = sorted(glob.glob(os.path.join(base_dir, "consumos-*.csv")))
    if not files or not cups_set:
        return pd.DataFrame(columns=["cups_sgc", "fecha_hora_consumo", "energia_consumida"])
    rows: List[Tuple[str, str, float]] = []
    for p in files:
        opened = False
        for enc in ("utf-8", "latin-1"):
            try:
                with open(p, "r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    # normalizar claves: usar originales del fichero
                    for r in reader:
                        cs = (r.get("cups_sgc") or r.get("CUPS") or r.get("cups") or "").strip().upper()
                        if cs and cs in cups_set:
                            dt = (r.get("fecha_hora_consumo") or r.get("fecha") or r.get("fh") or "").strip()
                            val = r.get("energia_consumida") or r.get("consumo") or r.get("ai") or r.get("AE") or r.get("ae") or ""
                            try:
                                e = float(val)
                            except Exception:
                                e = float("nan")
                            rows.append((cs, dt, e))
                opened = True
                break
            except Exception:
                continue
        if not opened:
            continue
    if not rows:
        return pd.DataFrame(columns=["cups_sgc", "fecha_hora_consumo", "energia_consumida"])
    df = pd.DataFrame(rows, columns=["cups_sgc", "fecha_hora_consumo", "energia_consumida"])
    df["fecha_hora_consumo"] = pd.to_datetime(df["fecha_hora_consumo"], errors="coerce")
    df["energia_consumida"] = pd.to_numeric(df["energia_consumida"], errors="coerce").fillna(0.0)
    df = df.sort_values(["cups_sgc", "fecha_hora_consumo"])
    return df

def iter_curva_paths() -> List[str]:
    # Recolecta todos los parquet de curva horaria
    pats = [
        "src/feature_step/datasets/curva_horaria/partition_0=*/partition_1=*/partition_2=*/part-*.parquet",
        "src/feature_step/datasets/curva_horaria/**/part-*.parquet",
    ]
    seen: Set[str] = set()
    out: List[str] = []
    for pat in pats:
        for p in glob.glob(pat, recursive=True):
            if p not in seen:
                seen.add(p)
                out.append(p)
    out.sort()
    return out

def load_curvas_for_devices(cnc_set: Set[str], cnt_set: Set[str], as_of_hint: Optional[str] = None) -> pd.DataFrame:
    cols_try = ["data_date", "fh", "cnc_id", "cnt_id", "ai", "ae", "r1", "r2", "r3", "r4", "season", "bc"]
    frames: List[pd.DataFrame] = []
    paths = iter_curva_paths()
    # Si hay asOf, priorizar partición del día
    if as_of_hint and len(as_of_hint) >= 10:
        try:
            y, m, d = as_of_hint[:4], as_of_hint[5:7], as_of_hint[8:10]
            preferred = [p for p in paths if f"partition_0={int(y)}" in p and f"partition_1={int(m)}" in p and f"partition_2={int(d)}" in p]
            others = [p for p in paths if p not in preferred]
            paths = preferred + others
        except Exception:
            pass

    for p in paths:
        try:
            df = pd.read_parquet(p, columns=[c for c in cols_try if c in pd.read_parquet(p, nrows=1).columns])
        except Exception:
            try:
                df = pd.read_parquet(p)
            except Exception:
                continue
        # Normalizar ids
        for c in ["cnc_id", "cnt_id"]:
            if c in df.columns:
                df[c] = df[c].astype(str).str.upper().str.strip()
        # Filtrar sólo filas relacionadas
        cond = pd.Series([False] * len(df))
        if "cnc_id" in df.columns and cnc_set:
            cond = cond | df["cnc_id"].isin(cnc_set)
        if "cnt_id" in df.columns and cnt_set:
            cond = cond | df["cnt_id"].isin(cnt_set)
        df = df[cond]
        if not df.empty:
            frames.append(df)
        if frames and len(frames[-1]) > 0 and as_of_hint:
            # Ya capturamos curvas del día, suficiente para top20
            # Pero seguimos por si hay varias particiones del mismo día
            pass
    if frames:
        cur = pd.concat(frames, ignore_index=True)
        # Orden por fecha si existe
        if "data_date" in cur.columns:
            cur["data_date"] = pd.to_datetime(cur["data_date"], errors="coerce")
            cur = cur.sort_values(["data_date", "fh"] if "fh" in cur.columns else ["data_date"])
        return cur
    return pd.DataFrame(columns=cols_try)

def attach_cups_to_curvas(curvas: pd.DataFrame, grid_top: pd.DataFrame) -> pd.DataFrame:
    if curvas.empty:
        return curvas
    # Map por cnc
    out = curvas.copy()
    added = False
    if "cnc_id" in curvas.columns and "cnc_s05" in grid_top.columns:
        m1 = curvas.merge(
            grid_top[["cups_sgc", "cnc_s05"]].rename(columns={"cnc_s05": "cnc_id"}),
            on="cnc_id",
            how="left",
        )
        out = m1
        added = True
    if "cnt_id" in curvas.columns and "cnt_sgc" in grid_top.columns:
        m2 = curvas.merge(
            grid_top[["cups_sgc", "cnt_sgc"]].rename(columns={"cnt_sgc": "cnt_id"}),
            on="cnt_id",
            how="left",
        )
        if added:
            # Coalesce cups_sgc
            out["cups_sgc"] = out["cups_sgc"].fillna(m2.get("cups_sgc"))
        else:
            out = m2
            added = True
    # Mantener sólo curvas con cups identificado si es posible
    if "cups_sgc" in out.columns:
        return out
    else:
        return curvas

def build_outputs(scoring_path: Optional[str], topn: int, out_dir: str) -> Tuple[str, str, str, str]:
    os.makedirs(out_dir, exist_ok=True)

    sc = load_scoring(scoring_path)
    as_of = str(sc["asOf"].iloc[0]) if "asOf" in sc.columns and not sc.empty else "unknown"
    sc = sc.sort_values("prob_fraude", ascending=False).drop_duplicates("cups_sgc")
    top = sc.head(topn).copy()
    cups = top["cups_sgc"].tolist()

    # Guardar lista top
    top_list_path = os.path.join(out_dir, f"top{topn}_cups_{as_of}.csv")
    top[["cups_sgc", "prob_fraude"]].to_csv(top_list_path, index=False, encoding="utf-8")

    # Consumos horarios reales desde CSV históricos (ignorando asOf)
    cons = load_consumos_historicos_for_cups(set(cups))
    consumos_path = os.path.join(out_dir, f"consumos_horarios_top{topn}.csv")
    if not cons.empty:
        # Asegurar tipo datetime antes de usar .dt
        if not np.issubdtype(cons["fecha_hora_consumo"].dtype, np.datetime64):
            cons["fecha_hora_consumo"] = pd.to_datetime(cons["fecha_hora_consumo"], errors="coerce")
        cons.to_csv(consumos_path, index=False, encoding="utf-8")
        # Resumen por día y totales por CUPS
        cons["fecha"] = cons["fecha_hora_consumo"].dt.date
        resumen = (
            cons.groupby(["cups_sgc", "fecha"], as_index=False)["energia_consumida"]
            .sum()
            .rename(columns={"energia_consumida": "energia_dia"})
        )
        resumen_total = (
            cons.groupby("cups_sgc", as_index=False)["energia_consumida"]
            .sum()
            .rename(columns={"energia_consumida": "energia_total"})
        )
        resumen_path = os.path.join(out_dir, f"consumos_resumen_top{topn}.csv")
        resumen.to_csv(resumen_path, index=False, encoding="utf-8")
        resumen_total_path = os.path.join(out_dir, f"consumos_totales_top{topn}.csv")
        resumen_total.to_csv(resumen_total_path, index=False, encoding="utf-8")
        print(f" - Consumos horarios: {consumos_path}")
        print(f" - Resumen diario: {resumen_path}")
        print(f" - Totales: {resumen_total_path}")
    else:
        # crear fichero vacío
        pd.DataFrame(columns=["cups_sgc", "fecha_hora_consumo", "energia_consumida"]).to_csv(consumos_path, index=False, encoding="utf-8")
        print(f" - Consumos horarios: {consumos_path} (vacío)")

    # Datos de contador (preferir features/datos_contador_clean.parquet, fallback a grid)
    try:
        datos_cnt = load_datos_contador()
        datos_cnt_top = datos_cnt[datos_cnt["cups_sgc"].isin(cups)].copy()
        grid_path = os.path.join(out_dir, f"datos_contador_top{topn}_{as_of}.csv")
        datos_cnt_top.to_csv(grid_path, index=False, encoding="utf-8")
    except Exception:
        grid = load_grid()
        grid_top = grid[grid["cups_sgc"].isin(cups)].copy()
        grid_path = os.path.join(out_dir, f"grid_contadores_top{topn}_{as_of}.csv")
        grid_top.to_csv(grid_path, index=False, encoding="utf-8")

    # Curvas horarias
    # 1) Intento por dispositivos (cnc_id/cnt_id) si hay grid con esos campos
    curvas = pd.DataFrame()
    curvas_path = os.path.join(out_dir, f"curvas_horarias_top{topn}_{as_of}.parquet")
    try:
        # si hemos generado grid_top por fallback, puede no existir aquí; recalcular conjunto de dispositivos
        try:
            grid_ref = datos_cnt_top  # del bloque anterior
        except Exception:
            grid_ref = grid_top if 'grid_top' in locals() else pd.DataFrame()
        cnc_set = set(grid_ref["cnc_s05"].dropna().astype(str).str.upper().str.strip()) if "cnc_s05" in grid_ref.columns else set()
        cnt_set = set(grid_ref["cnt_sgc"].dropna().astype(str).str.upper().str.strip()) if "cnt_sgc" in grid_ref.columns else set()
        if cnc_set or cnt_set:
            curvas = load_curvas_for_devices(cnc_set, cnt_set, as_of_hint=as_of)
            curvas = attach_cups_to_curvas(curvas, grid_ref if not grid_ref.empty else pd.DataFrame())
    except Exception:
        curvas = pd.DataFrame()

    if curvas is None or curvas.empty:
        # 2) Fallback: exportar resumenes de curva desde snapshot de features
        snap_path = f"features/dataset_cups_snapshot_{as_of}.parquet"
        if os.path.exists(snap_path):
            d2 = pd.read_parquet(snap_path)
            if "cups_sgc" in d2.columns:
                d2["cups_sgc"] = d2["cups_sgc"].astype(str).str.upper().str.strip()
                resumen = d2[d2["cups_sgc"].isin(cups)].copy()
                curva_cols = [c for c in resumen.columns if isinstance(c, str) and c.startswith("curva_")]
                keep_cols = ["cups_sgc", "asOf"] + curva_cols
                resumen = resumen[keep_cols]
                resumen.to_parquet(curvas_path, index=False)
            else:
                # crear vacío
                pd.DataFrame(columns=["cups_sgc", "asOf"]).to_parquet(curvas_path, index=False)
        else:
            pd.DataFrame(columns=["cups_sgc", "asOf"]).to_parquet(curvas_path, index=False)
    else:
        curvas.to_parquet(curvas_path, index=False)

    # Expedientes filtrados (preferir CSV crudo por cobertura)
    exped_csv_path = "data/Expedientes.csv"
    exped_path = os.path.join(out_dir, f"expedientes_top{topn}_{as_of}.csv")
    cups_set = set(cups)
    exped_top = pd.DataFrame()
    if os.path.exists(exped_csv_path):
        try:
            exped_top = filter_expedientes_csv(cups_set, exped_csv_path)
        except Exception:
            exped_top = pd.DataFrame()
    if exped_top.empty:
        try:
            exped = load_expedientes()
            if "cups_sgc" in exped.columns:
                exped_top = exped[exped["cups_sgc"].isin(cups)].copy()
            else:
                exped_top = pd.DataFrame(columns=exped.columns)
        except Exception:
            exped_top = pd.DataFrame()
    exped_top.to_csv(exped_path, index=False, encoding="utf-8")

    return top_list_path, curvas_path, exped_path, grid_path

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extrae artefactos (curvas, expedientes, datos contador) para el top-N de fraude.")
    p.add_argument("--scoring", type=str, default=None, help="Ruta a scoring_full_*.parquet. Por defecto usa el último.")
    p.add_argument("--topn", type=int, default=20, help="Número de CUPS a extraer.")
    p.add_argument("--out-dir", type=str, default="informes", help="Directorio base de salida.")
    return p.parse_args()

def main():
    args = parse_args()
    out_base = os.path.join(args.out_dir, "top_extracts")
    os.makedirs(out_base, exist_ok=True)
    try:
        top_csv, curvas_pq, exped_csv, grid_csv = build_outputs(args.scoring, args.topn, out_base)
        print("Generado:")
        print(" - Lista top:", top_csv)
        print(" - Curvas horarias:", curvas_pq)
        print(" - Expedientes:", exped_csv)
        print(" - Datos contador:", grid_csv)
    except Exception as e:
        print(f"[ERROR] {e}")
        raise

if __name__ == "__main__":
    main()
