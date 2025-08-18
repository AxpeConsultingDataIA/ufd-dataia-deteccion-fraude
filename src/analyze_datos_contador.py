import argparse
import os
import re
from glob import glob
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_classif

RANDOM_SEED = 42

NUM_COL_CANDIDATES = {
    "potencia_contrato",
    "potencia_maxima_contrato",
    "tension_suministro",
    "coordenada_x",
    "coordenada_y",
}

DATE_COL_CANDIDATES = {
    "fecha_instalacion_aparato",
    "fecha_alta_contrato",
    "fecha_baja_contrato",
}

ID_COL_CANDS = ["cups_sgc", "cups", "CUPS", "nis_rad", "nis", "NIS"]

def _find_latest_labels(features_dir: str = "features") -> str:
    files = sorted(glob(os.path.join(features_dir, "labels_cups_snapshot_*.parquet")))
    if not files:
        raise FileNotFoundError("No se encontró labels_cups_snapshot_*.parquet en 'features/'. Ejecuta pipeline_build_snapshot.")
    return files[-1]

def _guess_id_col(cols: List[str]) -> str | None:
    for c in ID_COL_CANDS:
        if c in cols:
            return c
    for c in cols:
        lc = c.lower()
        if "cup" in lc or "nis" in lc:
            return c
    return None

def _is_numeric_series(s: pd.Series) -> bool:
    try:
        pd.to_numeric(s, errors="raise")
        return True
    except Exception:
        return False

def _coerce_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    df = df.copy()
    cols = list(df.columns)
    # ID normalization
    id_col = _guess_id_col(cols)
    if id_col and id_col != "cups_sgc":
        df = df.rename(columns={id_col: "cups_sgc"})
    # dates
    for c in df.columns:
        if c in DATE_COL_CANDIDATES or "fecha" in c.lower() or "date" in c.lower():
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=True)
            except Exception:
                pass
    # numerics
    numeric_cols: List[str] = []
    for c in df.columns:
        if c == "cups_sgc":
            continue
        if c in NUM_COL_CANDIDATES:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            numeric_cols.append(c)
            continue
        # fallback quick sniff
        sample = df[c].dropna().astype(str).head(50)
        if len(sample) > 0 and all(re.fullmatch(r"-?\d+(\.\d+)?", x.replace(",", ".")) for x in sample):
            df[c] = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="coerce")
            numeric_cols.append(c)
    # categorical = object/string after coercions
    cat_cols = [c for c in df.columns if c not in ["cups_sgc"] + numeric_cols and not np.issubdtype(df[c].dtype, np.datetime64)]
    return df, numeric_cols, cat_cols

def _profile_columns(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for c in df.columns:
        non_null_pct = int(df[c].notna().mean() * 100)
        nunique = int(df[c].nunique(dropna=True))
        dtype = str(df[c].dtype)
        sample_vals = [str(v) for v in pd.Series(df[c].dropna().astype(str).unique()).head(5).tolist()]
        rows.append({"col": c, "dtype": dtype, "non_null_%": non_null_pct, "nunique": nunique, "sample": sample_vals})
    return pd.DataFrame(rows).sort_values("col")

def _mutual_info_per_column(cnt: pd.DataFrame, labels: pd.DataFrame, numeric_cols: List[str], cat_cols: List[str]) -> pd.DataFrame:
    df = cnt.merge(labels[["cups_sgc", "label_fraude"]], on="cups_sgc", how="inner").dropna(subset=["label_fraude"])
    if df.empty:
        return pd.DataFrame(columns=["col", "mi", "coverage_%"]).astype({"mi": float, "coverage_%": int})
    y = df["label_fraude"].astype(int).to_numpy()
    rng = np.random.RandomState(RANDOM_SEED)
    results = []

    # numeric
    for c in numeric_cols:
        x = pd.to_numeric(df[c], errors="coerce")
        cov = int(x.notna().mean() * 100)
        if cov == 0:
            results.append({"col": c, "mi": 0.0, "coverage_%": 0})
            continue
        xx = x.fillna(x.median())
        mi = float(mutual_info_classif(xx.to_numpy().reshape(-1, 1), y, discrete_features=False, random_state=rng))
        results.append({"col": c, "mi": mi, "coverage_%": cov})

    # categorical
    for c in cat_cols:
        s = df[c].astype("category")
        cov = int(s.notna().mean() * 100)
        if cov == 0:
            results.append({"col": c, "mi": 0.0, "coverage_%": 0})
            continue
        codes = s.cat.add_categories(["__NA__"]).fillna("__NA__").cat.codes.to_numpy()
        mi = float(mutual_info_classif(codes.reshape(-1, 1), y, discrete_features=True, random_state=rng))
        results.append({"col": c, "mi": mi, "coverage_%": cov})

    out = pd.DataFrame(results).sort_values("mi", ascending=False)
    return out

def _top_category_stats(cnt: pd.DataFrame, labels: pd.DataFrame, col: str, top_k: int = 5) -> List[str]:
    df = cnt.merge(labels[["cups_sgc", "label_fraude"]], on="cups_sgc", how="inner").dropna(subset=["label_fraude"])
    if df.empty or col not in df.columns:
        return []
    s = df[col].astype(str)
    vc = s.value_counts(dropna=True)
    cats = vc.head(top_k).index.tolist()
    out = []
    for cat in cats:
        frac = (df.loc[s == cat, "label_fraude"].mean() if (s == cat).any() else np.nan)
        out.append(f"{cat} -> tasa_fraude={0.0 if pd.isna(frac) else float(frac):.3f} (n={int((s == cat).sum())})")
    return out

def build_report(csv_path: str, labels_path: str | None = None, out_path: str = "informes/informe_datos_contador.md") -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    labels_path = labels_path or _find_latest_labels()

    cnt_raw = pd.read_csv(csv_path, dtype=str)
    id_col = _guess_id_col(list(cnt_raw.columns))
    if id_col is None:
        raise ValueError("No se encontró columna identificadora (cups_sgc / nis_rad) en el CSV.")
    cnt, num_cols, cat_cols = _coerce_types(cnt_raw)

    labels = pd.read_parquet(labels_path)
    if "cups_sgc" not in labels.columns or "label_fraude" not in labels.columns:
        raise ValueError(f"El fichero de labels no tiene columnas esperadas: {labels_path}")

    # Perfilado básico
    prof = _profile_columns(cnt)

    # Señal predictiva (MI) por columna
    mi_df = _mutual_info_per_column(cnt, labels, num_cols, cat_cols)
    top_mi = mi_df.head(15)

    # Recomendación simple
    rec_cols = top_mi.loc[top_mi["coverage_%"] >= 50, "col"].tolist()  # cobertura mínima 50%
    rec_text = (
        "Integrar columnas con MI alta y cobertura >=50%: "
        + (", ".join(rec_cols) if rec_cols else "No hay columnas destacadas con cobertura suficiente.")
    )

    # Preparar Markdown
    lines: List[str] = []
    lines.append("# Análisis de datos_contador.csv")
    lines.append("")
    lines.append(f"- Fichero analizado: {csv_path}")
    lines.append(f"- Labels usados: {labels_path}")
    lines.append(f"- Filas CSV: {len(cnt)}")
    lines.append("")
    lines.append("## Perfil de columnas (muestra)")
    lines.append("")
    head_prof = prof.sort_values("non_null_%", ascending=False).head(20)
    lines.append("| Columna | dtype | No nulo % | únicos | ejemplo |")
    lines.append("|---|---|---:|---:|---|")
    for _, r in head_prof.iterrows():
        sample = ", ".join(r["sample"][:3]) if isinstance(r["sample"], list) else ""
        lines.append(f"| {r['col']} | {r['dtype']} | {r['non_null_%']} | {r['nunique']} | {sample} |")

    lines.append("")
    lines.append("## Señal predictiva (Mutual Information por columna)")
    lines.append("")
    lines.append("| Columna | MI | Cobertura % | Top categorías con mayor tasa de fraude (si aplica) |")
    lines.append("|---|---:|---:|---|")
    for _, r in top_mi.iterrows():
        extras = ""
        if r["col"] in cat_cols:
            extras = "; ".join(_top_category_stats(cnt, labels, r["col"], top_k=3))
        lines.append(f"| {r['col']} | {r['mi']:.4f} | {int(r['coverage_%'])} | {extras} |")

    lines.append("")
    lines.append("## Recomendación")
    lines.append("")
    lines.append(f"- {rec_text}")
    lines.append("")
    if rec_cols:
        lines.append("- Candidatas claras (por semántica):")
        for c in rec_cols:
            if c in {"potencia_contrato", "potencia_maxima_contrato", "tension_suministro"}:
                lines.append(f"  - {c}: magnitudes eléctricas relevantes (potencia/tensión).")
            elif c in {"municipio", "provincia", "comunidad_autonoma", "codigo_postal"}:
                lines.append(f"  - {c}: contexto geográfico (riesgo territorial).")
            elif "actividad" in c:
                lines.append(f"  - {c}: actividad económica (patrones de uso).")
            elif "tarifa" in c:
                lines.append(f"  - {c}: tarifa/ATR (curva de carga).")
            elif "fecha_" in c:
                lines.append(f"  - {c}: fechas (antigüedad de equipo/contrato).")
            else:
                lines.append(f"  - {c}: candidata por MI y cobertura.")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Exportar versión "limpia" con tipos y solo columnas recomendadas (si existieran)
    clean_out = os.path.join("features", "datos_contador_clean.parquet")
    os.makedirs("features", exist_ok=True)
    use_cols = ["cups_sgc"] + (rec_cols if rec_cols else [c for c in list(cnt.columns) if c != "cups_sgc"])
    cnt[use_cols].to_parquet(clean_out, index=False)

    print(f"Informe generado: {out_path}")
    print(f"Perfil columnas (top 10 por no nulos):")
    print(head_prof.head(10).to_string(index=False))
    print(f"Top columnas por MI:")
    print(top_mi[["col", "mi", "coverage_%"]].to_string(index=False))
    print(f"Recomendación: {rec_text}")
    print(f"Exportado features limpias: {clean_out}")
    return out_path

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analiza el CSV datos_contador.csv y estima su valor para el modelo de fraude.")
    p.add_argument("--csv", type=str, default="data/datos_contador.csv", help="Ruta del CSV de datos de contador.")
    p.add_argument("--labels", type=str, default=None, help="Ruta parquet de labels (por defecto usa el último en features/).")
    p.add_argument("--out", type=str, default="informes/informe_datos_contador.md", help="Ruta de salida del informe Markdown.")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        build_report(csv_path=args.csv, labels_path=args.labels, out_path=args.out)
    except Exception as e:
        print(f"[ERROR] {e}")
        raise

if __name__ == "__main__":
    main()
