import argparse
import os
from glob import glob
from typing import List, Optional, Tuple, Set

import numpy as np
import pandas as pd
import datetime as dt


def _find_latest_topk(scoring_dir: str = "scoring") -> str:
    files = sorted(glob(os.path.join(scoring_dir, "scoring_top*.parquet")))
    if not files:
        raise FileNotFoundError("No se encontró ningún fichero scoring_top*.parquet en 'scoring/'. Ejecuta el script de scoring primero.")
    return files[-1]


def _find_bucketized_csv(data_dir: str = "data") -> Optional[str]:
    candidates = sorted(glob(os.path.join(data_dir, "*_bucketized.csv")))
    return candidates[-1] if candidates else None


def _read_bucketized_features(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    # Detectar id
    for cand in ["cups_sgc", "CUPS", "cups", "nis_rad", "NIS", "nis"]:
        if cand in df.columns:
            if cand != "cups_sgc":
                df = df.rename(columns={cand: "cups_sgc"})
            break
    if "cups_sgc" not in df.columns:
        raise ValueError(f"No se encontró identificador en {path}. Se esperaba 'cups_sgc' o 'nis_rad'.")
    # Usar solo columnas relevantes para explicación rápida
    explain_cols = [c for c in df.columns if c.startswith("eventos_grupo4_total")]
    # Si no hay columnas 'eventos_grupo4_total', usar todas las que empiezan por 'eventos' o 'count'
    if not explain_cols:
        explain_cols = [c for c in df.columns if c.startswith("eventos") or c.startswith("count")]
    # Mantener id + explicables
    keep = ["cups_sgc"] + explain_cols
    df = df[keep].copy()
    df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=["cups_sgc"])
    return df

def _infer_id_column(cols) -> Optional[str]:
    for cand in ["cups_sgc", "nis_rad", "CUPS", "cups", "nis", "NIS"]:
        if cand in cols:
            return cand
    for c in cols:
        lc = c.lower()
        if "cup" in lc or "nis" in lc:
            return c
    return None

def _parse_date_series(s: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(s, dayfirst=True, errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def _collect_recent_cleared_cups(expedientes_csv: str, cups: Set[str], as_of: str, days: int = 30) -> Set[str]:
    if not expedientes_csv or not os.path.exists(expedientes_csv):
        return set()
    cutoff = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(cutoff):
        return set()
    cutoff = cutoff - pd.Timedelta(days=days)
    cleared: Set[str] = set()
    # header to infer columns
    try:
        sample = pd.read_csv(expedientes_csv, nrows=1)
    except Exception:
        return set()
    id_col = _infer_id_column(sample.columns)
    if id_col is None:
        return set()
    date_pref = ["fec_alta", "fecha_alta", "fec_acta", "fecha_acta", "fecha_inicio_anomalia", "fecha_inicio"]
    date_col = next((c for c in date_pref if c in sample.columns), None)
    if date_col is None:
        for c in sample.columns:
            lc = c.lower()
            if "fec" in lc or "fecha" in lc:
                date_col = c
                break
    text_cols = [c for c in sample.columns if any(k in c.lower() for k in ["fraud", "fraude", "irreg", "resultado", "estado", "clasif", "observ", "coment"])]
    try:
        for chunk in pd.read_csv(expedientes_csv, chunksize=200_000, low_memory=False):
            if id_col not in chunk.columns:
                continue
            sub = chunk[chunk[id_col].astype(str).str.upper().str.strip().isin(cups)].copy()
            if sub.empty:
                continue
            if date_col and date_col in sub.columns:
                ds = _parse_date_series(sub[date_col])
                sub = sub.assign(__date=ds)
                sub = sub[(sub["__date"] >= cutoff) & (sub["__date"] <= pd.to_datetime(as_of, errors="coerce"))]
                if sub.empty:
                    continue
            if text_cols:
                try:
                    txt = sub[text_cols].astype(str).agg(" ".join, axis=1).str.upper()
                except Exception:
                    txt = sub[text_cols[0]].astype(str).str.upper()
            else:
                txt = pd.Series([""] * len(sub), index=sub.index)
            mask_clear = txt.str.contains("DESCART", na=False) | txt.str.contains("ARCHIV", na=False) | txt.str.contains("NO FRAUD", na=False) | txt.str.contains("SIN IRREG", na=False)
            mask_conf = txt.str.contains("CONFIRM", na=False) | txt.str.contains("FRAUD", na=False)
            sub_clear = sub[mask_clear & (~mask_conf)]
            if not sub_clear.empty:
                ids = sub_clear[id_col].astype(str).str.upper().str.strip().unique().tolist()
                cleared.update(ids)
    except Exception:
        return cleared
    return cleared


def _mk_reason_row(row: pd.Series, bucketized_row: Optional[pd.Series] = None) -> List[str]:
    reasons: List[str] = []
    # Señales de grafo/vecindario
    if "fraude_hist" in row and pd.notna(row["fraude_hist"]) and int(row["fraude_hist"]) == 1:
        reasons.append("Historial de fraude confirmado en el propio suministro")
    if "tasa_fraude_vec" in row and pd.notna(row["tasa_fraude_vec"]):
        tasa = float(row["tasa_fraude_vec"])
        if tasa >= 0.2:
            reasons.append(f"Alta tasa de fraude en vecindario ({tasa:.0%})")
        elif tasa >= 0.1:
            reasons.append(f"Tasa de fraude vecinal moderada ({tasa:.0%})")
    if "vecinos_fraude" in row and "vecinos_total" in row and pd.notna(row["vecinos_fraude"]) and pd.notna(row["vecinos_total"]):
        vf = int(row["vecinos_fraude"])
        vt = int(row["vecinos_total"])
        if vf >= 1 and vt >= 3:
            reasons.append(f"{vf}/{vt} vecinos con fraude histórico")

    # Señales de eventos/curva del propio scoring (lookback configurable)
    # Eventos agregados (parseo robusto)
    def _safe_int(v) -> int:
        try:
            if pd.isna(v):
                return 0
            s = str(v).strip()
            if s == "" or s.lower() == "none":
                return 0
            # soportar "0.0" o floats
            return int(float(s))
        except Exception:
            try:
                num = pd.to_numeric(v, errors="coerce")
                return int(num) if pd.notna(num) else 0
            except Exception:
                return 0

    evcnt_col = next((c for c in row.index if isinstance(c, str) and c.startswith("event_count_")), None)
    evsus_col = next((c for c in row.index if isinstance(c, str) and c.startswith("event_suspicious_")), None)
    evcnt_val = _safe_int(row[evcnt_col]) if evcnt_col else 0
    evsus_val = _safe_int(row[evsus_col]) if evsus_col else 0
    if evcnt_val > 0:
        reasons.append(f"{evcnt_val} eventos en ventana")
    if evsus_val > 0:
        reasons.append(f"{evsus_val} eventos con patrón sospechoso")

    # Top grupos ET por actividad
    grp_counts = []
    for c in row.index:
        if isinstance(c, str) and c.startswith("event_g") and c.endswith("_"):
            # evitar falsos positivos; manejado abajo
            continue
        if isinstance(c, str) and c.startswith("event_g") and c.endswith("d"):
            try:
                val = int(row[c]) if pd.notna(row[c]) else 0
            except Exception:
                val = 0
            if val > 0:
                grp_counts.append((c, val))
    if grp_counts:
        grp_counts.sort(key=lambda x: -x[1])
        top = [f"{name.replace('event_g','G').replace('_',' ')}={val}" for name, val in grp_counts[:3]]
        reasons.append("Alta actividad por grupos: " + ", ".join(top))

    # Curva horaria
    zpp_col = next((c for c in row.index if isinstance(c, str) and c.startswith("curva_zero_hours_pct_")), None)
    if zpp_col and pd.notna(row[zpp_col]):
        try:
            pct = float(row[zpp_col])
            if pct >= 0.3:
                reasons.append(f"{pct:.0%} de horas con consumo cero")
            elif pct >= 0.15:
                reasons.append(f"{pct:.0%} de horas con consumo cero (moderado)")
        except Exception:
            pass
    ai_sum_col = next((c for c in row.index if isinstance(c, str) and c.startswith("curva_ai_sum_")), None)
    ae_sum_col = next((c for c in row.index if isinstance(c, str) and c.startswith("curva_ae_sum_")), None)
    # Parseo robusto de AI/AE (evitar ValueError con strings vacíos)
    ai_val = pd.to_numeric(row[ai_sum_col], errors="coerce") if ai_sum_col else np.nan
    ae_val = pd.to_numeric(row[ae_sum_col], errors="coerce") if ae_sum_col else np.nan
    if (not pd.isna(ai_val) and float(ai_val) == 0.0) and (not pd.isna(ae_val) and float(ae_val) == 0.0):
        reasons.append("Consumos activos y exportados nulos en la ventana")

    # Contexto de contador/suministro
    ctx_fields = {
        "tipo_punto_suministro": "tipo de punto",
        "tension_suministro": "tensión",
        "identificador_actividad_economica": "actividad",
        "potencia_contrato": "potencia",
        "municipio": "municipio",
        "provincia": "provincia",
        "codigo_postal": "CP",
    }
    ctx = []
    for k, lbl in ctx_fields.items():
        if k in row and pd.notna(row[k]) and str(row[k]).strip():
            ctx.append(f"{lbl}: {row[k]}")
    if ctx:
        reasons.append("Contexto: " + ", ".join(ctx[:3]))

    # Señales de eventos bucketizados (legacy) si existen
    if bucketized_row is not None and not bucketized_row.empty:
        ev_cols = [c for c in bucketized_row.index if c != "cups_sgc"]
        candidates = []
        for c in ev_cols:
            val = str(bucketized_row[c])
            if val and val != "0" and val != "nan" and val.lower() != "none":
                approx = None
                if val.startswith("[") and "," in val:
                    try:
                        approx = float(val.strip("[]").split(",")[0])
                    except Exception:
                        approx = None
                candidates.append((c, val, approx))
        if candidates:
            candidates.sort(key=lambda x: (x[2] is None, -(x[2] or 0.0), x[0]))
            picks = [f"{c}={v}" for c, v, _ in candidates[:2]]
            reasons.append("Bucketizado eventos: " + ", ".join(picks))
    return reasons


def build_report(
    topk_path: Optional[str] = None,
    bucketized_path: Optional[str] = None,
    out_dir: str = "informes",
    topn_detail: int = 20,
    expedientes_csv: Optional[str] = "data/Expedientes.csv",
    filter_recent_clear_days: int = 30,
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    topk_path = topk_path or _find_latest_topk()
    topk = pd.read_parquet(topk_path)

    # Filtrar CUPS con expedientes que descartan anomalía en últimos días
    try:
        if expedientes_csv:
            as_of = str(topk["asOf"].iloc[0]) if "asOf" in topk.columns and not topk.empty else dt.date.today().isoformat()
            cups_set = set(topk["cups_sgc"].astype(str).str.upper().str.strip().unique().tolist()) if "cups_sgc" in topk.columns else set()
            cleared = _collect_recent_cleared_cups(expedientes_csv, cups_set, as_of, days=filter_recent_clear_days)
            if cleared:
                topk = topk[~topk["cups_sgc"].astype(str).str.upper().str.strip().isin(cleared)].reset_index(drop=True)
    except Exception as e:
        print(f"[WARN] filtro expedientes recientes omitido: {e}")

    # Datos básicos
    if "prob_fraude" not in topk.columns:
        raise ValueError("El fichero de scoring no contiene la columna 'prob_fraude'.")

    # Adjuntar bucketizado si existe
    if bucketized_path is None:
        bucketized_path = _find_bucketized_csv()
    bucket = None
    if bucketized_path:
        try:
            bucket = _read_bucketized_features(bucketized_path)
        except Exception:
            bucket = None

    if bucket is not None and not bucket.empty:
        topk = topk.merge(bucket, on="cups_sgc", how="left", suffixes=("", "_bucket"))

    # Estadísticos generales
    probs = topk["prob_fraude"].astype(float).values
    as_of = str(topk["asOf"].iloc[0]) if "asOf" in topk.columns and not topk.empty else "unknown"
    summary = {
        "n_top": len(topk),
        "as_of": as_of,
        "prob_min": float(np.min(probs)) if len(probs) else float("nan"),
        "prob_p25": float(np.percentile(probs, 25)) if len(probs) else float("nan"),
        "prob_mediana": float(np.median(probs)) if len(probs) else float("nan"),
        "prob_p75": float(np.percentile(probs, 75)) if len(probs) else float("nan"),
        "prob_max": float(np.max(probs)) if len(probs) else float("nan"),
    }

    # Construir razones por fila
    razones_list: List[List[str]] = []
    for _, row in topk.iterrows():
        brow = None
        if bucket is not None and "cups_sgc" in topk.columns:
            # bucket ya está mezclado en topk, extraer sub-serie
            if bucket is not None:
                # tomar solo columnas bucket del topk actual
                bcols = [c for c in topk.columns if c.startswith("eventos")]
                brow = row[bcols] if bcols else None
        reasons = _mk_reason_row(row, brow)
        razones_list.append(reasons if reasons else ["Probabilidad alta según patrón multivariable del modelo"])

    # Preparar Markdown
    lines: List[str] = []
    lines.append(f"# Informe Top Probables de Fraude (asOf {summary['as_of']})")
    lines.append("")
    lines.append("## Resumen")
    lines.append(f"- Casos analizados: {summary['n_top']}")
    lines.append(f"- Probabilidad (min/mediana/max): {summary['prob_min']:.3f} / {summary['prob_mediana']:.3f} / {summary['prob_max']:.3f}")
    lines.append(f"- Cuartiles (p25/p75): {summary['prob_p25']:.3f} / {summary['prob_p75']:.3f}")
    lines.append("")
    lines.append("## Principales motivos por caso (Top-N)")
    head_n = min(topn_detail, len(topk))
    lines.append("")
    lines.append("| Rank | CUPS | Prob. | Tasa vec. | Vecinos F/T | Hist. Fraude | Ev90d | Zero%90d | Motivos |")
    lines.append("|---:|---|---:|---:|---:|:---:|---:|---:|---|")

    for i in range(head_n):
        row = topk.iloc[i]
        prob = float(row["prob_fraude"])
        tasa = float(row["tasa_fraude_vec"]) if "tasa_fraude_vec" in row and pd.notna(row["tasa_fraude_vec"]) else np.nan
        vf = int(row["vecinos_fraude"]) if "vecinos_fraude" in row and pd.notna(row["vecinos_fraude"]) else 0
        vt = int(row["vecinos_total"]) if "vecinos_total" in row and pd.notna(row["vecinos_total"]) else 0
        hist = int(row["fraude_hist"]) if "fraude_hist" in row and pd.notna(row["fraude_hist"]) else 0
        # Buscar columnas dinámicas para ev count y zero pct con valores por defecto
        evcnt_val = None
        for c in row.index:
            if isinstance(c, str) and c.startswith("event_count_"):
                val = row[c]
                try:
                    evcnt_val = int(val) if pd.notna(val) and str(val).lower() != "none" else 0
                except Exception:
                    evcnt_val = 0
                break
        evcnt = str(evcnt_val if evcnt_val is not None else 0)

        zpct_val = None
        for c in row.index:
            if isinstance(c, str) and c.startswith("curva_zero_hours_pct_"):
                val = row[c]
                try:
                    zpct_val = float(val) if pd.notna(val) and str(val).lower() != "none" else 0.0
                except Exception:
                    zpct_val = 0.0
                break
        zpct = f"{(zpct_val if zpct_val is not None else 0.0):.0%}"

        reasons = "; ".join(razones_list[i])
        lines.append(f"| {i+1} | {row['cups_sgc']} | {prob:.3f} | {'' if np.isnan(tasa) else f'{tasa:.0%}'} | {vf}/{vt} | {('Sí' if hist==1 else 'No')} | {evcnt} | {zpct} | {reasons} |")

    lines.append("")
    lines.append("## Detalle completo (Top-k)")
    # Exportar detalle completo en CSV aparte y enlazar
    detalle_csv = os.path.join(out_dir, f"detalle_topk_{as_of}.csv")
    # Selección de columnas útiles
    cols = [c for c in ["rank", "cups_sgc", "asOf", "prob_fraude", "tasa_fraude_vec", "vecinos_fraude", "vecinos_total", "fraude_hist"] if c in topk.columns]
    extra_cols = [c for c in topk.columns if str(c).startswith(("event_", "curva_"))]
    # Añadir contexto relevante si existe
    ctx_cols = [c for c in ["tipo_punto_suministro", "tension_suministro", "identificador_actividad_economica", "potencia_contrato", "municipio", "provincia", "codigo_postal"] if c in topk.columns]
    extra_cols = list(dict.fromkeys(extra_cols + ctx_cols))
    export_cols = list(dict.fromkeys(cols + extra_cols))  # mantener orden y únicos
    topk.reset_index(drop=True).assign(rank=lambda d: d.index + 1)[export_cols].to_csv(detalle_csv, index=False, encoding="utf-8")
    lines.append(f"- Fichero CSV con el detalle completo: {detalle_csv}")

    # Guardar informe
    out_path = os.path.join(out_dir, f"informe_topk_{as_of}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return out_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Genera un informe explicativo de los top-k casos de fraude.")
    p.add_argument("--topk", type=str, default=None, help="Ruta a scoring_top*.parquet. Por defecto usa el último en 'scoring/'.")
    p.add_argument("--bucketized", type=str, default=None, help="Ruta al CSV bucketizado. Por defecto usa el último en 'data/'.")
    p.add_argument("--out-dir", type=str, default="informes", help="Directorio de salida para el informe.")
    p.add_argument("--topn-detail", type=int, default=20, help="Número de filas a mostrar en la tabla del informe.")
    p.add_argument("--expedientes-csv", type=str, default="data/Expedientes.csv", help="CSV de expedientes para filtrar inspecciones recientes.")
    p.add_argument("--filter-recent-clear-days", type=int, default=30, help="Días hacia atrás para excluir CUPS con expedientes que descartan anomalía.")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        out_path = build_report(
            topk_path=args.topk,
            bucketized_path=args.bucketized,
            out_dir=args.out_dir,
            topn_detail=args.topn_detail,
            expedientes_csv=args.expedientes_csv,
            filter_recent_clear_days=args.filter_recent_clear_days,
        )
        print(f"Informe generado: {out_path}")
    except Exception as e:
        print(f"[ERROR] {e}")
        raise


if __name__ == "__main__":
    main()
