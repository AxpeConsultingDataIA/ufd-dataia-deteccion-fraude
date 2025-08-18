import argparse
import os
import glob
from typing import Optional, Set, List, Dict

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    brier_score_loss,
    precision_recall_curve,
)

def find_latest_scoring(scoring_dir: str = "scoring") -> str:
    files = sorted(glob.glob(os.path.join(scoring_dir, "scoring_full_*.parquet")))
    if not files:
        raise FileNotFoundError("No se encontró scoring_full_*.parquet en 'scoring/'.")
    return files[-1]

def load_scoring(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    need = ["cups_sgc", "prob_fraude"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"Falta la columna requerida en scoring: {c}")
    if "asOf" not in df.columns:
        df["asOf"] = "unknown"
    df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    return df

def load_labels_for_asof(as_of: str) -> Optional[pd.DataFrame]:
    cand_dataset = f"features/dataset_cups_snapshot_{as_of}.parquet"
    cand_labels = f"features/labels_cups_snapshot_{as_of}.parquet"
    if os.path.exists(cand_dataset):
        df = pd.read_parquet(cand_dataset)
    elif os.path.exists(cand_labels):
        df = pd.read_parquet(cand_labels)
    else:
        return None
    if "cups_sgc" not in df.columns:
        return None
    df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
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

def collect_recent_cleared_cups(expedientes_csv: str, cups: Set[str], as_of: str, days: int = 30) -> Set[str]:
    if not expedientes_csv or not os.path.exists(expedientes_csv):
        return set()
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_ts):
        return set()
    cutoff = as_of_ts - pd.Timedelta(days=days)
    cleared: Set[str] = set()

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
                sub = sub[(sub["__date"] >= cutoff) & (sub["__date"] <= as_of_ts)]
                if sub.empty:
                    continue
            if text_cols:
                try:
                    txt = sub[text_cols].astype(str).agg(" ".join, axis=1).str.upper()
                except Exception:
                    txt = sub[text_cols[0]].astype(str).str.upper()
            else:
                txt = pd.Series([""] * len(sub), index=sub.index)
            # "Descartan" indicios vs. "Confirman"
            mask_clear = txt.str.contains("DESCART", na=False) | txt.str.contains("ARCHIV", na=False) | txt.str.contains("NO FRAUD", na=False) | txt.str.contains("SIN IRREG", na=False)
            mask_conf = txt.str.contains("CONFIRM", na=False) | txt.str.contains("FRAUD", na=False)
            sub_clear = sub[mask_clear & (~mask_conf)]
            if not sub_clear.empty:
                ids = sub_clear[id_col].astype(str).str.upper().str.strip().unique().tolist()
                cleared.update(ids)
    except Exception:
        return cleared
    return cleared

def precision_recall_at_k(y_true: np.ndarray, y_scores: np.ndarray, k: int) -> Dict[str, float]:
    k = max(1, min(k, len(y_scores)))
    order = np.argsort(-y_scores)
    top_idx = order[:k]
    y_top = y_true[top_idx]
    tp = int(y_top.sum())
    return {
        "k": int(k),
        "precision@k": tp / k,
        "recall@k": tp / max(1, int(y_true.sum())),
        "positives_in_topk": tp,
    }

def compute_metrics(df: pd.DataFrame, label_col: str, prob_col: str, ks: List[int]) -> Dict:
    y = df[label_col].astype(int).to_numpy()
    p = df[prob_col].astype(float).to_numpy()
    res = {}
    # Global metrics
    try:
        res["AUPRC"] = float(average_precision_score(y, p))
    except Exception:
        res["AUPRC"] = float("nan")
    try:
        res["ROC_AUC"] = float(roc_auc_score(y, p))
    except Exception:
        res["ROC_AUC"] = float("nan")
    try:
        res["Brier"] = float(brier_score_loss(y, p))
    except Exception:
        res["Brier"] = float("nan")

    # PR curve points (downsampled)
    try:
        pr, rc, _ = precision_recall_curve(y, p)
        # Sample 20 points for report
        idx = np.linspace(0, len(pr) - 1, num=min(20, len(pr))).astype(int)
        res["PR_curve"] = [{"precision": float(pr[i]), "recall": float(rc[i])} for i in idx]
    except Exception:
        res["PR_curve"] = []

    # Ranking metrics
    order = np.argsort(-p)
    ks_clean = [int(min(k, len(p))) for k in ks]
    res["ranking"] = [precision_recall_at_k(y, p, k) for k in ks_clean]
    return res

def build_eval(
    scoring_path: Optional[str],
    expedientes_csv: Optional[str],
    out_dir: str,
    filter_recent_clear_days: int,
    k_list: List[int],
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    scoring_path = scoring_path or find_latest_scoring()
    sc = load_scoring(scoring_path)
    as_of = str(sc["asOf"].iloc[0]) if "asOf" in sc.columns and not sc.empty else "unknown"

    labels = load_labels_for_asof(as_of)
    if labels is None:
        raise FileNotFoundError(f"No se encontraron labels/dataset para asOf {as_of} en 'features/'.")
    # Elegir columna label
    label_col = "label_fraude" if "label_fraude" in labels.columns else None
    if label_col is None:
        raise ValueError("No existe columna 'label_fraude' en el dataset/labels para evaluar.")

    # Merge por CUPS
    merged = sc.merge(labels[["cups_sgc", label_col]], on="cups_sgc", how="left")
    merged = merged.dropna(subset=[label_col]).copy()
    merged[label_col] = merged[label_col].astype(int)

    report = {
        "scoring_file": scoring_path,
        "asOf": as_of,
        "n_total_scored": int(sc.shape[0]),
        "n_labeled": int(merged.shape[0]),
        "positives": int(merged[label_col].sum()),
    }

    # Métricas globales
    metrics_all = compute_metrics(merged, label_col, "prob_fraude", k_list)
    report["overall"] = metrics_all

    # Métricas filtrando expedientes que descartan anomalía en últimos N días
    try:
        if expedientes_csv and filter_recent_clear_days > 0:
            cups = set(merged["cups_sgc"].astype(str).str.upper().str.strip().unique().tolist())
            cleared = collect_recent_cleared_cups(expedientes_csv, cups, as_of, days=filter_recent_clear_days)
            if cleared:
                filt = merged[~merged["cups_sgc"].astype(str).str.upper().str.strip().isin(cleared)].copy()
            else:
                filt = merged.copy()
            report["filtered_recent_clear_days"] = {
                "days": int(filter_recent_clear_days),
                "n_after_filter": int(filt.shape[0]),
                "positives_after_filter": int(filt[label_col].sum()),
                "metrics": compute_metrics(filt, label_col, "prob_fraude", k_list),
            }
    except Exception as e:
        report["filtered_recent_clear_days_error"] = str(e)

    # Guardar JSON y MD
    json_path = os.path.join(out_dir, f"eval_{as_of}.json")
    md_path = os.path.join(out_dir, f"eval_{as_of}.md")

    import json
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    lines: List[str] = []
    lines.append(f"# Evaluación de scoring (asOf {as_of})")
    lines.append("")
    lines.append(f"- Scoring: {scoring_path}")
    lines.append(f"- Casos con label: {report['n_labeled']} (de {report['n_total_scored']})")
    lines.append(f"- Positivos (fraude): {report['positives']}")
    lines.append("")
    lines.append("## Métricas globales")
    lines.append(f"- AUPRC: {report['overall']['AUPRC']:.4f}")
    lines.append(f"- ROC_AUC: {report['overall']['ROC_AUC']:.4f}")
    lines.append(f"- Brier: {report['overall']['Brier']:.4f}")
    lines.append("- Ranking:")
    for r in report["overall"]["ranking"]:
        lines.append(f"  - precision@{r['k']}: {r['precision@k']:.3f} | recall@{r['k']}: {r['recall@k']:.3f} | positivos en top-{r['k']}: {r['positives_in_topk']}")

    if "filtered_recent_clear_days" in report:
        fr = report["filtered_recent_clear_days"]
        lines.append("")
        lines.append(f"## Métricas excluyendo expedientes que descartan anomalía (últimos {fr['days']} días)")
        lines.append(f"- Casos tras filtro: {fr['n_after_filter']} | Positivos: {fr['positives_after_filter']}")
        lines.append(f"- AUPRC: {fr['metrics']['AUPRC']:.4f}")
        lines.append(f"- ROC_AUC: {fr['metrics']['ROC_AUC']:.4f}")
        lines.append(f"- Brier: {fr['metrics']['Brier']:.4f}")
        lines.append("- Ranking:")
        for r in fr["metrics"]["ranking"]:
            lines.append(f"  - precision@{r['k']}: {r['precision@k']:.3f} | recall@{r['k']}: {r['recall@k']:.3f} | positivos en top-{r['k']}: {r['positives_in_topk']}")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Informe: {md_path}")
    print(f"JSON: {json_path}")
    return md_path

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evalúa qué tan bien el scoring predice fraude contra labels disponibles.")
    p.add_argument("--scoring", type=str, default=None, help="Ruta a scoring_full_*.parquet. Por defecto usa el último.")
    p.add_argument("--expedientes-csv", type=str, default="data/Expedientes.csv", help="CSV de expedientes (para filtrar revisiones recientes que descartan).")
    p.add_argument("--out-dir", type=str, default="informes", help="Directorio de salida.")
    p.add_argument("--filter-recent-clear-days", type=int, default=30, help="Excluir CUPS con expedientes que descartan anomalía en últimos N días.")
    p.add_argument("--k-list", type=str, default="10,50,100", help="Lista de k para precision@k/recall@k, separada por comas.")
    return p.parse_args()

def main():
    args = parse_args()
    ks = [int(x) for x in str(args.k_list).split(",") if str(x).strip().isdigit()]
    try:
        md = build_eval(
            scoring_path=args.scoring,
            expedientes_csv=args.expedientes_csv,
            out_dir=args.out_dir,
            filter_recent_clear_days=args.filter_recent_clear_days,
            k_list=ks if ks else [10, 50, 100],
        )
    except Exception as e:
        print(f"[ERROR] {e}")
        raise

if __name__ == "__main__":
    main()
