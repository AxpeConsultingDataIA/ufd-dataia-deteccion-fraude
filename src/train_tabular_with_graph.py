import argparse
import json
import os
import re
import datetime as dt
from glob import glob
from typing import List, Tuple, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

# Config defaults (seed)
try:
    from config import RANDOM_SEED
except Exception:
    RANDOM_SEED = 42


def _find_latest_dataset(features_dir: str = "features") -> str:
    files = sorted(glob(os.path.join(features_dir, "dataset_cups_snapshot_*.parquet")))
    if not files:
        raise FileNotFoundError(
            "No se encontró un dataset snapshot en 'features/'. Ejecuta primero: "
            "python -m src.pipeline_build_snapshot --as-of YYYY-MM-DD"
        )
    return files[-1]

def _load_datasets(dataset_path: str | None, dataset_glob: str | None) -> pd.DataFrame:
    paths: List[str] = []
    if dataset_glob:
        paths = sorted(glob(dataset_glob))
    if dataset_path:
        paths.append(dataset_path)
    if not paths:
        p = _find_latest_dataset()
        paths = [p]
    frames = []
    for p in paths:
        try:
            frames.append(pd.read_parquet(p))
        except Exception:
            pass
    if not frames:
        raise FileNotFoundError("No se pudieron cargar datasets de entrenamiento.")
    df = pd.concat(frames, ignore_index=True)
    if "asOf" not in df.columns:
        df["asOf"] = "unknown"
    df["asOf"] = df["asOf"].astype(str)
    try:
        df["_asOf_date"] = pd.to_datetime(df["asOf"], errors="coerce").dt.date
    except Exception:
        df["_asOf_date"] = pd.NaT
    if "cups_sgc" in df.columns:
        df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    return df


def _split_xy(df: pd.DataFrame, label_col: str) -> Tuple[pd.DataFrame, pd.Series]:
    # Columnas a excluir del entrenamiento
    drop_cols = ["cups_sgc", "asOf"]
    drop_cols = [c for c in drop_cols if c in df.columns]
    X = df.drop(columns=drop_cols + [label_col], errors="ignore")
    y = df[label_col].astype(int)
    return X, y


def _build_preprocess(X: pd.DataFrame) -> ColumnTransformer:
    # Categóricas = dtype object o boolean; numéricas = resto
    cat_cols = [c for c in X.columns if X[c].dtype == "object" or str(X[c].dtype) == "category" or X[c].dtype == "bool"]
    num_cols = [c for c in X.columns if c not in cat_cols]

    # Imputación para evitar NaNs en el estimador
    cat_pipe = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=True)),
        ]
    )
    num_pipe = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="median")),
        ]
    )

    pre = ColumnTransformer(
        transformers=[
            ("cat", cat_pipe, cat_cols),
            ("num", num_pipe, num_cols),
        ],
        sparse_threshold=1.0,
        remainder="drop",
        verbose_feature_names_out=False,
    )
    return pre


def precision_recall_at_k(y_true: np.ndarray, y_scores: np.ndarray, k: int) -> Tuple[float, float]:
    k = max(1, min(k, len(y_scores)))
    order = np.argsort(-y_scores)
    top_idx = order[:k]
    y_top = y_true[top_idx]
    tp = int(y_top.sum())
    precision_k = tp / k
    recall_k = tp / max(1, int(y_true.sum()))
    return precision_k, recall_k


def train(
    dataset_path: str | None,
    label_col: str = "label_fraude",
    test_size: float = 0.2,
    random_state: int = RANDOM_SEED,
    n_estimators: int = 500,
    max_depth: int | None = None,
    out_model_path: str = "models/tabular_calibrated.joblib",
    out_metrics_path: str | None = None,
    k_inspecciones: int = 100,
    dataset_glob: str | None = None,
    time_split_last_days: int = 0,
) -> Tuple[str, dict]:
    os.makedirs(os.path.dirname(out_model_path), exist_ok=True)
    if out_metrics_path:
        os.makedirs(os.path.dirname(out_metrics_path), exist_ok=True)

    df = _load_datasets(dataset_path, dataset_glob)
    if label_col not in df.columns:
        raise ValueError(f"No existe la columna de etiqueta '{label_col}' en el dataset.")

    # Eliminar filas sin label
    df = df.dropna(subset=[label_col])
    df[label_col] = df[label_col].astype(int)

    # Split temporal por asOf si se solicita; si no, aleatorio
    train_df = df.copy()
    val_df = None
    if time_split_last_days and "_asOf_date" in df.columns:
        asof_non_na = df["_asOf_date"].dropna()
        if len(asof_non_na):
            max_d = asof_non_na.max()
            cutoff = max_d - dt.timedelta(days=time_split_last_days - 1)
            val_mask = df["_asOf_date"] >= cutoff
            tr_mask = df["_asOf_date"] < cutoff
            if val_mask.any() and tr_mask.any():
                val_df = df.loc[val_mask].copy()
                train_df = df.loc[tr_mask].copy()

    # Construir X/y
    X_train, y_train = _split_xy(train_df, label_col)
    if val_df is not None:
        X_val, y_val = _split_xy(val_df, label_col)
    else:
        X_all, y_all = _split_xy(df, label_col)
        X_train, X_val, y_train, y_val = train_test_split(
            X_all, y_all, test_size=test_size, random_state=random_state, stratify=y_all
        )

    pre = _build_preprocess(X_train)

    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        n_jobs=-1,
        random_state=random_state,
        class_weight="balanced",
    )
    pipe = Pipeline(steps=[("pre", pre), ("clf", clf)])

    # Calibración isotónica con 3-fold sobre el conjunto de entrenamiento (internamente vuelve a ajustar)
    cal = CalibratedClassifierCV(pipe, method="isotonic", cv=3)
    cal.fit(X_train, y_train)

    # Métricas
    prob_val = cal.predict_proba(X_val)[:, 1]
    auprc = float(average_precision_score(y_val, prob_val))
    try:
        rocauc = float(roc_auc_score(y_val, prob_val))
    except Exception:
        rocauc = float("nan")
    k = min(k_inspecciones, len(prob_val))
    prec_k, rec_k = precision_recall_at_k(y_val.to_numpy(), prob_val, k=k)

    # Extraer asOf del nombre de archivo si existe
    m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(dataset_path))
    as_of = m.group(1) if m else df.get("asOf", pd.Series(["unknown"])).iloc[0]

    metrics = {
        "dataset_path": dataset_path,
        "asOf": str(as_of),
        "label_col": label_col,
        "test_size": test_size,
        "random_state": random_state,
        "model": "RandomForest + Calibrated (isotonic)",
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "metrics": {
            "validation": {
                "AUPRC": auprc,
                "ROC_AUC": rocauc,
                f"precision@{k}": float(prec_k),
                f"recall@{k}": float(rec_k),
                "positives_in_val": int(y_val.sum()),
                "n_val": int(y_val.shape[0]),
            }
        },
    }

    joblib.dump(cal, out_model_path)
    if out_metrics_path:
        with open(out_metrics_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2)

    return out_model_path, metrics


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Entrena y calibra un modelo tabular con features de grafo/tabulares.")
    p.add_argument("--dataset", type=str, default=None, help="Ruta parquet del dataset snapshot (features+labels).")
    p.add_argument("--label-col", type=str, default="label_fraude", help="Columna de etiqueta (binaria).")
    p.add_argument("--test-size", type=float, default=0.2, help="Proporción de validación.")
    p.add_argument("--n-estimators", type=int, default=500, help="Árboles en RandomForest.")
    p.add_argument("--max-depth", type=int, default=None, help="Profundidad máxima (None = sin límite).")
    p.add_argument("--k", type=int, default=100, help="k para métricas precision@k/recall@k.")
    p.add_argument("--out-model", type=str, default="models/tabular_calibrated.joblib", help="Ruta de salida del modelo calibrado.")
    p.add_argument("--out-metrics", type=str, default="metrics/training_metrics.json", help="Ruta JSON de métricas.")
    p.add_argument("--dataset-glob", type=str, default=None, help="Glob para cargar múltiples snapshots dataset_cups_snapshot_*.parquet")
    p.add_argument("--time-split-last-days", type=int, default=0, help="Usar últimos N días (asOf) como validación; resto para entrenar")
    return p.parse_args()


def main():
    args = parse_args()
    dataset_path = args.dataset or _find_latest_dataset()
    try:
        out_model, metrics = train(
            dataset_path=dataset_path,
            label_col=args.label_col,
            test_size=args.test_size,
            random_state=RANDOM_SEED,
            n_estimators=args.n_estimators,
            max_depth=args.max_depth,
            out_model_path=args.out_model,
            out_metrics_path=args.out_metrics,
            k_inspecciones=args.k,
            dataset_glob=args.dataset_glob,
            time_split_last_days=args.time_split_last_days,
        )
        print(f"Modelo guardado en: {out_model}")
        if args.out_metrics:
            print(f"Métricas guardadas en: {args.out_metrics}")
        else:
            print(metrics)
    except Exception as e:
        print(f"[ERROR] {e}")
        raise


if __name__ == "__main__":
    main()
