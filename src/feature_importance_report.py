import os
import sys
import glob
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance

def find_latest_dataset() -> str:
    paths = sorted(glob.glob("features/dataset_cups_snapshot_*.parquet"))
    if not paths:
        raise FileNotFoundError("No datasets found at features/dataset_cups_snapshot_*.parquet")
    return paths[-1]

def load_data(path: str) -> tuple[pd.DataFrame, pd.Series, str]:
    df = pd.read_parquet(path)
    if "label_fraude" not in df.columns:
        raise ValueError("Column 'label_fraude' not found in dataset")
    df = df.dropna(subset=["label_fraude"]).copy()
    df["label_fraude"] = df["label_fraude"].astype(int)
    # Derivar _asOf_date si falta (para alinearse con entrenamiento)
    if "_asOf_date" not in df.columns and "asOf" in df.columns:
        try:
            df["_asOf_date"] = pd.to_datetime(df["asOf"], errors="coerce").dt.date
        except Exception:
            pass
    # Drop ID / meta columns
    drop_cols = [c for c in ["cups_sgc", "asOf", "label_fraude"] if c in df.columns]
    X = df.drop(columns=drop_cols, errors="ignore")
    y = df["label_fraude"]
    as_of = str(df.get("asOf", pd.Series(["unknown"])).iloc[0])
    return X, y, as_of

def try_model_load(path: str = "models/tabular_calibrated.joblib"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model not found: {path}")
    return joblib.load(path)

def compute_permutation_importance(estimator, X: pd.DataFrame, y: pd.Series, n: int = 5000):
    # Sample to speed up if needed
    n = min(len(X), n)
    if len(X) > n:
        Xs = X.sample(n=n, random_state=0)
        ys = y.loc[Xs.index]
    else:
        Xs = X
        ys = y
    pi = permutation_importance(
        estimator,
        Xs,
        ys,
        n_repeats=3,
        random_state=0,
        scoring="average_precision",
        n_jobs=-1,
    )
    imps = sorted(zip(list(Xs.columns), pi.importances_mean), key=lambda x: -x[1])
    return imps, n

def fallback_tree_importance(model_pipeline, X: pd.DataFrame):
    # Attempts to extract RF feature_importances_ mapped to transformed names
    try:
        pre = model_pipeline.named_steps["pre"]
        clf = model_pipeline.named_steps["clf"]
        if not hasattr(clf, "feature_importances_"):
            return None
        # Get transformed feature names
        try:
            feat_names = list(pre.get_feature_names_out())
        except Exception:
            # Fallback: use positional names
            feat_names = [f"f_{i}" for i in range(len(clf.feature_importances_))]
        return sorted(zip(feat_names, clf.feature_importances_), key=lambda x: -x[1])
    except Exception:
        return None

def get_expected_columns(model) -> list | None:
    # Intenta extraer las columnas esperadas usadas en el entrenamiento
    for obj in [model, (model.calibrated_classifiers_[0] if hasattr(model, "calibrated_classifiers_") and len(model.calibrated_classifiers_) > 0 else None)]:
        if obj is None:
            continue
        # Directamente en el objeto
        if hasattr(obj, "feature_names_in_"):
            try:
                return list(getattr(obj, "feature_names_in_"))
            except Exception:
                pass
        # Intentar vía estimator/base_estimator
        for inner_attr in ["estimator", "base_estimator"]:
            if hasattr(obj, inner_attr):
                inner = getattr(obj, inner_attr)
                if hasattr(inner, "feature_names_in_"):
                    try:
                        return list(getattr(inner, "feature_names_in_"))
                    except Exception:
                        pass
    return None

def main():
    ds_path = find_latest_dataset()
    X, y, as_of = load_data(ds_path)
    model = try_model_load()
    # Alinear columnas a las usadas en entrenamiento
    expected = get_expected_columns(model)
    if expected:
        missing = [c for c in expected if c not in X.columns]
        if missing:
            X = X.copy()
            for col in missing:
                X[col] = 0
        # Reordenar/filtrar a las esperadas exactamente
        X = X.reindex(columns=expected)

    informes_dir = "informes"
    os.makedirs(informes_dir, exist_ok=True)
    out_csv = os.path.join(informes_dir, f"feature_importance_{as_of}.csv")
    out_json = os.path.join(informes_dir, f"feature_importance_{as_of}.json")

    # Prefer permutation importance on the calibrated model (handles preprocessing)
    try:
        imps, n_used = compute_permutation_importance(model, X, y, n=5000)
        top25 = imps[:25]
        pd.DataFrame(top25, columns=["feature", "perm_importance_AP"]).to_csv(out_csv, index=False, encoding="utf-8")
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "dataset": os.path.basename(ds_path),
                    "asOf": as_of,
                    "method": "permutation_importance(average_precision)",
                    "n_rows_used": n_used,
                    "top25": [{"feature": k, "importance": float(v)} for k, v in top25],
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"Top 25 features by permutation importance (AP) from {os.path.basename(ds_path)} (n={n_used}):")
        for i, (k, v) in enumerate(top25, start=1):
            print(f"{i:02d}. {k}: {v:.6f}")
        print(f"Saved: {out_csv}")
        return
    except Exception as e:
        print(f"[WARN] Permutation importance failed: {e}")

    # Fallback: if calibrated model is CalibratedClassifierCV, try a base pipeline
    try:
        cc = model.calibrated_classifiers_[0]
        base = getattr(cc, "estimator", None) or getattr(cc, "base_estimator", None)  # Pipeline(pre, rf)
        imps2 = fallback_tree_importance(base, X)
        if imps2:
            top25 = imps2[:25]
            pd.DataFrame(top25, columns=["feature", "rf_feature_importance"]).to_csv(out_csv, index=False, encoding="utf-8")
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "dataset": os.path.basename(ds_path),
                        "asOf": as_of,
                        "method": "rf_feature_importances_ (transformed space)",
                        "n_rows_used": int(len(X)),
                        "top25": [{"feature": k, "importance": float(v)} for k, v in top25],
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            print(f"Top 25 features by RF feature_importances_ from {os.path.basename(ds_path)}:")
            for i, (k, v) in enumerate(top25, start=1):
                print(f"{i:02d}. {k}: {v:.6f}")
            print(f"Saved: {out_csv}")
            return
    except Exception as e:
        print(f"[WARN] Fallback RF importance failed: {e}")

    print("[ERROR] Could not compute feature importances.")

if __name__ == "__main__":
    main()
