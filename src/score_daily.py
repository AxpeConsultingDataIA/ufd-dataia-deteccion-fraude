import argparse
import os
import re
from glob import glob
from typing import Optional, Tuple

import joblib
import numpy as np
import pandas as pd

from utils.neo4j_features import graph_features_for_cups
from utils.events_consumption_features import build_events_consumption_features

def _find_bucketized_csv(data_dir: str = "data") -> str:
    candidates = sorted(glob(os.path.join(data_dir, "*_bucketized.csv")))
    if not candidates:
        raise FileNotFoundError(
            "No se encontró ningún CSV bucketizado (pattern: data/*_bucketized.csv). "
            "Ejecuta utils/bucketize_csv.py o indica --bucketized-path."
        )
    return candidates[0]

def _read_bucketized_features(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)  # conservar categorías como string
    # Normalizar nombre de id
    id_col = None
    for cand in ["cups_sgc", "CUPS", "cups", "nis_rad", "NIS", "nis"]:
        if cand in df.columns:
            id_col = cand
            break
    if id_col is None:
        raise ValueError(f"No se encontró columna de id en {path} (se esperaba cups_sgc o nis_rad)")
    if id_col != "cups_sgc":
        df = df.rename(columns={id_col: "cups_sgc"})
    # Normalizar valores de CUPS y eliminar duplicados
    df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=["cups_sgc"])
    return df

def _build_scoring_features(
    as_of: Optional[str],
    bucketized_path: Optional[str],
    database: Optional[str],
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    contador_parquet: Optional[str] = "features/datos_contador_clean.parquet",
    events_lookback: int = 90,
    include_embeddings: bool = True,
    embeddings_graph_name: str = "cupsGraph",
    embeddings_dim: int = 64,
) -> pd.DataFrame:
    as_of_str = as_of or pd.Timestamp.today().date().isoformat()
    bpath = bucketized_path or _find_bucketized_csv()
    tab = _read_bucketized_features(bpath)
    tab.insert(1, "asOf", as_of_str)

    try:
        gfeat = graph_features_for_cups(
            tab["cups_sgc"].tolist(),
            as_of=as_of_str,
            database=database,
            include_embeddings=include_embeddings,
            embeddings_graph_name=embeddings_graph_name,
            embeddings_dim=embeddings_dim,
            uri=uri,
            user=user,
            password=password,
        )
    except Exception as e:
        print(f"[WARN] Neo4j features desactivadas por error: {e}")
        gfeat = pd.DataFrame({"cups_sgc": tab["cups_sgc"], "asOf": as_of_str})
    feat = tab.merge(gfeat, on=["cups_sgc", "asOf"], how="left")
    # rellenar NaNs de grafo
    graph_cols = [c for c in feat.columns if c.startswith("vecinos_") or c.startswith("tasa_") or c.startswith("fraude_hist") or c.startswith("emb_")]
    for c in graph_cols:
        if c in feat.columns:
            if feat[c].dtype.kind in "fbiu":
                feat[c] = feat[c].fillna(0)
            else:
                feat[c] = feat[c].fillna("")

    # Unir features de datos de contador si existen
    if contador_parquet and os.path.exists(contador_parquet):
        try:
            cnt = pd.read_parquet(contador_parquet)
            # Normalizar id a 'cups_sgc'
            id_cnt = next((c for c in ["cups_sgc", "cups", "CUPS", "nis_rad", "NIS", "nis"] if c in cnt.columns), None)
            if id_cnt and id_cnt != "cups_sgc":
                cnt = cnt.rename(columns={id_cnt: "cups_sgc"})
            if "cups_sgc" in cnt.columns:
                dup_cols = [c for c in cnt.columns if c in feat.columns and c != "cups_sgc"]
                if dup_cols:
                    cnt = cnt.drop(columns=dup_cols)
                # Normalizar formato de CUPS en ambos dataframes
                feat["cups_sgc"] = feat["cups_sgc"].astype(str).str.upper().str.strip()
                cnt["cups_sgc"] = cnt["cups_sgc"].astype(str).str.upper().str.strip()
                feat = feat.merge(cnt, on="cups_sgc", how="left")
        except Exception as e:
            print(f"[WARN] contador features omitidas: {e}")

    # Unir features de eventos y curva horaria (lookback configurable)
    try:
        evcv = build_events_consumption_features(as_of=as_of_str, days_lookback=events_lookback)
        ren = {c: c.replace("XXd", f"{events_lookback}d") for c in evcv.columns if "XXd" in c}
        if ren:
            evcv = evcv.rename(columns=ren)
        # Normalizar id a 'cups_sgc' y formato CUPS
        id_evcv = next((c for c in ["cups_sgc", "cups", "CUPS", "nis_rad", "NIS", "nis"] if c in evcv.columns), None)
        if id_evcv and id_evcv != "cups_sgc":
            evcv = evcv.rename(columns={id_evcv: "cups_sgc"})
        feat["cups_sgc"] = feat["cups_sgc"].astype(str).str.upper().str.strip()
        evcv["cups_sgc"] = evcv["cups_sgc"].astype(str).str.upper().str.strip()
        feat = feat.merge(evcv, on="cups_sgc", how="left")
    except Exception as e:
        print(f"[WARN] eventos/curva omitidas: {e}")

    # Rellenar NaNs en señales de eventos/curva para que aparezcan en el informe
    evcv_cols = [c for c in feat.columns if isinstance(c, str) and (c.startswith("event_") or c.startswith("curva_"))]
    for c in evcv_cols:
        if feat[c].dtype.kind in "fbiu":
            feat[c] = feat[c].fillna(0)
        else:
            feat[c] = feat[c].fillna("")
    # Compatibilidad con modelo entrenado (columnas *_90d esperadas por el pipeline)
    try:
        canonical_90d = [
            "event_count_90d",
            "event_suspicious_90d",
            "curva_ai_sum_90d",
            "curva_ae_sum_90d",
            "curva_hours_90d",
            "curva_zero_hours_90d",
            "curva_zero_hours_pct_90d",
        ]
        if events_lookback != 90:
            for name in canonical_90d:
                if name not in feat.columns:
                    alt = name.replace("_90d", f"_{events_lookback}d")
                    if alt in feat.columns:
                        feat[name] = feat[alt]
                    else:
                        # por defecto 0 para numéricas
                        feat[name] = 0
    except Exception as _:
        pass
    return feat

def _prepare_X(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Index]:
    keep_cols = [c for c in df.columns if c not in ["cups_sgc", "asOf"]]
    X = df[keep_cols].copy()
    return X, df.index

def precision_recall_at_k(y_scores: np.ndarray, k: int, y_true: Optional[np.ndarray] = None) -> Tuple[float, float]:
    k = max(1, min(k, len(y_scores)))
    order = np.argsort(-y_scores)
    top_idx = order[:k]
    if y_true is None:
        return float("nan"), float("nan")
    y_top = y_true[top_idx]
    tp = int(y_top.sum())
    precision_k = tp / k
    recall_k = tp / max(1, int(y_true.sum()))
    return float(precision_k), float(recall_k)

def score(
    as_of: Optional[str],
    bucketized_path: Optional[str],
    database: Optional[str],
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    model_path: str = "models/tabular_calibrated.joblib",
    out_dir: str = "scoring",
    k_top: int = 100,
    contador_parquet: Optional[str] = "features/datos_contador_clean.parquet",
    events_lookback: int = 90,
    include_embeddings: bool = True,
    embeddings_graph_name: str = "cupsGraph",
    embeddings_dim: int = 64,
) -> Tuple[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    feat = _build_scoring_features(
        as_of=as_of,
        bucketized_path=bucketized_path,
        database=database,
        uri=uri,
        user=user,
        password=password,
        contador_parquet=contador_parquet,
        events_lookback=events_lookback,
        include_embeddings=include_embeddings,
        embeddings_graph_name=embeddings_graph_name,
        embeddings_dim=embeddings_dim,
    )

    # Cargar modelo
    model = joblib.load(model_path)

    # Compatibilidad con columnas usadas en entrenamiento
    if "label_3clases" not in feat.columns:
        feat["label_3clases"] = 0

    X, _ = _prepare_X(feat)
    # Predecir
    probs = model.predict_proba(X)[:, 1]
    scored = feat[["cups_sgc", "asOf"]].copy()
    scored["prob_fraude"] = probs
    # Señales explicativas mínimas
    for c in ["tasa_fraude_vec", "vecinos_total", "vecinos_fraude", "fraude_hist"]:
        if c in feat.columns:
            scored[c] = feat[c]

    # Señales explicativas de eventos/curva y contexto de contador
    explain_prefixes = ("event_", "curva_")
    for col in [col for col in feat.columns if isinstance(col, str) and col.startswith(explain_prefixes)]:
        scored[col] = feat[col]
    for col in ["tipo_punto_suministro", "tension_suministro", "identificador_actividad_economica", "potencia_contrato", "municipio", "provincia", "codigo_postal"]:
        if col in feat.columns:
            scored[col] = feat[col]

    # Ranking y top-k
    scored = scored.sort_values("prob_fraude", ascending=False).reset_index(drop=True)
    scored["rank"] = np.arange(1, len(scored) + 1)
    top_k = scored.head(min(k_top, len(scored)))

    # Fechas para nombre de archivos
    as_of_str = scored["asOf"].iloc[0] if "asOf" in scored.columns and not scored.empty else (as_of or "unknown")
    full_path = os.path.join(out_dir, f"scoring_full_{as_of_str}.parquet")
    topk_path = os.path.join(out_dir, f"scoring_top{len(top_k)}_{as_of_str}.parquet")

    scored.to_parquet(full_path, index=False)
    top_k.to_parquet(topk_path, index=False)
    return full_path, topk_path

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scoring diario de CUPS aprovechando features tabulares+grafo.")
    p.add_argument("--as-of", type=str, default=None, help="Fecha de corte (YYYY-MM-DD). Por defecto hoy.")
    p.add_argument("--bucketized-path", type=str, default=None, help="Ruta del CSV bucketizado (por defecto busca data/*_bucketized.csv).")
    p.add_argument("--database", type=str, default=None, help="Nombre de base de datos en Neo4j (opcional).")
    p.add_argument("--neo4j-uri", type=str, default=None, help="URI de Neo4j (p. ej., bolt://localhost:7687).")
    p.add_argument("--neo4j-user", type=str, default=None, help="Usuario de Neo4j (por defecto env NEO4J_USER o neo4j).")
    p.add_argument("--neo4j-password", type=str, default=None, help="Password de Neo4j (por defecto env NEO4J_PASSWORD).")
    p.add_argument("--model", type=str, default="models/tabular_calibrated.joblib", help="Ruta del modelo calibrado.")
    p.add_argument("--out-dir", type=str, default="scoring", help="Directorio de salida.")
    p.add_argument("--k", type=int, default=100, help="Top-k de inspecciones sugeridas.")
    p.add_argument("--contador-parquet", type=str, default="features/datos_contador_clean.parquet", help="Parquet con features de datos de contador (opcional).")
    p.add_argument("--events-lookback", type=int, default=90, help="Días de ventana para eventos/curva horaria.")
    p.add_argument("--no-embeddings", action="store_true", help="Desactivar extracción de embeddings GDS.")
    p.add_argument("--emb-graph", type=str, default="cupsGraph", help="Nombre del grafo GDS para embeddings.")
    p.add_argument("--emb-dim", type=int, default=64, help="Dimensión de embeddings fastRP.")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        full_path, topk_path = score(
            as_of=args.as_of,
            bucketized_path=args.bucketized_path,
            database=args.database,
            uri=args.neo4j_uri,
            user=args.neo4j_user,
            password=args.neo4j_password,
            model_path=args.model,
            out_dir=args.out_dir,
            k_top=args.k,
            contador_parquet=args.contador_parquet,
            events_lookback=args.events_lookback,
            include_embeddings=not args.no_embeddings,
            embeddings_graph_name=args.emb_graph,
            embeddings_dim=args.emb_dim,
        )
        print("Scoring completo:", full_path)
        print("Top-k:", topk_path)
    except Exception as e:
        print(f"[ERROR] {e}")
        raise

if __name__ == "__main__":
    main()
