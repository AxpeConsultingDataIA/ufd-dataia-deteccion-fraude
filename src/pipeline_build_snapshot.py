import argparse
import datetime as dt
import os
import sys
from glob import glob
from typing import Iterable, Optional, Tuple, List, Dict

import pandas as pd

# Local utils
from utils.neo4j_features import graph_features_for_cups
from utils.events_consumption_features import build_events_consumption_features


def _find_bucketized_csv(data_dir: str = "data") -> str:
    candidates = sorted(glob(os.path.join(data_dir, "*_bucketized.csv")))
    if not candidates:
        raise FileNotFoundError(
            "No se encontró ningún CSV bucketizado (pattern: data/*_bucketized.csv). "
            "Ejecuta utils/bucketize_csv.py o especifica --bucketized-path."
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
    # Normalizar formato y eliminar duplicados
    df["cups_sgc"] = df["cups_sgc"].astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=["cups_sgc"])
    return df


def _infer_id_column(cols: Iterable[str]) -> Optional[str]:
    for cand in ["cups_sgc", "nis_rad", "CUPS", "cups", "nis", "NIS"]:
        if cand in cols:
            return cand
    # fallback: primera columna que contenga 'cup' o 'nis'
    for c in cols:
        lc = c.lower()
        if "cup" in lc or "nis" in lc:
            return c
    return None


def _label_from_chunk(chunk: pd.DataFrame, id_col: str, cups_set: set, classes_map: Dict[str, int]) -> pd.DataFrame:
    # Columnas candidatas con información de fraude
    candidates = [c for c in chunk.columns if any(k in c.lower() for k in ["fraud", "fraude", "irregular", "clasific", "resultado", "estado"])]
    # Columna de fecha de alta de la irregularidad/fraude (prioridad por nombre)
    date_pref = ["fec_alta", "fecha_alta", "fec_acta", "fecha_acta", "fecha_inicio_anomalia", "fecha_inicio"]
    date_col = next((c for c in date_pref if c in chunk.columns), None)
    if date_col is None:
        # fallback genérico a cualquier columna con 'fec' o 'fecha'
        for c in chunk.columns:
            lc = c.lower()
            if "fec" in lc or "fecha" in lc:
                date_col = c
                break

    cols = [id_col] + candidates + ([date_col] if date_col else [])
    cols = [c for c in cols if c in chunk.columns]
    sub = chunk.loc[chunk[id_col].astype(str).isin(cups_set), cols].copy()
    if sub.empty:
        return pd.DataFrame(columns=["cups_sgc", "label_fraude", "label_3clases", "fec_alta_irreg"])

    # Normalizar strings en columnas texto
    for c in candidates:
        try:
            sub[c] = sub[c].astype(str).str.upper()
        except Exception:
            pass

    # Flags por fila
    is_fraud = pd.Series(False, index=sub.index)
    is_irreg = pd.Series(False, index=sub.index)
    for c in candidates:
        s = sub[c]
        is_fraud = is_fraud | s.str.contains("FRAUD", na=False)
        is_irreg = is_irreg | s.str_contains("IRREG", na=False) if hasattr(s, "str_contains") else (is_irreg | s.str.contains("IRREG", na=False))
    is_any = (is_fraud | is_irreg)

    # Reducir por id: agregados de flags
    agg = pd.DataFrame({
        id_col: sub[id_col].astype(str),
        "fraud_any": is_fraud.astype(int),
        "irreg_any": (~is_fraud & is_irreg).astype(int),  # irreg solo si no es fraude
    }).groupby(id_col, as_index=False).sum()

    # Fecha mínima de alta (entre filas con irregularidad/fraude)
    if date_col and date_col in sub.columns:
        try:
            ds = pd.to_datetime(sub[date_col], dayfirst=True, errors="coerce")
        except Exception:
            ds = pd.to_datetime(sub[date_col], errors="coerce")
        ds_masked = ds.where(is_any, pd.NaT)
        min_dates = pd.DataFrame({
            id_col: sub[id_col].astype(str),
            "fec_alta_irreg": ds_masked
        }).groupby(id_col, as_index=False)["fec_alta_irreg"].min()
        agg = agg.merge(min_dates, on=id_col, how="left")
    else:
        agg["fec_alta_irreg"] = pd.NaT

    # Labels
    agg["label_fraude"] = (agg["fraud_any"] > 0).astype(int)
    def to_multiclass(row) -> int:
        if row["fraud_any"] > 0:
            return classes_map.get("FRAUDE", 1)
        if row["irreg_any"] > 0:
            return classes_map.get("IRREGULARIDAD", 2)
        return classes_map.get("NORMAL", 0)
    agg["label_3clases"] = agg.apply(to_multiclass, axis=1)

    out = agg.rename(columns={id_col: "cups_sgc"})[["cups_sgc", "label_fraude", "label_3clases", "fec_alta_irreg"]]
    # Formatear fecha como ISO si existe
    try:
        out["fec_alta_irreg"] = pd.to_datetime(out["fec_alta_irreg"], errors="coerce").dt.date.astype("string")
    except Exception:
        pass
    return out


def _build_labels(expedientes_csv: str, cups: Iterable[str], classes_map: Dict[str, int]) -> pd.DataFrame:
    cups = pd.Series(list(cups)).dropna().astype(str).unique().tolist()
    cups_set = set(cups)
    # Cargar primera fila para detectar id_col
    sample = pd.read_csv(expedientes_csv, nrows=1)
    id_col = _infer_id_column(sample.columns)
    if id_col is None:
        # No hay columna id detectable, retornar 0s
        return pd.DataFrame({"cups_sgc": cups, "label_fraude": 0, "label_3clases": classes_map.get("NORMAL", 0)})

    # Proceso en chunks
    labels_list: List[pd.DataFrame] = []
    for chunk in pd.read_csv(expedientes_csv, chunksize=200_000, low_memory=False):
        if id_col not in chunk.columns:
            continue
        labels_list.append(_label_from_chunk(chunk, id_col, cups_set, classes_map))
    if not labels_list:
        return pd.DataFrame({"cups_sgc": cups, "label_fraude": 0, "label_3clases": classes_map.get("NORMAL", 0)})

    labels = pd.concat(labels_list, ignore_index=True)
    # Normalizar ids
    if "cups_sgc" in labels.columns:
        labels["cups_sgc"] = labels["cups_sgc"].astype(str).str.upper().str.strip()
    # En caso de ids repetidos en distintos chunks, agregamos
    labels = labels.groupby("cups_sgc", as_index=False).max()
    # Asegurar cubrir todos los cups
    cups_norm = [str(c).upper().strip() for c in cups]
    full = pd.DataFrame({"cups_sgc": cups_norm}).merge(labels, on="cups_sgc", how="left").fillna(
        {"label_fraude": 0, "label_3clases": classes_map.get("NORMAL", 0)}
    )
    return full


def build_snapshot(
    as_of: Optional[str],
    bucketized_path: Optional[str],
    expedientes_csv: str,
    database: Optional[str],
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    out_dir: str = "features",
    contador_parquet: Optional[str] = "features/datos_contador_clean.parquet",
    events_lookback: int = 90,
    include_embeddings: bool = True,
    embeddings_graph_name: str = "cupsGraph",
    embeddings_dim: int = 64,
) -> Tuple[str, str, str]:
    os.makedirs(out_dir, exist_ok=True)
    as_of_str = as_of or dt.date.today().isoformat()

    # 1) Cargar features tabulares bucketizadas
    bpath = bucketized_path or _find_bucketized_csv()
    tab = _read_bucketized_features(bpath)
    tab.insert(1, "asOf", as_of_str)

    # 2) Features de grafo desde Neo4j
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

    # 3) Join tabular + grafo
    # Normalizar ids para merge con grafo
    tab["cups_sgc"] = tab["cups_sgc"].astype(str).str.upper().str.strip()
    if "cups_sgc" in gfeat.columns:
        gfeat["cups_sgc"] = gfeat["cups_sgc"].astype(str).str.upper().str.strip()
    feat = tab.merge(gfeat, on=["cups_sgc", "asOf"], how="left")
    # rellenar NaNs de grafo
    graph_cols = [c for c in feat.columns if c.startswith("vecinos_") or c.startswith("tasa_") or c.startswith("fraude_hist") or c.startswith("emb_")]
    for c in graph_cols:
        if feat[c].dtype.kind in "fbiu":
            feat[c] = feat[c].fillna(0)
        else:
            feat[c] = feat[c].fillna("")

    # 3bis) Unir features de datos de contador si existen
    if contador_parquet and os.path.exists(contador_parquet):
        try:
            cnt = pd.read_parquet(contador_parquet)
            if "cups_sgc" in cnt.columns:
                dup_cols = [c for c in cnt.columns if c in feat.columns and c != "cups_sgc"]
                if dup_cols:
                    cnt = cnt.drop(columns=dup_cols)
                feat = feat.merge(cnt, on="cups_sgc", how="left")
        except Exception as e:
            print(f"[WARN] contador features omitidas: {e}")

    # 3ter) Features de eventos y curva horaria (lookback)
    try:
        evcv = build_events_consumption_features(as_of=as_of_str, days_lookback=events_lookback)
        # Renombrar sufijos XXd -> {events_lookback}d para trazabilidad
        ren = {c: c.replace("XXd", f"{events_lookback}d") for c in evcv.columns if "XXd" in c}
        if ren:
            evcv = evcv.rename(columns=ren)
        # Normalizar ids antes del merge
        if "cups_sgc" in feat.columns:
            feat["cups_sgc"] = feat["cups_sgc"].astype(str).str.upper().str.strip()
        if "cups_sgc" in evcv.columns:
            evcv["cups_sgc"] = evcv["cups_sgc"].astype(str).str.upper().str.strip()
        feat = feat.merge(evcv, on="cups_sgc", how="left")
        # Rellenar NaNs en señales de eventos/curva
        evcv_cols = [c for c in feat.columns if isinstance(c, str) and (c.startswith("event_") or c.startswith("curva_"))]
        for c in evcv_cols:
            if feat[c].dtype.kind in "fbiu":
                feat[c] = feat[c].fillna(0)
            else:
                feat[c] = feat[c].fillna("")
    except Exception as e:
        print(f"[WARN] eventos/curva omitidas: {e}")

    # 4) Labels desde Expedientes.csv
    from config import FRAUD_CLASSES as CLASSES_MAP  # reusar configuración existente
    labels = _build_labels(expedientes_csv, feat["cups_sgc"].tolist(), CLASSES_MAP)

    # 5) Guardar
    feat_path = os.path.join(out_dir, f"features_cups_snapshot_{as_of_str}.parquet")
    labels_path = os.path.join(out_dir, f"labels_cups_snapshot_{as_of_str}.parquet")
    merged_path = os.path.join(out_dir, f"dataset_cups_snapshot_{as_of_str}.parquet")

    feat.to_parquet(feat_path, index=False)
    labels.to_parquet(labels_path, index=False)
    # Unir labels por CUPS únicamente, conservando la fecha de alta de la irregularidad
    feat.merge(labels, on=["cups_sgc"], how="left").to_parquet(merged_path, index=False)

    return feat_path, labels_path, merged_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Construye snapshot de features tabulares+grafo y labels a una fecha asOf.")
    p.add_argument("--as-of", type=str, default=None, help="Fecha de corte (YYYY-MM-DD). Por defecto hoy.")
    p.add_argument("--bucketized-path", type=str, default=None, help="Ruta del CSV bucketizado. Si no se indica, busca data/*_bucketized.csv")
    p.add_argument("--expedientes-csv", type=str, default="data/Expedientes.csv", help="Ruta del CSV de expedientes")
    p.add_argument("--database", type=str, default=None, help="Nombre de base de datos en Neo4j (opcional)")
    p.add_argument("--neo4j-uri", type=str, default=None, help="URI de Neo4j (p. ej., bolt://localhost:7687)")
    p.add_argument("--neo4j-user", type=str, default=None, help="Usuario de Neo4j (por defecto env NEO4J_USER o neo4j)")
    p.add_argument("--neo4j-password", type=str, default=None, help="Password de Neo4j (por defecto env NEO4J_PASSWORD)")
    p.add_argument("--out-dir", type=str, default="features", help="Directorio de salida para parquet")
    p.add_argument("--contador-parquet", type=str, default="features/datos_contador_clean.parquet", help="Parquet con features de datos de contador (opcional)")
    p.add_argument("--events-lookback", type=int, default=90, help="Días de ventana para eventos/curva horaria")
    p.add_argument("--no-embeddings", action="store_true", help="Desactivar extracción de embeddings GDS")
    p.add_argument("--emb-graph", type=str, default="cupsGraph", help="Nombre del grafo GDS para embeddings")
    p.add_argument("--emb-dim", type=int, default=64, help="Dimensión de embeddings fastRP")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        feat_path, labels_path, merged_path = build_snapshot(
            as_of=args.as_of,
            bucketized_path=args.bucketized_path,
            expedientes_csv=args.expedientes_csv,
            database=args.database,
            uri=args.neo4j_uri,
            user=args.neo4j_user,
            password=args.neo4j_password,
            out_dir=args.out_dir,
            contador_parquet=args.contador_parquet,
            include_embeddings=not args.no_embeddings,
            embeddings_graph_name=args.emb_graph,
            embeddings_dim=args.emb_dim,
            events_lookback=args.events_lookback,
        )
        print("Features:", feat_path)
        print("Labels:", labels_path)
        print("Dataset:", merged_path)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
