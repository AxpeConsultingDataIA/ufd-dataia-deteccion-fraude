from __future__ import annotations

import datetime as dt
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from utils.neo4j_client import get_session


def _normalize_asof(as_of: Optional[str | dt.date]) -> str:
    if isinstance(as_of, dt.date):
        return as_of.isoformat()
    if isinstance(as_of, str) and as_of:
        return as_of
    return dt.date.today().isoformat()


def neighbors_fraud_rate(
    nis_list: Iterable[str],
    as_of: Optional[str | dt.date] = None,
    database: Optional[str] = None,
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calcula, para cada SUMINISTRO (nis_rad), la tasa de fraude en su vecindario
    definido por:
      - Compartir CONCENTRADOR (vía CONTADOR)
      - Compartir UBICACION (vía CONTADOR)

    Parámetros
    - nis_list: lista de nis_rad a evaluar (equivalente a cups_sgc)
    - as_of: fecha de corte ISO (YYYY-MM-DD). Solo se considera fraude con fecha_acta <= as_of.
    - database: nombre de BD en Neo4j si procede.

    Retorna DataFrame con columnas:
      - nis_rad
      - vecinos_total
      - vecinos_fraude
      - tasa_fraude_vec
      - fraude_hist (1 si el propio suministro tuvo fraude histórico <= as_of)
    """
    nis_list = list({n for n in nis_list if n and isinstance(n, str)})
    if not nis_list:
        return pd.DataFrame(columns=["nis_rad", "vecinos_total", "vecinos_fraude", "tasa_fraude_vec", "fraude_hist"])

    as_of_str = _normalize_asof(as_of)

    cypher = """
    UNWIND $nis_list AS nis
    // Punto de partida: suministro y su contador
    MATCH (s:SUMINISTRO {nis_rad: nis})<-[:MIDE_CONSUMO_DE]-(c:CONTADOR)

    // Vecinos por concentrador
    OPTIONAL MATCH (c)-[:CONECTADO_A]->(conc:CONCENTRADOR)<-[:CONECTADO_A]-(c2:CONTADOR)-[:MIDE_CONSUMO_DE]->(v1:SUMINISTRO)

    // Vecinos por ubicación
    OPTIONAL MATCH (c)-[:INSTALADO_EN]->(u:UBICACION)<-[:INSTALADO_EN]-(c3:CONTADOR)-[:MIDE_CONSUMO_DE]->(v2:SUMINISTRO)

    WITH nis, collect(DISTINCT v1) + collect(DISTINCT v2) AS vs
    UNWIND vs AS v
    WITH nis, collect(DISTINCT v) AS vecinos
    WITH nis, vecinos, size(vecinos) AS vecinos_total
    UNWIND vecinos AS v
    OPTIONAL MATCH (v)<-[:MIDE_CONSUMO_DE]-(cv:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
    WHERE e.fecha_acta IS NULL OR e.fecha_acta <= date($asOf)
    WITH nis, vecinos_total, count(DISTINCT CASE WHEN e IS NOT NULL THEN v END) AS vecinos_fraude
    // fraude histórico propio
    OPTIONAL MATCH (s2:SUMINISTRO {nis_rad: nis})<-[:MIDE_CONSUMO_DE]-(cself:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(es:EXPEDIENTE_FRAUDE)
    WHERE es.fecha_acta IS NULL OR es.fecha_acta <= date($asOf)
    WITH nis, vecinos_total, vecinos_fraude, (COUNT(es) > 0) AS self_fraude
    RETURN
      nis AS nis_rad,
      vecinos_total,
      vecinos_fraude,
      CASE WHEN vecinos_total = 0 THEN 0.0 ELSE toFloat(vecinos_fraude)/toFloat(vecinos_total) END AS tasa_fraude_vec,
      CASE WHEN self_fraude THEN 1 ELSE 0 END AS fraude_hist
    """

    with get_session(database=database, uri=uri, user=user, password=password) as s:
        recs = s.run(cypher, parameters={"nis_list": nis_list, "asOf": as_of_str}).data()

    df = pd.DataFrame.from_records(recs)
    if df.empty:
        df = pd.DataFrame(
            {
                "nis_rad": nis_list,
                "vecinos_total": [0] * len(nis_list),
                "vecinos_fraude": [0] * len(nis_list),
                "tasa_fraude_vec": [0.0] * len(nis_list),
                "fraude_hist": [0] * len(nis_list),
            }
        )
    return df


def fastRP_embeddings(
    database: Optional[str] = None,
    graph_name: str = "cupsGraph",
    dim: int = 64,
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extrae embeddings fastRP para nodos SUMINISTRO si existe un grafo GDS llamado graph_name.
    Devuelve DataFrame con columnas: nis_rad y emb_0..emb_{dim-1}.
    Si el grafo no existe o GDS no está disponible, devuelve DataFrame vacío.
    """
    # Verificar existencia del grafo
    q_exists = "CALL gds.graph.exists($g) YIELD exists RETURN exists"
    try:
        with get_session(database=database, uri=uri, user=user, password=password) as s:
            exists = s.run(q_exists, parameters={"g": graph_name}).single()
            if not exists or not exists["exists"]:
                return pd.DataFrame()
    except Exception:
        # GDS no instalado o sin permiso
        return pd.DataFrame()

    # Obtener embeddings
    q_embed = f"""
    CALL gds.fastRP.stream($g, {{ embeddingDimension: $dim }})
    YIELD nodeId, embedding
    WITH gds.util.asNode(nodeId) AS n, embedding
    WHERE n:SUMINISTRO AND n.nis_rad IS NOT NULL
    RETURN n.nis_rad AS nis_rad, embedding AS emb
    """
    with get_session(database=database, uri=uri, user=user, password=password) as s:
        rows = s.run(q_embed, parameters={"g": graph_name, "dim": dim}).data()

    if not rows:
        return pd.DataFrame()

    # Expandir vector a columnas emb_*
    df = pd.DataFrame(rows)
    emb_df = pd.DataFrame(df["emb"].tolist(), index=df.index)
    emb_df.columns = [f"emb_{i}" for i in range(emb_df.shape[1])]
    out = pd.concat([df[["nis_rad"]], emb_df], axis=1)
    return out


def graph_features_for_cups(
    cups_series: Iterable[str],
    as_of: Optional[str | dt.date] = None,
    database: Optional[str] = None,
    include_embeddings: bool = True,
    embeddings_graph_name: str = "cupsGraph",
    embeddings_dim: int = 64,
    uri: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """
    Wrapper principal para extraer features de grafo para una lista de CUPS (asumidos = nis_rad).
    Devuelve DataFrame con:
      - nis_rad (igual a cups_sgc)
      - asOf
      - vecinos_total, vecinos_fraude, tasa_fraude_vec, fraude_hist
      - (opcional) columnas emb_*
    """
    nis_list = [c for c in pd.Series(list(cups_series)).dropna().astype(str).unique().tolist() if c]
    as_of_str = _normalize_asof(as_of)

    base = neighbors_fraud_rate(nis_list, as_of=as_of_str, database=database, uri=uri, user=user, password=password)
    base.insert(1, "asOf", as_of_str)

    if include_embeddings:
        try:
            emb = fastRP_embeddings(database=database, graph_name=embeddings_graph_name, dim=embeddings_dim, uri=uri, user=user, password=password)
        except Exception:
            emb = pd.DataFrame()
        if not emb.empty:
            base = base.merge(emb, on="nis_rad", how="left")

    # Renombrar nis_rad -> cups_sgc para facilitar el join con tabular
    base = base.rename(columns={"nis_rad": "cups_sgc"})
    return base
