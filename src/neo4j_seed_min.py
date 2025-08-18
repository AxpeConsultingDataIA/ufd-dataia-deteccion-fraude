import argparse
import os
from typing import List, Optional
from glob import glob

import pandas as pd

from utils.neo4j_client import get_session

DEF_CONC_ID = "CNC1"
DEF_UBI_ID = "U1"

def _find_latest_topk(scoring_dir: str = "scoring") -> Optional[str]:
    files = sorted(glob(os.path.join(scoring_dir, "scoring_top*.parquet")))
    return files[-1] if files else None

def _load_cups(source_topk: Optional[str], limit: int) -> List[str]:
    cups: List[str] = []
    if source_topk and os.path.exists(source_topk):
        df = pd.read_parquet(source_topk)
        if "cups_sgc" in df.columns:
            cups = df["cups_sgc"].astype(str).dropna().unique().tolist()
    else:
        latest = _find_latest_topk()
        if latest:
            df = pd.read_parquet(latest)
            if "cups_sgc" in df.columns:
                cups = df["cups_sgc"].astype(str).dropna().unique().tolist()
    return cups[:limit]

def _ensure_constraints(session) -> None:
    cy = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:SUMINISTRO) REQUIRE s.nis_rad IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (m:CONTADOR) REQUIRE m.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:CONCENTRADOR) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (u:UBICACION) REQUIRE u.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (e:EXPEDIENTE_FRAUDE) REQUIRE e.id IS UNIQUE",
    ]
    for q in cy:
        session.run(q)

def _seed_for_cups(session, cups: List[str], conc_id: str = DEF_CONC_ID, ubi_id: str = DEF_UBI_ID) -> None:
    # Nodo comunes
    session.run(
        """
        MERGE (conc:CONCENTRADOR {id:$conc})
        MERGE (u:UBICACION {id:$ubi})
        """,
        {"conc": conc_id, "ubi": ubi_id},
    )

    # Crear nodos de suministro + contador conectados al concentrador y ubicacion
    for i, nis in enumerate(cups):
        session.run(
            """
            MERGE (s:SUMINISTRO {nis_rad:$nis})
            MERGE (m:CONTADOR {id:$mid})
            MERGE (m)-[:MIDE_CONSUMO_DE]->(s)
            MERGE (conc:CONCENTRADOR {id:$conc})
            MERGE (m)-[:CONECTADO_A]->(conc)
            MERGE (u:UBICACION {id:$ubi})
            MERGE (m)-[:INSTALADO_EN]->(u)
            """,
            {"nis": nis, "mid": f"M_{nis}", "conc": conc_id, "ubi": ubi_id},
        )

    # Crear un vecino con fraude histórico colgando del mismo concentrador (para inducir tasa>0)
    # Si ya existe, no duplica (MERGE)
    if cups:
        fraud_nis = f"{cups[0]}_NEIGH_FRAUD"
        session.run(
            """
            MERGE (sfx:SUMINISTRO {nis_rad:$fnis})
            MERGE (mf:CONTADOR {id:$mfid})
            MERGE (mf)-[:MIDE_CONSUMO_DE]->(sfx)
            MERGE (conc:CONCENTRADOR {id:$conc})
            MERGE (mf)-[:CONECTADO_A]->(conc)
            MERGE (exp:EXPEDIENTE_FRAUDE {id:$expid})
            SET exp.fecha_acta = date($acta)
            MERGE (mf)-[:INVOLUCRADO_EN_FRAUDE]->(exp)
            """,
            {
                "fnis": fraud_nis,
                "mfid": f"M_{fraud_nis}",
                "conc": conc_id,
                "expid": f"EXP_{fraud_nis}",
                "acta": "2024-10-10",
            },
        )

def _counts(session) -> tuple[int, int]:
    try:
        n_cnt = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        r_cnt = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        return int(n_cnt), int(r_cnt)
    except Exception:
        return -1, -1

def main():
    ap = argparse.ArgumentParser(description="Seed mínimo de grafo para validar pipeline de features Neo4j.")
    ap.add_argument("--neo4j-uri", dest="uri", type=str, required=True)
    ap.add_argument("--neo4j-user", dest="user", type=str, required=True)
    ap.add_argument("--neo4j-password", dest="password", type=str, required=True)
    ap.add_argument("--database", dest="database", type=str, default=None)
    ap.add_argument("--from-topk", dest="from_topk", type=str, default=None, help="Ruta a scoring_top*.parquet para tomar CUPS reales")
    ap.add_argument("--limit", type=int, default=5, help="Número de CUPS a poblar")
    args = ap.parse_args()

    cups = _load_cups(args.from_topk, args.limit)
    if not cups:
        # Fallback a valores dummy si no hay scoring disponible
        cups = [f"ES_FAKE_{i+1}" for i in range(args.limit)]

    with get_session(database=args.database, uri=args.uri, user=args.user, password=args.password) as s:
        _ensure_constraints(s)
        _seed_for_cups(s, cups)
        n_cnt, r_cnt = _counts(s)
        print(f"Seed completado. Nodes: {n_cnt}, Relationships: {r_cnt}. Ejemplos CUPS: {cups[:3]}")

if __name__ == "__main__":
    main()
