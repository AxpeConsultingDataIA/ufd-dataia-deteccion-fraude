import argparse
from typing import List

from utils.neo4j_client import get_session

def list_labels(session) -> List[str]:
    try:
        return [r["label"] for r in session.run("CALL db.labels() YIELD label RETURN label ORDER BY label")]
    except Exception:
        # Neo4j 5+: db.labels() -> SHOW LABELS
        return [r["label"] for r in session.run("SHOW LABELS YIELD name AS label RETURN label ORDER BY label")]

def list_relationship_types(session) -> List[str]:
    try:
        return [r["relationshipType"] for r in session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")]
    except Exception:
        # Neo4j 5+: SHOW RELATIONSHIP TYPES
        return [r["relationshipType"] for r in session.run("SHOW RELATIONSHIP TYPES YIELD name AS relationshipType RETURN relationshipType ORDER BY relationshipType")]

def sample_keys_for_label(session, label: str):
    try:
        rec = session.run(f"MATCH (n:`{label}`) RETURN keys(n) AS keys, count(n) AS cnt LIMIT 1").single()
        if rec:
            return rec["keys"] or [], rec["cnt"]
    except Exception:
        pass
    try:
        rec = session.run(f"MATCH (n:`{label}`) RETURN keys(n) AS keys LIMIT 1").single()
        if rec:
            return rec["keys"] or [], None
    except Exception:
        pass
    return [], None

def main():
    ap = argparse.ArgumentParser(description="Sondeo de esquema Neo4j (labels, relaciones, y claves por label).")
    ap.add_argument("--neo4j-uri", dest="uri", type=str, required=True)
    ap.add_argument("--neo4j-user", dest="user", type=str, required=True)
    ap.add_argument("--neo4j-password", dest="password", type=str, required=True)
    ap.add_argument("--database", dest="database", type=str, default=None)
    ap.add_argument("--limit-labels", type=int, default=100)
    args = ap.parse_args()

    # Listado de bases de datos desde 'system'
    print("# Databases")
    try:
        with get_session(database="system", uri=args.uri, user=args.user, password=args.password) as ssys:
            try:
                dbs = [r["name"] for r in ssys.run("SHOW DATABASES YIELD name RETURN name ORDER BY name")]
            except Exception:
                dbs = []
        print(", ".join(dbs))
    except Exception as e:
        print(f"[WARN] No se pudieron listar las bases de datos: {e}")

    with get_session(database=args.database, uri=args.uri, user=args.user, password=args.password) as s:
        print("\n# Counts")
        try:
            n_cnt = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            r_cnt = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        except Exception:
            n_cnt, r_cnt = "?", "?"
        print(f"Nodes: {n_cnt}, Relationships: {r_cnt}")

        print("\n# Labels")
        labels = list_labels(s)
        print(", ".join(labels))

        print("\n# Relationship Types")
        rels = list_relationship_types(s)
        print(", ".join(rels))

        print("\n# Keys per Label (sample)")
        for i, label in enumerate(labels[: args.limit_labels]):
            keys, cnt = sample_keys_for_label(s, label)
            cnt_str = "" if cnt is None else f" (count sample: {cnt})"
            print(f"- {label}{cnt_str}: {', '.join(keys)}")

        # Heurísticas: sugerencias para mapping
        print("\n# Heurísticas de mapeo sugeridas")
        def find_like(candidates: List[str], pool: List[str]) -> List[str]:
            out = []
            for p in pool:
                lp = p.lower()
                if any(c in lp for c in candidates):
                    out.append(p)
            return out

        supply_like = find_like(["suministro", "supply", "cups", "pod", "meterpoint", "nis"], labels)
        meter_like = find_like(["contador", "meter"], labels)
        concentrador_like = find_like(["concentrador", "concentrator"], labels)
        ubicacion_like = find_like(["ubicacion", "location", "address"], labels)
        expediente_like = find_like(["expediente", "fraud", "case", "inspection"], labels)

        print(f"- Posibles SUMINISTRO: {supply_like}")
        print(f"- Posibles CONTADOR: {meter_like}")
        print(f"- Posibles CONCENTRADOR: {concentrador_like}")
        print(f"- Posibles UBICACION: {ubicacion_like}")
        print(f"- Posibles EXPEDIENTE_FRAUDE: {expediente_like}")

if __name__ == "__main__":
    main()
