import os
from contextlib import contextmanager

def _require_neo4j():
    try:
        from neo4j import GraphDatabase  # type: ignore
    except Exception as e:
        raise ImportError(
            "Missing dependency 'neo4j'. Install with: pip install neo4j"
        ) from e
    return GraphDatabase

def get_driver(uri: str | None = None, user: str | None = None, password: str | None = None):
    """
    Returns a Neo4j driver using env vars if args are not provided.
    Env:
      - NEO4J_URI (default bolt://localhost:7687)
      - NEO4J_USER (default neo4j)
      - NEO4J_PASSWORD (required)
    """
    GraphDatabase = _require_neo4j()

    uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = user or os.getenv("NEO4J_USER", "neo4j")
    password = password or os.getenv("NEO4J_PASSWORD")

    if not password:
        raise ValueError("NEO4J_PASSWORD not set. Export NEO4J_PASSWORD to connect.")

    # Keep connection settings conservative/safe
    return GraphDatabase.driver(
        uri,
        auth=(user, password),
        max_connection_lifetime=60,
        max_connection_pool_size=20,
        connection_acquisition_timeout=30,
    )

@contextmanager
def get_session(database: str | None = None, uri: str | None = None, user: str | None = None, password: str | None = None):
    """
    Context manager that yields a Neo4j session and closes it safely.
    Allows overriding connection via parameters; falls back to env vars.
    """
    driver = get_driver(uri=uri, user=user, password=password)
    try:
        if database:
            with driver.session(database=database) as session:
                yield session
        else:
            with driver.session() as session:
                yield session
    finally:
        driver.close()

def test_connection(database: str | None = None, uri: str | None = None, user: str | None = None, password: str | None = None) -> dict:
    """
    Simple connectivity check. Returns Neo4j version and server agent.
    """
    with get_session(database=database, uri=uri, user=user, password=password) as s:
        rec = s.run("RETURN 1 AS ok").single()
        if not rec or rec["ok"] != 1:
            raise RuntimeError("Neo4j connectivity test failed.")
        # Try to fetch basic info (best-effort)
        try:
            info = s.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0] AS version, edition").single()
            return {"name": info["name"], "version": info["version"], "edition": info["edition"]}
        except Exception:
            return {"name": "unknown", "version": "unknown", "edition": "unknown"}
