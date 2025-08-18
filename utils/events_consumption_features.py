import os
import re
import datetime as dt
from glob import glob
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

DATA_BASE = "src/feature_step/datasets"

# ------------ Maestro de eventos ------------- #
def _load_maestro(path_glob: str = os.path.join(DATA_BASE, "grupo_eventos_codigos", "*.parquet")) -> pd.DataFrame:
    files = sorted(glob(path_glob))
    if not files:
        return pd.DataFrame(columns=["grupo", "numero", "desc_evento", "grupo_int", "numero_int"])
    df = pd.read_parquet(files[0])
    # Normaliza columnas esperadas
    for col in ["grupo", "numero", "desc_evento"]:
        if col not in df.columns:
            df[col] = None
    # Parseo de grupo: puede tener "1, 2, 3, 4, 5 y 6"
    def split_grupo(g: str) -> List[int]:
        if not isinstance(g, str):
            return []
        # quedarse con números
        nums = re.findall(r"\d+", g)
        return [int(x) for x in nums]
    df["numero_int"] = pd.to_numeric(df["numero"], errors="coerce").astype("Int64")
    df["grupos_list"] = df["grupo"].apply(split_grupo)
    df = df.explode("grupos_list").rename(columns={"grupos_list": "grupo_int"})
    df["grupo_int"] = pd.to_numeric(df["grupo_int"], errors="coerce").astype("Int64")
    # Dejar sólo filas con clave completa
    df = df.dropna(subset=["grupo_int", "numero_int"])
    return df[["grupo_int", "numero_int", "desc_evento"]].drop_duplicates()


# ------------ Grid contadores (mapas a CUPS) ------------- #
def _load_grid(path_glob: str = os.path.join(DATA_BASE, "grid_contadores", "*.parquet")) -> pd.DataFrame:
    files = sorted(glob(path_glob))
    if not files:
        return pd.DataFrame(columns=["cups_sgc", "cnt_key", "cnc_key"])
    df = pd.read_parquet(files[0])
    # Posibles columnas útiles
    cups_col = None
    for cand in ["cups_sgc", "CUPS", "cups", "cups_id"]:
        if cand in df.columns:
            cups_col = cand
            break
    if cups_col is None:
        cups_col = "cups_sgc"
        df[cups_col] = None
    # Claves de contador y de concentrador
    # La conexión oficial: cnt_id (eventos/curva) = cnt_sgc (grid)
    cnt_keys = ["cnt_sgc"] if "cnt_sgc" in df.columns else []
    cnc_keys = [c for c in ["cnc_s05", "cnc_id", "concentrador_id"] if c in df.columns]
    rows = []
    for _, r in df.iterrows():
        cups = r.get(cups_col, None)
        cnc_key = None
        for ck in cnc_keys:
            val = r.get(ck, None)
            if pd.notna(val):
                cnc_key = str(val)
                break
        # contadores (conservar data_date para resolver duplicados por cnt_key)
        any_cnt = False
        for ck in cnt_keys:
            val = r.get(ck, None)
            if pd.notna(val):
                rows.append({
                    "cups_sgc": str(cups) if pd.notna(cups) else None,
                    "cnt_key": str(val),
                    "cnc_key": cnc_key,
                    "data_date": r.get("data_date", None),
                })
                any_cnt = True
        if not any_cnt:
            # al menos fila por concentrador si hay CUPS único (respaldo)
            rows.append({
                "cups_sgc": str(cups) if pd.notna(cups) else None,
                "cnt_key": None,
                "cnc_key": cnc_key,
                "data_date": r.get("data_date", None),
            })
    map_df = pd.DataFrame(rows)
    # Resolver duplicados por cnt_key quedándose con el más reciente
    if "data_date" in map_df.columns:
        map_df["data_date"] = pd.to_datetime(map_df["data_date"], errors="coerce")
        map_df = map_df.sort_values(["cnt_key", "data_date"]).drop_duplicates(subset=["cnt_key"], keep="last")
    # Eliminar filas sin identificador utilizable y columnas auxiliares
    map_df = map_df[(map_df["cups_sgc"].notna()) & (map_df["cups_sgc"] != "None")]
    if "data_date" in map_df.columns:
        map_df = map_df.drop(columns=["data_date"])
    map_df = map_df.drop_duplicates()
    return map_df


# ------------ Carga de eventos (logs) ------------- #
def _iter_eventos_paths(base: str, as_of: dt.date, days: int) -> List[str]:
    # Particionado: .../eventos/partition_0=YYYY/partition_1=MM/partition_2=DD/part-*.parquet
    start = as_of - dt.timedelta(days=days)
    paths = []
    for y in range(start.year, as_of.year + 1):
        for m in range(1, 13):
            # dentro del rango
            for d in range(1, 32):
                try:
                    date = dt.date(y, m, d)
                except ValueError:
                    continue
                if start <= date <= as_of:
                    p = os.path.join(
                        base,
                        f"partition_0={y}",
                        f"partition_1={m}",
                        f"partition_2={d}",
                        "*.parquet",
                    )
                    paths.extend(glob(p))
    return sorted(paths)


def _load_eventos(as_of: str, days_lookback: int = 90) -> pd.DataFrame:
    as_of_date = pd.to_datetime(as_of).date()
    base = os.path.join(DATA_BASE, "eventos")
    files = _iter_eventos_paths(base, as_of_date, days_lookback)
    if not files:
        return pd.DataFrame(columns=["cnt_id", "cnc_id", "et", "c", "data_date"])
    use_cols = ["data_date", "fh", "cnc_id", "cnt_id", "et", "c"]
    dfs = []
    for f in files[:500]:  # límite de seguridad
        try:
            df = pd.read_parquet(f)
            keep = [c for c in use_cols if c in df.columns]
            df = df[keep]
            for col in use_cols:
                if col not in df.columns:
                    df[col] = pd.NA
            dfs.append(df[use_cols])
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame(columns=use_cols)
    ev = pd.concat(dfs, ignore_index=True)
    # Tipos
    ev["et"] = pd.to_numeric(ev["et"], errors="coerce").astype("Int64")
    ev["c"] = pd.to_numeric(ev["c"], errors="coerce").astype("Int64")
    ev["data_date"] = pd.to_datetime(ev["data_date"], errors="coerce")
    if "fh" in ev.columns:
        ev["fh"] = pd.to_datetime(ev["fh"], errors="coerce")
    # Fecha de evento (día): usar fh si existe, si no data_date
    ev["event_date"] = (
        pd.to_datetime(ev.get("fh"), errors="coerce")
        .fillna(pd.to_datetime(ev.get("data_date"), errors="coerce"))
        .dt.floor("D")
    )
    # Drop nulos claves
    ev = ev.dropna(subset=["et", "c"])
    return ev


# ------------ Carga de curva horaria ------------- #
def _iter_curva_paths(base: str, as_of: dt.date, days: int) -> List[str]:
    return _iter_eventos_paths(base, as_of, days)


def _load_curva(as_of: str, days_lookback: int = 90) -> pd.DataFrame:
    as_of_date = pd.to_datetime(as_of).date()
    base = os.path.join(DATA_BASE, "curva_horaria")
    files = _iter_curva_paths(base, as_of_date, days_lookback)
    if not files:
        return pd.DataFrame(columns=["cnt_id", "cnc_id", "ai", "ae", "r1", "r2", "r3", "r4", "data_date"])
    use_cols = ["data_date", "fh", "cnc_id", "cnt_id", "ai", "ae", "r1", "r2", "r3", "r4"]
    dfs = []
    for f in files[:500]:
        try:
            df = pd.read_parquet(f)
            keep = [c for c in use_cols if c in df.columns]
            df = df[keep]
            for col in use_cols:
                if col not in df.columns:
                    df[col] = pd.NA
            dfs.append(df[use_cols])
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame(columns=use_cols)
    cv = pd.concat(dfs, ignore_index=True)
    # Tipos
    for c in ["ai", "ae", "r1", "r2", "r3", "r4"]:
        cv[c] = pd.to_numeric(cv[c], errors="coerce")
    cv["data_date"] = pd.to_datetime(cv["data_date"], errors="coerce")
    if "fh" in cv.columns:
        cv["fh"] = pd.to_datetime(cv["fh"], errors="coerce")
    return cv


# ------------ Feature engineering ------------- #
def _join_ev_with_maestro(ev: pd.DataFrame, maestro: pd.DataFrame) -> pd.DataFrame:
    if ev.empty or maestro.empty:
        ev["desc_evento"] = pd.NA
        return ev
    out = ev.merge(
        maestro.rename(columns={"grupo_int": "et", "numero_int": "c"}),
        on=["et", "c"],
        how="left",
    )
    return out


def _map_to_cups(df: pd.DataFrame, grid: pd.DataFrame) -> pd.DataFrame:
    if df.empty or grid.empty:
        df["cups_sgc"] = pd.NA
        return df

    # Normalizar tipos para joins
    for c in ["cnt_id", "cnc_id"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    for c in ["cnt_key", "cnc_key"]:
        if c in grid.columns:
            grid[c] = grid[c].astype(str)

    # Intento 1: por contador
    if "cnt_id" in df.columns:
        m1 = df.merge(
            grid.dropna(subset=["cnt_key"])[["cnt_key", "cups_sgc"]],
            left_on="cnt_id",
            right_on="cnt_key",
            how="left",
        )
        if "cnt_key" in m1.columns:
            m1 = m1.drop(columns=["cnt_key"])
    else:
        m1 = df.copy()

    # Asegurar existencia de columna cups_sgc
    if "cups_sgc" not in m1.columns:
        m1["cups_sgc"] = pd.NA

    # Intento 2: por concentrador si no hay cnt o no hubo match
    if "cnc_id" in m1.columns and "cnc_key" in grid.columns:
        cnc_map = grid.dropna(subset=["cnc_key"])[["cnc_key", "cups_sgc"]].drop_duplicates()
        mask = m1["cups_sgc"].isna()
        if mask.any():
            m2 = m1.loc[mask].merge(
                cnc_map,
                left_on="cnc_id",
                right_on="cnc_key",
                how="left",
            )
            # Consolidar posibles sufijos
            if "cups_sgc_y" in m2.columns and "cups_sgc" not in m2.columns:
                m2 = m2.rename(columns={"cups_sgc_y": "cups_sgc"})
            m1.loc[mask, "cups_sgc"] = m2.get("cups_sgc", pd.Series([pd.NA] * len(m2))).values

    # Normalizar CUPS final
    m1["cups_sgc"] = m1["cups_sgc"].astype(str).str.upper().str.strip()
    m1.loc[m1["cups_sgc"].isin(["", "NONE", "NAN"]), "cups_sgc"] = pd.NA
    return m1


def _flag_suspicious(desc: pd.Series) -> pd.Series:
    # Heurística: palabras indicativas
    kw = [
        "fraud", "fraude", "manipul", "iman", "imán", "puerta", "tapa", "sello", "apertura",
        "tamper", "intrus", "bypass", "enganche", "enganche ilegal", "anomal", "burla"
    ]
    desc = desc.fillna("").astype(str).str.lower()
    patt = re.compile("|".join([re.escape(k) for k in kw]))
    return desc.str.contains(patt)


def build_events_consumption_features(
    as_of: str,
    days_lookback: int = 90,
    maestro_glob: Optional[str] = None,
    grid_glob: Optional[str] = None,
) -> pd.DataFrame:
    """
    Devuelve un DataFrame por CUPS con:
      - event_count_XXd, event_suspicious_XXd
      - event_by_group_g{et}_XXd (para grupos ET más frecuentes)
      - curva_ai_sum_XXd, curva_ae_sum_XXd, curva_hours_XXd, curva_zero_hours_%_XXd
      - Métricas de curva ALINEADAS A EVENTOS por (cups_sgc, data_date):
        curva_ai_sum_on_event_days_XXd, curva_ae_sum_on_event_days_XXd,
        curva_hours_on_event_days_XXd, curva_zero_hours_on_event_days_XXd,
        curva_zero_hours_pct_on_event_days_XXd
    """
    maestro = _load_maestro(maestro_glob or os.path.join(DATA_BASE, "grupo_eventos_codigos", "*.parquet"))
    grid = _load_grid(grid_glob or os.path.join(DATA_BASE, "grid_contadores", "*.parquet"))

    # Eventos
    ev = _load_eventos(as_of=as_of, days_lookback=days_lookback)
    ev = _join_ev_with_maestro(ev, maestro)
    ev = _map_to_cups(ev, grid)
    ev = ev.dropna(subset=["cups_sgc"])

    # Agregados eventos por CUPS
    if not ev.empty:
        ev["is_susp"] = _flag_suspicious(ev["desc_evento"]).astype(int)
        ev_grp = (
            ev.groupby("cups_sgc")
            .agg(event_count_XXd=("c", "count"), event_suspicious_XXd=("is_susp", "sum"))
            .reset_index()
        )
        # Por grupo ET más frecuentes
        top_groups = (
            ev.groupby("et").size().sort_values(ascending=False).head(5).index.tolist()
            if "et" in ev.columns and not ev["et"].isna().all()
            else []
        )
        ev_by_group = []
        for g in top_groups:
            tmp = ev.loc[ev["et"] == g].groupby("cups_sgc").size().rename(f"event_g{int(g)}_XXd").reset_index()
            ev_by_group.append(tmp)
        if ev_by_group:
            ev_grp = ev_grp.merge(
                pd.concat([x.set_index("cups_sgc") for x in ev_by_group], axis=1).reset_index(),
                on="cups_sgc",
                how="left",
            )
    else:
        ev_grp = pd.DataFrame(columns=["cups_sgc", "event_count_XXd", "event_suspicious_XXd"])

    # Curva horaria
    cv = _load_curva(as_of=as_of, days_lookback=days_lookback)
    cv = _map_to_cups(cv, grid)
    cv = cv.dropna(subset=["cups_sgc"])
    if not cv.empty:
        # horas totales por cups en toda la ventana
        cv["zero_hour"] = ((cv["ai"].fillna(0) == 0) & (cv["ae"].fillna(0) == 0)).astype(int)
        agg = cv.groupby("cups_sgc").agg(
            curva_ai_sum_XXd=("ai", "sum"),
            curva_ae_sum_XXd=("ae", "sum"),
            curva_hours_XXd=("ai", "size"),
            curva_zero_hours_XXd=("zero_hour", "sum"),
        )
        agg["curva_zero_hours_pct_XXd"] = (agg["curva_zero_hours_XXd"] / agg["curva_hours_XXd"]).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        cv_grp = agg.reset_index()

        # Agregados diarios de curva por (cnt_id, día de fh)
        cv_day_cnt = (
            cv.assign(
                cv_day=(
                    pd.to_datetime(cv.get("fh"), errors="coerce")
                    .fillna(pd.to_datetime(cv.get("data_date"), errors="coerce"))
                    .dt.floor("D")
                )
            )
            .dropna(subset=["cnt_id", "cv_day"])
            .groupby(["cnt_id", "cv_day"])
            .agg(
                curva_ai_sum_day=("ai", "sum"),
                curva_ae_sum_day=("ae", "sum"),
                curva_hours_day=("ai", "size"),
                curva_zero_hours_day=("zero_hour", "sum"),
            )
            .reset_index()
            .rename(columns={"cv_day": "event_date"})
        )

        # Alineación por fecha de evento y contador: join por (cnt_id, event_date)
        if not ev.empty:
            if "event_date" not in ev.columns:
                ev["event_date"] = (
                    pd.to_datetime(ev["data_date"], errors="coerce").dt.floor("D")
                    if "data_date" in ev.columns
                    else pd.NaT
                )
            ev_day_cnt = (
                ev.dropna(subset=["cnt_id", "event_date"])
                  .groupby(["cnt_id", "event_date"])
                  .size()
                  .reset_index(name="events_day")
            )
            on_ev = ev_day_cnt.merge(cv_day_cnt, on=["cnt_id", "event_date"], how="inner")
            if not on_ev.empty:
                # Mapear a CUPS vía grid (cnt_id = cnt_sgc)
                on_ev = on_ev.merge(
                    grid[["cnt_key", "cups_sgc"]].dropna().drop_duplicates(),
                    left_on="cnt_id",
                    right_on="cnt_key",
                    how="left",
                ).drop(columns=["cnt_key"], errors="ignore")
                on_ev_agg = (
                    on_ev.dropna(subset=["cups_sgc"])
                        .groupby("cups_sgc")
                        .agg(
                            curva_ai_sum_on_event_days_XXd=("curva_ai_sum_day", "sum"),
                            curva_ae_sum_on_event_days_XXd=("curva_ae_sum_day", "sum"),
                            curva_hours_on_event_days_XXd=("curva_hours_day", "sum"),
                            curva_zero_hours_on_event_days_XXd=("curva_zero_hours_day", "sum"),
                        )
                        .reset_index()
                )
                on_ev_agg["curva_zero_hours_pct_on_event_days_XXd"] = (
                    on_ev_agg["curva_zero_hours_on_event_days_XXd"] / on_ev_agg["curva_hours_on_event_days_XXd"]
                ).replace([np.inf, -np.inf], 0.0).fillna(0.0)
                cv_grp = cv_grp.merge(on_ev_agg, on="cups_sgc", how="left")
    else:
        cv_grp = pd.DataFrame(
            columns=[
                "cups_sgc",
                "curva_ai_sum_XXd",
                "curva_ae_sum_XXd",
                "curva_hours_XXd",
                "curva_zero_hours_XXd",
                "curva_zero_hours_pct_XXd",
                "curva_ai_sum_on_event_days_XXd",
                "curva_ae_sum_on_event_days_XXd",
                "curva_hours_on_event_days_XXd",
                "curva_zero_hours_on_event_days_XXd",
                "curva_zero_hours_pct_on_event_days_XXd",
            ]
        )

    # Merge final
    out = pd.merge(ev_grp, cv_grp, on="cups_sgc", how="outer")
    # Rellenos
    for c in out.columns:
        if c == "cups_sgc":
            continue
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].fillna(0)
        else:
            out[c] = out[c].fillna("")
    return out


if __name__ == "__main__":
    # Ejecución de prueba
    as_of = dt.date.today().isoformat()
    df = build_events_consumption_features(as_of=as_of, days_lookback=90)
    print("Rows:", len(df), "Cols:", len(df.columns))
    print(df.head(5).to_string(index=False))
