import os
import pandas as pd
from datetime import date

from utils.events_consumption_features import (
    _load_eventos,
    _load_curva,
    _load_grid,
)

def _norm_cups(s: pd.Series) -> pd.Series:
    return s.astype(str).str.upper().str.strip()

def check_partitioned(as_of: str = "2024-11-05", days: int = 90):
    grid = _load_grid()
    # Eventos particionados
    ev = _load_eventos(as_of=as_of, days_lookback=days)
    if ev.empty:
        print("[partitioned] eventos: 0 filas en ventana")
        return
    ev["cups_sgc"] = _norm_cups(ev.get("cups_sgc", pd.Series(index=ev.index, dtype=str)))
    # Si no existe cups_sgc en eventos, no podemos alinear por CUPS aquí (se asume mapeo previo en el pipeline)
    if "cups_sgc" not in ev.columns or ev["cups_sgc"].isna().all():
        print("[partitioned] eventos sin cups_sgc. Necesario mapeo previo cnt/cnc->CUPS en el pipeline.")
        return
    ev_day = (
        ev.dropna(subset=["cups_sgc", "data_date"])
          .groupby(["cups_sgc", "data_date"])
          .size()
          .reset_index(name="events_day")
    )

    # Curva particionada
    cv = _load_curva(as_of=as_of, days_lookback=days)
    if cv.empty:
        print("[partitioned] curva_horaria: 0 filas en ventana")
        return
    cv["cups_sgc"] = _norm_cups(cv.get("cups_sgc", pd.Series(index=cv.index, dtype=str)))
    cv["zero_hour"] = ((cv["ai"].fillna(0) == 0) & (cv["ae"].fillna(0) == 0)).astype(int)
    cv_day = (
        cv.dropna(subset=["cups_sgc", "data_date"])
          .groupby(["cups_sgc", "data_date"])
          .agg(ai_sum=("ai", "sum"), ae_sum=("ae", "sum"), hours=("ai", "size"), zero=("zero_hour", "sum"))
          .reset_index()
    )

    joined = ev_day.merge(cv_day, on=["cups_sgc", "data_date"], how="inner")
    print("[partitioned] as_of:", as_of, "days:", days)
    print("  eventos unique (cups,date):", len(ev_day))
    print("  curva   unique (cups,date):", len(cv_day))
    print("  joined  (inner)           :", len(joined))
    if len(ev_day):
        print("  coverage eventos matched  :", round(len(joined) / len(ev_day), 3))
    print("  sample joined rows:")
    print(joined.head(5).to_string(index=False))

def check_csv_events():
    path = "data/datos_eventos_enero_a_julio_2025.csv"
    if not os.path.exists(path):
        print("[csv] eventos CSV no encontrado:", path)
        return
    ev = pd.read_csv(path, dtype=str)
    if not {"cups_sgc", "fecha_hora_evento"}.issubset(ev.columns):
        print("[csv] columnas requeridas faltan en CSV")
        return
    ev["cups_sgc"] = _norm_cups(ev["cups_sgc"])
    ev["data_date"] = pd.to_datetime(ev["fecha_hora_evento"], errors="coerce").dt.floor("D")
    ev = ev.dropna(subset=["cups_sgc", "data_date"])
    if ev.empty:
        print("[csv] eventos: 0 filas válidas")
        return
    min_d = ev["data_date"].min().date()
    max_d = ev["data_date"].max().date()
    days = (max_d - min_d).days + 1
    as_of = max_d.isoformat()

    cv = _load_curva(as_of=as_of, days_lookback=max(1, min(days, 120)))
    if cv.empty:
        print("[csv] curva_horaria: 0 filas en ventana", as_of, "days", days)
        return
    # En CSV ya viene cups_sgc; en curva asumimos que ya está mapeado a cups_sgc en el pipeline
    cv["cups_sgc"] = _norm_cups(cv.get("cups_sgc", pd.Series(index=cv.index, dtype=str)))
    cv["zero_hour"] = ((cv["ai"].fillna(0) == 0) & (cv["ae"].fillna(0) == 0)).astype(int)
    cv_day = (
        cv.dropna(subset=["cups_sgc", "data_date"])
          .groupby(["cups_sgc", "data_date"])
          .agg(ai_sum=("ai", "sum"), ae_sum=("ae", "sum"), hours=("ai", "size"), zero=("zero_hour", "sum"))
          .reset_index()
    )

    ev_day = ev.groupby(["cups_sgc", "data_date"]).size().reset_index(name="events_day")
    joined = ev_day.merge(cv_day, on=["cups_sgc", "data_date"], how="inner")
    print("[csv] ventana:", min_d, "->", max_d, "as_of:", as_of, "days:", days)
    print("  eventos unique (cups,date):", len(ev_day))
    print("  curva   unique (cups,date):", len(cv_day))
    print("  joined  (inner)           :", len(joined))
    if len(ev_day):
        print("  coverage eventos matched  :", round(len(joined) / len(ev_day), 3))
    print("  sample joined rows:")
    print(joined.head(5).to_string(index=False))

def main():
    print("== Check particionado (joins por cups_sgc y data_date) ==")
    check_partitioned("2024-11-05", 90)
    print("\n== Check CSV de eventos (joins por cups_sgc y data_date) ==")
    check_csv_events()

if __name__ == "__main__":
    main()
