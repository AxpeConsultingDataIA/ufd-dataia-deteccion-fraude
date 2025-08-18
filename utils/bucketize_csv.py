import pandas as pd
import numpy as np
import re

# Ruta del archivo original y de salida
input_csv = 'data/5ab50c64-8a37-45b8-8c12-ddf42f75c02a.csv'
output_csv = 'data/5ab50c64-8a37-45b8-8c12-ddf42f75c02a_bucketized.csv'

df = pd.read_csv(input_csv, skip_blank_lines=True)
df = df.dropna(axis=1, how='all')

# Sumar columnas eventos_grupo4_tipoX por periodo
grupo4_cols = [col for col in df.columns if col.startswith('eventos_grupo4_tipo')]
periodos = set()
for col in grupo4_cols:
    m = re.match(r'eventos_grupo4_tipo\d+(_.*)', col)
    if m:
        periodos.add(m.group(1))
for periodo in periodos:
    cols_periodo = [col for col in grupo4_cols if col.endswith(periodo)]
    df[f'eventos_grupo4_total{periodo}'] = df[cols_periodo].sum(axis=1)

df_grouped = df.groupby('cups_sgc', as_index=False).mean(numeric_only=True)
non_numeric_cols = [col for col in df.columns if col not in df_grouped.columns and col != 'cups_sgc']
for col in non_numeric_cols:
    df_grouped[col] = df.groupby('cups_sgc')[col].first().values

cols_to_bucket = [col for col in df_grouped.columns if col != 'cups_sgc']

result_dict = {'cups_sgc': df_grouped['cups_sgc']}

def get_qcut_bins(data, max_bins=10):
    # Eliminar NaN e infinitos antes de calcular bins
    clean_data = data.replace([np.inf, -np.inf], np.nan).dropna()
    n_bins = min(max_bins, len(np.unique(clean_data)))
    while n_bins > 1:
        try:
            buckets, bins = pd.qcut(clean_data, n_bins, retbins=True, duplicates='drop')
            # Reemplazar inf en bins por valores finitos
            bins = np.where(np.isinf(bins), np.nan, bins)
            bins = pd.Series(bins).fillna(method='ffill').fillna(method='bfill').to_numpy()
            labels = [f"[{int(round(bins[i]))}, {int(round(bins[i+1]))})" for i in range(len(bins)-1)]
            if len(set(labels)) == len(labels) and len(np.unique(bins)) == len(bins):
                return bins, labels
        except Exception:
            pass
        n_bins -= 1
    min_val = clean_data.min() if not clean_data.empty else 0
    max_val = clean_data.max() if not clean_data.empty else 1
    bins = np.array([min_val, max_val])
    labels = [f"[{int(round(bins[0]))}, {int(round(bins[1]))})"]
    return bins, labels

print("Resumen de valores únicos y buckets por columna:")
for col in cols_to_bucket:
    unique_vals = df_grouped[col].nunique(dropna=True)
    print(f"{col}: {unique_vals} valores únicos")
    if pd.api.types.is_numeric_dtype(df_grouped[col]):
        is_eventos = col.startswith("eventos")
        is_count = col.startswith("count")
        col_data = df_grouped[col]
        bucketized = pd.Series(index=col_data.index, dtype=object)
        if is_eventos or is_count:
            mask_zero = col_data == 0
            bucketized[mask_zero] = "0"
            nonzero = col_data[~mask_zero]
            if len(nonzero) > 0:
                bins, labels = get_qcut_bins(nonzero, max_bins=10)
                bucketized[~mask_zero] = pd.cut(nonzero, bins=bins, labels=labels, include_lowest=True, duplicates='drop').astype(str)
            result_dict[col] = bucketized
            print(f"Distribución de buckets para {col}:")
            print(bucketized.value_counts())
        else:
            bins, labels = get_qcut_bins(col_data, max_bins=10)
            bucketized = pd.cut(col_data, bins=bins, labels=labels, include_lowest=True, duplicates='drop').astype(str)
            result_dict[col] = bucketized
    else:
        result_dict[col] = df_grouped[col].astype(str)

result_df = pd.DataFrame(result_dict)
result_df.to_csv(output_csv, index=False)
print(f'Archivo bucketizado guardado en {output_csv}')
print(f'Filas en archivo original: {len(df)}')
print(f'Filas en archivo bucketizado: {len(result_df)}')
