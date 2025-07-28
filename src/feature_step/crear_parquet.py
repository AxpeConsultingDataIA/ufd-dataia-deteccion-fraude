from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(
    "C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/GIT/ufd-dataia-deteccion-fraude/src/feature_step/datasets/denuncias.csv",
    header=True,
    sep=";",
    inferSchema=True  # esto detectará automáticamente que data_date es LongType
)

# Guardar en un único archivo parquet, sin modificar data_date
df.coalesce(1).write.mode("overwrite").parquet(
    "C:\\Users\\gmtorrealbac\\Documents\\AXPE_2025\\UFD\\GIT\\ufd-dataia-deteccion-fraude\\src\\feature_step\\datasets\\denuncias"
)


