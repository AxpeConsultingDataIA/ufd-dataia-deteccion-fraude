import os
from pyspark.sql import SparkSession

# Solución para Windows

os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['hadoop.home.dir'] = 'C:\\hadoop'
os.environ['PATH'] += os.pathsep + 'C:\\hadoop\\bin'



spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .config("spark.hadoop.home.dir", "C:\\hadoop") \
    .config("spark.hadoop.version", "3.3.6") \
    .getOrCreate()


df = spark.read.csv(
    "C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/GIT/ufd-dataia-deteccion-fraude/src/feature_step/datasets/s09.csv",
    header=True,
    sep=";",
    inferSchema=True
)

df.show()


print(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())


df.write \
  .mode("overwrite") \
  .partitionBy("partition_0", "partition_1", "partition_2") \
  .parquet("C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/GIT/ufd-dataia-deteccion-fraude/src/feature_step/datasets/eventos")
"""
df.write \
  .mode("overwrite") \
  .parquet("C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/GIT/ufd-dataia-deteccion-fraude/src/feature_step/datasets/grid_contadores")
"""


