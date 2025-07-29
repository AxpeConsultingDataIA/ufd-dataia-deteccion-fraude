import os
from pyspark.sql import SparkSession

# Configurar winutils.exe (si no está en variables de entorno del sistema)
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['hadoop.home.dir'] = 'C:\\hadoop'
os.environ['PATH'] += os.pathsep + 'C:\\hadoop\\bin'

# Crear SparkSession
spark = SparkSession.builder.appName("PruebaParquet").getOrCreate()

ruta = r"C:\Users\gmtorrealbac\Documents\AXPE_2025\UFD\GIT\ufd-dataia-deteccion-fraude\src\feature_step\datasets\grupo_eventos_codigos"

df = spark.read.parquet(ruta)
df.show(5)
