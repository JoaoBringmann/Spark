from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("NarrowVsWide") \
    .getOrCreate()

dados_infra = [
    ("Servidor-A", "Ativo", 16),
    ("Servidor-B", "Inativo", 8),
    ("Servidor-A", "Ativo", 32),
    ("Servidor-C", "Ativo", 64),
    ("Servidor-B", "Ativo", 16)
]
colunas = ["Hostname", "Status", "Memoria_GB"]
df_cluster = spark.createDataFrame(dados_infra, schema=colunas)
df_filtrado = df_cluster.filter(col("Status") == "Ativo")
df_agrupado = df_filtrado.groupBy("Hostname").agg(sum("Memoria_GB").alias("Total_Memoria"))

print("------- EXECUTANDO AÇÃO -------")
df_agrupado.show()

spark.stop()