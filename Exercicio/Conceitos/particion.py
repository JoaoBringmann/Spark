from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("ParticionamentoNoStorage") \
    .getOrCreate()

dados_vendas = [
    ("Pedido-1", "SP", 150.0),
    ("Pedido-2", "PR", 320.0),
    ("Pedido-3", "RJ", 90.0),
    ("Pedido-4", "PR", 410.0),
    ("Pedido-5", "SP", 200.0)
]
df = spark.createDataFrame(dados_vendas, ["ID_Pedido", "Estado", "Valor"])

print("------- SALVANDO DADOS PARTICIONADOS -------")

# Salvando em formato Parquet e particionando pela coluna 'Estado'
df.write \
    .mode("overwrite") \
    .partitionBy("Estado") \
    .parquet("dados_vendas_particionadas")

print("Dados salvos com sucesso!")
spark.stop()