from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName("BroadcastJoin") \
    .getOrCreate()

dados_vendas = [
    ("Pedido-1", "SP", 150.0),
    ("Pedido-2", "PR", 320.0),
    ("Pedido-3", "RJ", 90.0),
    ("Pedido-4", "PR", 410.0)
]
df_vendas = spark.createDataFrame(dados_vendas, ["ID_Pedido", "Estado", "Valor"])

dados_estados = [
    ("SP", "São Paulo"),
    ("PR", "Paraná"),
    ("RJ", "Rio de Janeiro")
]
df_estados = spark.createDataFrame(dados_estados, ["Sigla", "Nome_Estado"])

df_resultado = df_vendas.join(
    broadcast(df_estados), 
    df_vendas["Estado"] == df_estados["Sigla"], 
    "inner"
).select("ID_Pedido", "Nome_Estado", "Valor")

print("------- EXECUTANDO JOIN COM BROADCAST -------")
df_resultado.show()

spark.stop()