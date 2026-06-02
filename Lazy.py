from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("LazyEvaluation") \
    .getOrCreate()

dados_infra = [
    ("Pedido-1", 100.0),
    ("Pedido-2", 250.0),
    ("Pedido-3", 50.0)
]
colunas = ["ID_Pedido", "Valor_Original"]

df_cluster = spark.createDataFrame(dados_infra, schema=colunas)

# Criamos uma nova coluna calculando a memória em Megabytes usando .withColumn()
# lit() serve para criar um valor literal/constante no Spark
df_transformado = df_cluster.withColumn("Valor_Com_Desconto", col("Valor_Original") * 0.9)
df_focado = df_transformado.filter(col("Valor_Com_Desconto") > 80)
print("------- AÇÃO EXECUTADA -------")
df_focado.count()
df_focado.show()
spark.stop()