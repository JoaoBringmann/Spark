from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Escrita") \
    .getOrCreate()

dados = [("Maça", 1.99), ("Banana", 9.24), ("Tramontina", 10.0)]
colunas = ["Nome", "Preço"]


df = spark.createDataFrame(dados, schema=colunas)
df.show()
df.filter(df["Preço"] > 5).show()
df.printSchema()
spark.stop()
print(dados)