from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, count

spark = SparkSession.builder \
    .appName("IdentificandoDataSkew") \
    .master("local[4]") \
    .getOrCreate()

dados_skewed = [("SP", i) for i in range(100000)] + \
               [("AC", i) for i in range(100)] + \
               [("RR", i) for i in range(50)]

df = spark.createDataFrame(dados_skewed, ["Estado", "Valor"])

df_shuffled = df.repartition(4, "Estado")

print("------- ANALISANDO A DISTRIBUIÇÃO DAS PARTIÇÕES -------")

df_analise = df_shuffled.withColumn("Partition_ID", spark_partition_id()) \
                        .groupBy("Partition_ID") \
                        .agg(count("Estado").alias("Total_Linhas"))

df_analise.show()

spark.stop()