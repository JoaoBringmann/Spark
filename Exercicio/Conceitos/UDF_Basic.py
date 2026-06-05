from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("UDFvsBuiltIn") \
    .getOrCreate()

# Criando um mini dataset de servidores
dados = [("Servidor-A", "ativo"), ("Servidor-B", "inativo"), ("Servidor-C", "ativo")]
df = spark.createDataFrame(dados, ["Hostname", "Status"])

# --- ABORDAGEM 1: Utilizando Função Nativa (Built-in) ---

df_nativa = df.withColumn("Status_Upper_Nativo", upper(col("Status")))


# --- ABORDAGEM 2: Utilizando UDF (Python Puro) ---
# Mais lento, força a comunicação JVM <-> Python
def transformar_maiusculo(texto):
    if texto:
        return texto.upper()
    return None

# Registrando a função como uma UDF do Spark
maiusculo_udf = udf(transformar_maiusculo, StringType())

df_udf = df.withColumn("Status_Upper_UDF", maiusculo_udf(col("Status")))

print("------- EXECUTANDO PROCESSAMENTO -------")
df_nativa.show()
df_udf.show()

spark.stop()