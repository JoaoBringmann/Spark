from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ConceitosFundamentais") \
    .getOrCreate()

dados_vendas = [
    ("Ana", "TI", 8000),
    ("Carlos", "RH", 5000),
    ("Bruno", "RH", 4000),
    ("Maria", "Financeiro", 6000),
    ("João", "TI", 7000),
    ("Diana", "Financeiro", 7500)
]
colunas = ["Nome", "Departamento", "Salario"]

df_vendas = spark.createDataFrame(dados_vendas, schema=colunas)

colunas_presentes = df_vendas.columns
print(f"--> Colunas do DataFrame: {colunas_presentes}")

total_registros = df_vendas.count()
print(f"--> Total de registros processados: {total_registros}")

# Importante: No PySpark, usamos os operadores lógicos & (E), | (OU) e cada condição DEVE estar entre parênteses.
df_filtrado = df_vendas.filter((df_vendas["Salario"] > 5000) | (df_vendas["Departamento"] == "RH"))
df_filtrado.select("Nome", "Salario").show()
df_filtrado.show()
spark.stop()