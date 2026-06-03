from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("CacheEPersist") \
    .getOrCreate()

# Criando um DataFrame simulando dados volumosos
dados_grandes = [(i, f"Usuario_{i}", i * 2) for i in range(1000000)]
df_pesado = spark.createDataFrame(dados_grandes, ["ID", "Nome", "Pontuacao"])

# Simulando uma transformação complexa/demorada
df_transformado = df_pesado.filter(df_pesado.ID % 2 == 0)

# --- CENÁRIO 1: Sem Cache (A Linhagem será recalculada) ---
print("------- EXECUTANDO CENÁRIO 1 (SEM CACHE) -------")
inicio = time.time()
total_linhas = df_transformado.count()
print(f"Ação 1 (Count): {total_linhas} linhas encontradas.")

# O Spark vai recalcular toda a transformação aqui novamente
primeiras_linhas = df_transformado.take(5)
print(f"Tempo total sem cache: {time.time() - inicio:.2f} segundos")

# --- CENÁRIO 2: Com Cache (Guardando o estado intermediário) ---
print("\n------- EXECUTANDO CENÁRIO 2 (COM CACHE) -------")
df_transformado.cache()

inicio_cache = time.time()
# Esta ação vai demorar um pouco porque vai calcular E salvar na memória (Cache Coeval)
total_linhas_c = df_transformado.count()
print(f"Ação 1 (Count com Cache): {total_linhas_c} linhas encontradas.")

# Esta ação será instantânea porque lerá direto da memória RAM do Worker
primeiras_linhas_c = df_transformado.take(5)
print(f"Tempo total com cache: {time.time() - inicio_cache:.2f} segundos")

# Libera a memória RAM do cluster
df_transformado.unpersist()

spark.stop()