# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr

def inicializar_ambiente():
    """Inicializa a SparkSession configurando o pacote do driver do PostgreSQL."""
    return SparkSession.builder \
        .appName("GeradorDadosDesafio1") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def gerar_tabela_lojas(spark):
    """Gera o cadastro de 51 lojas e insere diretamente no PostgreSQL via JDBC."""
    print("-> Gerando dados cadastrais das lojas...")
    
    # Criando IDs de 1 a 51
    df_lojas = spark.range(1, 52).withColumnRenamed("id", "id_loja")
    
    # Adicionando um nome fictício para cada loja
    df_lojas = df_lojas.withColumn("nome_loja", expr("concat('Filial_Id_', id_loja)"))
    
    print("-> Gravando tabela 'lojas' no PostgreSQL...")
    df_lojas.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres-db:5432/data_engineering") \
        .option("dbtable", "lojas") \
        .option("user", "spark_user") \
        .option("password", "spark_password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    print("-> Tabela 'lojas' gravada com sucesso no Postgres.")

def gerar_tabela_vendas_skewed(spark):
    """
    Gera 2 milhões de registros de vendas com uma assimetria severa (Data Skew).
    A loja de ID 1 (São Paulo) conterá 75% de todos os registros do dataset.
    """
    print("-> Gerando 2 milhões de registros de vendas com desbalanceamento (Skew)...")
    
    # Criando a estrutura base de linhas
    total_registros = 2000000
    df_vendas = spark.range(1, total_registros + 1).withColumnRenamed("id", "id_venda")
    
    # Aplicando a regra do Data Skew na coluna id_loja:
    # Se um número aleatório entre 0 e 1 for menor que 0.75, a loja será a ID 1 (75% dos dados).
    # Caso contrário, escolhe aleatoriamente uma loja entre as IDs 2 e 51.
    df_vendas = df_vendas.withColumn(
        "id_loja",
        when(rand(seed=42) < 0.75, 1)
        .otherwise(expr("floor(rand(seed=43) * 50) + 2")).cast("integer")
    )
    
    # Adicionando colunas de valores e atributos adicionais simulados
    df_vendas = df_vendas.withColumn("valor_venda", expr("round(rand(seed=44) * 1000, 2)"))
    df_vendas = df_vendas.withColumn("data_venda", expr("date_add(to_date('2026-01-01'), cast(rand(seed=45) * 150 as int))"))
    
    # Salvando em formato Parquet local dentro do volume para simular o Data Lake de arquivos
    caminho_destino = "/opt/spark/apps/dados/vendas.parquet"
    print(f"-> Gravando arquivos Parquet de vendas em: {caminho_destino}...")
    
    df_vendas.write \
        .mode("overwrite") \
        .parquet(caminho_destino)
    print("-> Arquivos Parquet de vendas gravados com sucesso.")

if __name__ == "__main__":
    spark_session = inicializar_ambiente()
    try:
        gerar_tabela_lojas(spark_session)
        gerar_tabela_vendas_skewed(spark_session)
        print("\n[SUCESSO] Todo o ambiente de dados do Estudo de Caso 1 foi povoado!")
    finally:
        spark_session.stop()