# Spark

Treinando Apache Spark

# Comandos

## Rodar Spark

```bash
cd Exercicio
```

```bash
docker-compose up --build
```

```bash
docker cp .\SCRIPT.py spark-master:/opt/spark/SCRIPT.py  
```

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/SCRIPT.py
```

## Comandos Spark

`.filter()`

`.select()`

`.join()`

`.withColumn()`

`.drop()`

`.show()`

`.count()`

`.collect()`

`write.parquet()`

`.groupBy()`

`.orderBy()`

`.distinct()`

`.join()`

# O que é Spark

O Apache Spark é uma ferramenta de processamento de dados de forma paralela, ele não armazena dados, nem é o destino final destes dados ele é quem faz a transformação deles

### Camadas

O Spark precisa de 2 outras camadas para funcionar de maneira correta:

* Camada de Armazenamento: HDFS (Sistema de arquivos distribuidos tradicional) e Data Lakes (S3, ADLS, GCS) que o Spark possui conectores nativos para ler e escrever neles
* Camada de Gerenciamento de Recursos(Cluster Manager): Basicamente o RH do Spark, como Standalone, Yarn e Kubernetes

### Dados

O Spark usa 2 tipos de dados:

* RDD: Que é a unidade básica do Spark. Uma coleção de objetos distribuída entre as máquinas. Ele é "baixo nível". É flexível, mas o Spark não sabe o que tem dentro dele (se é um texto, um JSON, etc.), impossibilitando otimizações automáticas
* DataFrame: É uma evolução do RDD. Pense nele como uma tabela de banco de dados relacional distribuída (com linhas e colunas nomeadas/tipadas). Como o Spark conhece a estrutura (o Schema) do DataFrame, ele consegue otimizar as consultas antes de executá-las usando um motor interno chamado **Catalyst Optimizer**

### Transformações, Ações e Lazy Evaluation

O Spark por natureza não realiza nenhuma operação a não ser que seja explicitamente pedido algo a ele, como assim ao passar dados e funções como `.filter()`, `.select()`, `.join()`, `.withColumn()` vc não esta falando "Me mostre isso" vc esta dando instruções de como será feito a requisição dos dados isso seriam as Transformações, as Ações seriam `.show()`, `.count()`, `.collect()`, `write.parquet()` que vc esta pedindo ao Spark mostrar os dados.

Então as transformações constroem o Manual e as Ações realizam os passos deste Manual

# Perguntas

1. O Apache Spark foi projetado para substituir bancos de dados de armazenamento como o Hadoop HDFS ou ele desempenha outro papel? Explique.
   R:**Ele não foi feito para ser utilizado como BD mas sim como uma ferramenta de processamento de dados em paralelo**
2. Qual a principal vantagem regulatória/operacional de utilizarmos **DataFrames** ao invés de **RDDs** puros no nosso código de Engenharia de Dados?
   R:**Ao usar Dataframe o spark passa a conhecer o Schema dos dados possibilitando sua automação e busca do melhor jeito de processalos**
3. Se você precisar filtrar dados usando múltiplos critérios no PySpark (ex: Condição A **E** Condição B), qual detalhe de sintaxe é obrigatório para o código não falhar?
   R:**Para filtrar dados o necessarios é a utilização do .filter e se for um AND usar & e se for OU usar |, tambem é necessario realizar as comparações entre paranteses para não confuncir o Python**
4. O que é o **DAG** no Apache Spark e qual a relação dele com as  *Transformations* ?
   R:**O DAG seria o conjunto de instruçoes do Spark que são dividas em Transformations que é a etapa em que o spark constroi um guia para ele mesmo para que quando vc pessa algo a ele como me mostre isso ele realiza a Ação que seria a outra parte do proceso**
5. Se escrevermos um script PySpark contendo 50 transformações seguidas e, no final, **não** chamarmos nenhuma Action, o cluster do Spark gastará processamento computando os dados? Explique
   R:**O cluster não gastara nada em processamento ele nem mesmo rodara nada por ter o principio do lazy processing onde se nenhuma ação for solicitada em relação aqueles dados nada ocorrera alem da construção do manual**
6. Diga se as funções `.withColumn()` e `.count()` são uma *Transformation* ou uma  *Action* , respectivamente
   R:**A função .withColumn é uma transformação por criar uma noca coluna ou substituir outra mas não é algo que precise mostrar, ja o .count() é uma ação pois estou pedindo ao spark me mostre a contagem disso**
