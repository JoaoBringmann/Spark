# Spark

Treinando Apache Spark

## FUNDAMENTOS

### Comandos

### Rodar Spark

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

### Comandos Spark

`.filter()`

`.select()`

`.join()`

`.withColumn()`

`.drop()`

`.show()`

`.count()`

`.collect()`

`.write.parquet()`

`.groupBy()`

`.orderBy()`

`.distinct()`

### O que é Spark

O Apache Spark é um Motor de processamento de dados de forma paralela, ele não armazena dados, nem é o destino final destes dados ele é quem faz a transformação deles

#### Camadas

O Spark precisa de 2 outras camadas para funcionar de maneira correta:

* Camada de Armazenamento: HDFS (Sistema de arquivos distribuidos tradicional) e Data Lakes (S3, ADLS, GCS) que o Spark possui conectores nativos para ler e escrever neles
* Camada de Gerenciamento de Recursos(Cluster Manager): Basicamente o RH do Spark, como Standalone, Yarn e Kubernetes

#### Dados

O Spark usa 2 tipos de dados:

* RDD: Que é a unidade básica do Spark. Uma coleção de objetos distribuída entre as máquinas. Ele é "baixo nível". É flexível, mas o Spark não sabe o que tem dentro dele (se é um texto, um JSON, etc.), impossibilitando otimizações automáticas
* DataFrame: É uma evolução do RDD. Pense nele como uma tabela de banco de dados relacional distribuída (com linhas e colunas nomeadas/tipadas). Como o Spark conhece a estrutura (o Schema) do DataFrame, ele consegue otimizar as consultas antes de executá-las usando um motor interno chamado **Catalyst Optimizer**

#### Transformações, Ações e Lazy Evaluation

O Spark por natureza não realiza nenhuma operação a não ser que seja explicitamente pedido algo a ele, como assim ao passar dados e funções como `.filter()`, `.select()`, `.join()`, `.withColumn()` vc não esta falando "Me mostre isso" vc esta dando instruções de como será feito a requisição dos dados isso seriam as Transformações, as Ações seriam `.show()`, `.count()`, `.collect()`, `write.parquet()` que vc esta pedindo ao Spark mostrar os dados.

Então as transformações constroem o Manual e as Ações realizam os passos deste Manual

#### Wide Transformations/Narrow Transformations

Existe 2 tipos de transformações de dados a que ocorre apenas com dados locais e a outra ingloba dados de outra parte da rede

* Narrow Transformation: É a transformação que usa dados locais como `.filter()` e `.select()`
* Wide Transformation: É a transformação que precisa buscar dados em outra rede fazendo requisições ou juntar tabelas grandes de outras redes, o wide traz o coneceito de **shuffling** que é a ação de mover dados entre a rede, ele envolve gravar dados temporarios no disco, transferir dados via rede e ler e reorganizar os dados na memoria o que causa THE gargalo nos processos

#### Shuffle

A maneira mais comum de utilizar o shuffle é durante um `.join()` de 2 tabelas, neste processo dependendo dos dados existem 2 maneiras de realizar a ação

Shuffle Hash Join: É o comportamento padrão do Spark, ele aplica o shuffle nas 2 tabelas usando a chave do join para garantir registros com a mesma chave parem no mesmo executor então ordena os dados e faz o cruzamento deles

Broadcast Hash Join: É um atalho para  processamento de dados, se uma das tabelas for pequena é possivel mandar uma copia dela para os workers que compoem o cluster para que eles façam um `.join()` local que causa 0 shuffling o que aumenta a performance

#### Regras de Negocios

Na aplicação de regras de negocio como filtrar algo especifico, calcular valores etc, existem novamente 2 metodos para realizar tais ações, uma delas é recomendada e otimiada e a outra é mal otimizada mas funciona quando nao tem outra alternativa

* Funções Nativas: Estas são as funções ja criadas para o Spark que foram feitas em Scala/Java e ja foram otimizadas
* UDF: Estas são funções que vc faz manualmente em python mas existe um problema, o Spark roda com o JVM que nao roda python nativamente então ele tem que serializar mandar sua função a um worker, pegar o resultado e serializar ele e mandar para o JVM, estas ações formam o Serialization Overhead que causa lentidão

#### Persistencia de Dados

Existe novamente 2 maneiras de persistir os dados durante a execução das ações do Spark para não gerar retrabalho dos wrokers

* Cache - .cache() : É a forma simplificada de salvar o Dataframe na memoria do worker, por padrão ele usa o nivel de armazenamento MEMORY_AND_DISK, ou seja ele vai tentar salvar todos os dados na memoria RAM do worker e se não ter espaço o suficiente os dados vão para a memoria em disco do worker
* Persist - .persist() : É a versão melhorada do cache, ela permite que vc passe um parametro StorageLevel para definir onde e como quer armazenar os dados

##### StorageLevel

* MEMORY_ONLY: Guarda apenas na memoria RAM, se faltar espaço as partições extras são recalculadas do zero quando solicitado
* MEMORY_AND_DISK: Copia padrão do cache
* DISK_ONLY: Salva o resultado intermediario direto no disco do Worker (Usado se o calculo for pesado)
* MEMORY_AND_DISK_2 ou MEMORY_ONLY_2: Salva os dados e 2 Workers para prevenir falhas ou perda de dados

#### Camada de Storage

* HDFS: Esturutura de amazenamento em disco que junto os discos dos workers no cluster em grande disco e quando dados entram nele, ele quebra ele em partes de 128MB pelo cluster e replica este dados 3 vezes por padrão (Fator de Replicação)
* Object Storage (Cloud Lakes): Nas Nuvens (AWS S3, GCS) o HDFS é substituido pelo Object Storage que infinitamente escalaveis, mais baratos que servidores HDFS e desacoplam o processamento do armazenamento

##### Formato de Arquivos

CSV: O formato mais simples, baseado em texto puro. Salva os dados como uma tabela, onde cada linha é um registro e as colunas são separadas por vírgulas

JSON: Formato baseado em texto que organiza os dados em pares de "chave-valor" e listas, usando chaves { } e colchetes [ ]

Parquet: Formato colunar e binario, melhor formato para Spark, reduz o tamanho em até 75% pois os dados de cada tipo ficam juntos, Filtro de Fonte é algo que é possivel com o formato Parquet que é fazer um filtro de comparar a idade, buscar os dados de INT e pular qualquer outro que não for INT 

##### Particionamento de Dados

Para Otimizar ainda mais a busca dos dados é feito a divisão dos arquivos em subpastas estruturadas com base no valor de uma ou mais colunas, para evitar procurar em milhares de arquivos parquet, ou seja se vc instruir o spark a procurar por mes e ano ele vai isolar os arquivos que estão nestas datas e se alguem requerir dados do mes 3 todos os outros serão ignorados

**Problema:** Se for instruido ao Spark particionar os Cpf ele criara 1 arquivo para cada CPF pois ele é um dado unico, isso causaria o Over-Paticioning

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
7. Qual a diferença fundamental de comportamento (em relação ao uso de rede/infraestrutura) entre uma *Narrow Transformation* e uma  *Wide Transformation* ?
   R:**O Narrow Transformation irá usar menos rede e infraestrutura pois não ha necessidade de enviar ou receber dados de outras partes da rede, ele usa dados locais e os mantem local depois da transformação, ja o wide transformation por usar dados que não estão somente em um local ele necessita fazer requisições a rede para trazer os dados utilizando muito mais rede e infraestrutura**
8. Por que o processo de *Shuffle* é considerado o principal gargalo de performance em pipelines de Big Data com Spark?
   R:**Pois o Shuffle grava dados na memoria em disco e é dependente de quantos dados entram e saem da rede tendo limitação por parte do Hardware**
9. Se aplicarmos consecutivamente três `.filter()` e dois `.select()` em um DataFrame, quantos *Shuffles* o Spark realizará? Explique baseado no tipo de transformação dessas funções.
   R:**Neste caso nenhum processo de shuffle seria utilizado ja que ambas as funções movem dados locais que ingloba o narrow transformation**
10. Explique a diferença de fluxo de dados entre um *Sort-Merge Join* comum e um *Broadcast Hash Join*
    R:**O** **sort fará uma busca das 2 tabelas, armazenara seus dados em disco e os organiza então ele fara a troca de dados entra as redes que tem estas tabelas, o broad fará o envio da tabela pequena aos workers do cluster e ele farão um join interno**
11. Qual o principal risco de usarmos a função `broadcast()` em um DataFrame que, na verdade, possui múltiplos Gigabytes de tamanho? O que aconteceria com os Executors ou com o Driver?
    R:**Se usar o broad com uma quantia grande de dados terá um gargalo ainda maior do que o shuffle pois nao tera armazenamento em disco vai ser somente o envio de gb da dados para cada worker fazendo todo o cluster para tudo para receber os dados da tabela. Se você tentar dar `broadcast()` em um DataFrame de vários Gigabytes, você vai sobrecarregar o nó principal ( **Driver** ), que precisa coletar esses dados e replicá-los via rede para todos os workers. Isso estoura a memória RAM do Driver ou dos Executors, causando o temido erro de OOM**
12. Se você estivesse desenhando um pipeline de dados que cruza uma tabela histórica de logs de servidores de 500GB com uma tabela de cadastro de servidores de 15KB, qual estratégia de Join você aplicaria para garantir a melhor performance?
    R:**Usaria a estrategia de broadcast para enviar os dados de 15kb para cada worker por ser uma quantia baixa de dados**
13. Com suas palavras, por que uma função nativa como `upper()` ou `when()` é muito mais rápida do que uma função equivalente escrita por você em Python usando `def`?
    R:**Pois as funções nativas rodam diretamente no JVM mas as UDF precisam ser serializadas 2 vezes e mandar para um worker para ter o resultado do script python**
14. Se você herdar um código legado de um colega de trabalho contendo dezenas de UDFs em Python que estão deixando o pipeline extremamente lento, e você notar que essas funções fazem apenas limpezas simples de strings e cálculos matemáticos básicos, qual seria sua estratégia para refatorar esse código?
    R:**Utilizar funções nativas como .sum e .upper**
15. O que significa o conceito de *Serialization Overhead* no contexto do PySpark?
    R:**O Executor gerencia a memória em formato Java. Para rodar seu o def, ele precisa pegar aquele dado em formato Java na memória RAM, transformá-lo em uma cadeia de bytes que o Python entenda (fazer o  *Dump/Pickle* ), mandar para o processo do Python rodar a função e, depois, o Python precisa transformar o resultado em bytes de volta para o Java ler. Esse processo de conversão de formatos de memória objeto-para-bytes-para-objeto a cada linha processada é o que chamamos de *Overhead***
16. Qual é a diferença técnica crucial entre chamar `.cache()` e `.persist(StorageLevel.MEMORY_ONLY)` no Spark?
    R: **A diferença especifica entre estes 2 é que o .cache usa o MEMORY_AND_DISK que utiliza o disco rigido do worker se a RAM não for suficiente, ja o .persist com o MEMORY_ONLY não deixa os dados irem para o disco rigido pode se dizer que a diferença crucial é a flexibilidade de usar diferentes tipos de StorageLevel**
17. Imagine o seguinte fluxo em um script seu: Você lê um arquivo, remove linhas duplicadas, faz um `.cache()`, e logo na linha de baixo escreve um `.show()`. O script termina ali. Esse `.cache()` ajudou na performance do seu script? Justifique
    R:**Não ajudou pois nao teve 2 ações que geraria a necessidade de 2 processamentos, so ocorreu 1**
18. Por que deixar DataFrames armazenados com `.cache()` indefinidamente no script, sem executar o `.unpersist()`, pode acabar deixando seus Joins e Shuffles mais lentos no resto do pipeline?
    R:**O Spark divide a RAM em 2 partes o armazenamento e execução se você encher o cluster de caches esquecidos (`.cache()` sem `.unpersist()`), a área de Storage vai engolir a memória do Worker. Quando o Spark precisar de RAM para rodar um Join ou um Shuffle pesado nas etapas seguintes, ele não vai encontrar espaço livre de Execution. O resultado? O Spark será obrigado a jogar os dados do Join e do Shuffle para o disco rígido (Spill to Disk) do Worker. Como ler e escrever no disco é infinitamente mais lento do que na memória RAM, todo o restante do seu pipeline vai arrastar e ficar extremamente lento**

**CENÁRIO:**

Você recebeu a missão de criar um pipeline no Spark que lê uma tabela histórica de vendas com **80 colunas** e  **500 milhões de linhas** . A equipe de analistas de negócios vai usar esse DataFrame final para rodar relatórios diários, mas eles utilizam apenas **3 colunas** específicas para os cálculos deles: `Data_Venda`, `ID_Loja` e `Valor_Total`

19. Se você salvar esse dataset final em formato  **CSV** , o que vai acontecer com a rede e a memória do cluster toda vez que um analista rodar um relatório filtrando por uma loja específica?
    R:**O cluster vai sofrer um gargalo brutal de I/O (entrada/saída) e a memória RAM dos Executors vai ficar inflada com dezenas de colunas inúteis que os analistas nem queriam ver, só para poder aplicar o filtro na memória**
20. Se você salvar esse mesmo dataset em formato  **Parquet** , como o Spark se comportará para entregar o resultado dessas mesmas 3 colunas usadas pelos analistas?
    R:**O Spark lê os metadados do arquivo Parquet (o rodapé do arquivo), descobre a posição física exata na rede daquelas 3 colunas e ignora completamente o resto do arquivo**
21. Explique por que o conceito de "desacoplamento de armazenamento e processamento" (usar Spark com AWS S3/Cloud Storage em vez de servidores locais com HDFS) é o padrão mais adotado por grandes empresas hoje em dia.
    R:**No modelo antigo (HDFS tradicional), se você precisasse de mais espaço de armazenamento, era obrigado a comprar mais servidores inteiros (com CPU, RAM e Disco), mesmo que não precisasse de mais poder de processamento. Desacoplando (Spark rodando em instâncias efêmeras e dados no S3/GCS), você desliga o cluster Spark à noite ou quando os pipelines terminarem (pagando zero por CPU/RAM nessas horas) e seus dados continuam guardados de forma segura e barata na nuvem**

FIM CENÁRIO

22. Pensando no cenário real da sua empresa ou de grandes volumes de dados: Se você tiver uma tabela com dados coletados a cada segundo de sensores de aeronaves e navios, qual seria uma boa estratégia de coluna de partição para salvar esses dados no Data Lake sem causar  *Over-partitioning* ?
    R:**Pensando neste caso uma ideia obvia seria dividir o tipo de transporte que dividiria aeronaves de navios**
23. Explique com suas palavras o que é o *Partition Pruning* e por que ele economiza tempo de processamento.
    R:**O particionamento pruning é o pente fino focado em que vc ignora onde não existe os dados que vc quer e foca apenas onde eles estão evitando o dowload de dados que vc nao usara**
24. Se você rodar um script e perceber que ele gerou 15.000 pastas no seu S3, e dentro de cada pasta tem apenas um arquivo Parquet de 5KB, o que aconteceu e como você corrigiria isso no código do Spark?
    R:**Aconteceu um over-particioning pois as pastas estão muito especificas o que prejudicara o cluster, uma quantia saldavel de dados nas pastas seria em torno de 128MB a 512MB**
