### Estudo de Caso 1: O Gargalo da Black Friday

**Foco:** *Data Skew (Assimetria), Shuffles e Otimização de Joins.*

* **O Cenário:** Você precisa juntar duas tabelas para gerar um relatório de faturamento por filial.
  * A primeira tabela é a `vendas` (gerada via script com 20 milhões de linhas), onde a coluna `id_loja = 1` (São Paulo) representa 75% de todas as vendas. O resto está espalhado por outras 50 lojas menores.
  * A segunda tabela é a `lojas` (uma tabela cadastral pequena, vinda do PostgreSQL via JDBC, com apenas 51 linhas: o ID e o Nome da loja).
* **O Problema (A Dor):** Quando você executa um `.join()` tradicional entre essas duas tabelas e roda um `.show()`, o seu cluster Docker simplesmente trava ou joga o processamento de um único Executor lá para o alto, demorando minutos para concluir um volume relativamente pequeno.
* **O Desafio para Você Resolver:**
  1. Identifique via código (usando `spark_partition_id()`) qual partição está sobrecarregada.
  2. Resolva o problema sem aumentar a memória do Docker. Você deve testar duas abordagens: primeiro usando **Broadcast Join** (já que a tabela de lojas é minúscula) e medir a diferença de tempo.
  3. Depois, como desafio avançado, tente aplicar a técnica de **Salting** na chave de Join da tabela grande para ver como o Spark se comporta dividindo a loja 1 em múltiplos nós.
