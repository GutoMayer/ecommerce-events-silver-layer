# Relatório — Pipeline de Dados: Bronze → Silver

---

## 1. Estudo Inicial das Tabelas

### 1.1 Tabela de Usuários

**Estrutura:**

```
root
 |-- user_id:      long (nullable = true)  → PK / FK em Eventos (verificar compatibilidade de tipos)
 |-- country:      string (nullable = true)
 |-- signup_date:  timestamp_ntz (nullable = true)
 |-- age_band:     string (nullable = true)
```

**Volumetria:** 500.000 registros — sem partição, formato `.parquet`.

---

**Nulos e Duplicatas:**

```
Duplicatas: 0

--- Verificando valores nulos ---
+-------+-------+-----------+--------+
|user_id|country|signup_date|age_band|
+-------+-------+-----------+--------+
|      0|      0|          0|       0|
+-------+-------+-----------+--------+
```

**Cardinalidade:**

| Coluna       | Distintos |
|--------------|-----------|
| user_id      | 500.000   |
| country      | 7         |
| signup_date  | 1.916     |
| age_band     | 5         |

---

### 1.2 Distribuição de Usuários

**Por país:**

```
--- country ---
+-------+--------+
|country|   total|
+-------+--------+
|     BR| 200.049|
|     MX|  50.317|
|     US|  50.011|
|     CL|  49.952|
|     CO|  49.945|
|     PE|  49.904|
|     AR|  49.822|
+-------+--------+
```

**Por faixa etária:**

```
--- age_band ---
+--------+--------+
|age_band|   total|
+--------+--------+
|   25-34| 175.016|
|   35-44| 125.037|
|   18-24|  74.967|
|   45-54|  74.769|
|     55+|  50.211|
+--------+--------+
```

**Validação de datas futuras:**

```sql
SELECT signup_date
FROM usuarios
WHERE signup_date > current_date()
```

> ✅ Nenhuma data futura encontrada.

---

### 1.3 Tabela de Produtos

**Estrutura:**

```
root
 |-- product_id:   long (nullable = true)  → PK / FK em Eventos (tipo long — verificar compatibilidade)
 |-- category:     string (nullable = true)
 |-- brand:        string (nullable = true)
 |-- price:        double (nullable = true)
 |-- subcategory:  string (nullable = true)
```

> ⚠️ Tabela originalmente particionada por `subcategory`. Base pequena — particionamento por subcategoria não traz ganho de performance; será reescrita consolidada em 1 arquivo.

**Estatísticas descritivas:**

```
+-------+-----------------+--------+---------+------------------+-----------+
|Summary|       product_id|category|    brand|             price|subcategory|
+-------+-----------------+--------+---------+------------------+-----------+
|  count|            50000|   50000|    50000|             50000|      50000|
|   mean|          25000.5|    NULL|     NULL|124.09263120000007|       NULL|
| stddev|14433.90106658627|    NULL|     NULL|121.78284066530215|       NULL|
|    min|                1|  beleza|marca_000|              2.99| acessorios|
|    25%|            12497|    NULL|     NULL|             52.38|       NULL|
|    50%|            24995|    NULL|     NULL|              89.4|       NULL|
|    75%|            37496|    NULL|     NULL|             153.4|       NULL|
|    max|            50000|    moda|marca_199|            4943.3|   vestidos|
+-------+-----------------+--------+---------+------------------+-----------+
```

---

### 1.4 Distribuição de Produtos

**Por categoria:**

```
+-----------+-----+
|   category|total|
+-----------+-----+
|     livros| 8392|
|     beleza| 8378|
|       moda| 8375|
|    esporte| 8323|
|       casa| 8271|
|eletronicos| 8261|
+-----------+-----+
```

**Por subcategoria (top 20):**

```
+---------------+-----+
|    subcategory|total|
+---------------+-----+
|     acessorios| 3391|
|      maquiagem| 2172|
|        tecnico| 2149|
|       ciclismo| 2123|
|        cozinha| 2121|
|      decoracao| 2109|
|       infantil| 2103|
|         cabelo| 2098|
|       skincare| 2091|
|        futebol| 2085|
|     musculacao| 2079|
|      autoajuda| 2072|
|         ficcao| 2068|
|        corrida| 2036|
|cama_mesa_banho| 2028|
|       perfumes| 2017|
|         moveis| 2013|
|          tenis| 1698|
|            tvs| 1671|
|       vestidos| 1667|
+---------------+-----+
only showing top 20 rows
```

**Por marca (amostra):**

```
+---------+-----+
|    brand|total|
+---------+-----+
|marca_160|  288|
|marca_118|  286|
|marca_048|  284|
|marca_119|  283|
|marca_023|  282|
|marca_100|  278|
|marca_103|  278|
|marca_014|  277|
|marca_021|  277|
|marca_085|  277|
|marca_090|  275|
|      ...|  ...|
+---------+-----+
```

> 💡 1.600 linhas por partição é pouco, o ideal é que essa tabela tivesse todos os dados em somente uma partição...

---

### 1.5 Tabela de Eventos

> ⚠️ **Small File Problem detectado:** 120 arquivos por partição × 30 partições = **3.600 arquivos pequenos**. O Spark não lida bem com esse volume de micro-arquivos — a análise de metadados fica lenta e o desempenho de leitura é degradado.

**Solução adotada:** schema pré-definido (evita inferência cara) + reparticionamento em menos partições.

**Schema definido programaticamente:**

```python
schema = StructType([
    StructField("event_id",        StringType()),
    StructField("event_timestamp", TimestampType()),
    StructField("event_type",      StringType()),
    StructField("price_paid",      DoubleType()),
    StructField("product_id",      StringType()),
    StructField("quantity",        IntegerType()),
    StructField("session_id",      StringType()),
    StructField("user_id",         StringType()),
    StructField("context", StructType([
        StructField("device",     StringType()),
        StructField("ip_hash",    StringType()),
        StructField("user_agent", StringType()),
    ])),
    StructField("marketing", StructType([
        StructField("utm_campaign", StringType()),
        StructField("utm_source",   StringType()),
    ]))
])
```

> Os campos `context` e `marketing` são aninhados e precisam de **flattening** explícito antes da escrita.

Após a leitura, os dados são salvos em Parquet com `.repartition("dt")` para facilitar análise de distribuições e anomalias por data.

---

**Distribuição por tipo de evento:**

```
--- event_type ---
+-----------+----------+
| event_type|     total|
+-----------+----------+
|       view|13.997.473|
|add_to_cart| 4.003.614|
|   purchase| 1.998.893|
+-----------+----------+
```

**Top 20 produtos por volume de eventos:**

```
--- product_id ---
+----------+---------+
|product_id|    total|
+----------+---------+
|      6253|3.966.532|
|      5990|1.726.049|
|     12836|1.061.213|
|     15163|  751.622|
|     11697|  575.075|
|     20956|  462.356|
|     13370|  383.966|
|     23079|  327.381|
|     46376|  284.598|
|     11245|  249.942|
|     33414|  223.565|
|     48135|  201.653|
|     13671|  182.709|
|     43294|  167.548|
|     19139|  153.955|
|       821|  142.708|
|     10299|  132.080|
|     42343|  123.204|
|     42115|  115.285|
|     33653|  109.083|
+----------+---------+
only showing top 20 rows
```

> ⚠️ `product_id = 6253` concentra ~20% de todos os eventos — potencial **Data Skew** a ser tratado no Join.

**Por dispositivo:**

```
--- device ---
+-------+----------+
| device|     total|
+-------+----------+
| mobile|10.001.069|
|desktop| 6.665.436|
| tablet| 3.333.475|
+-------+----------+
```

---

## 2. Engenharia e Desenvolvimento da Camada Silver

Com base no estudo inicial das anomalias estruturais, volumetria e distribuições das tabelas brutas na camada Bronze, as seções a seguir detalham as decisões de engenharia, estratégias de otimização e tratamento de falhas adotadas no script `silver.py`.

---

### 2.1 Decisões de Engenharia e Tratamento dos Dados de Origem

#### A. Manipulação e Estruturação de Eventos Semi-Estruturados

A tabela bruta de eventos apresentava campos aninhados (`context` e `marketing`) dentro de estruturas JSON.

- **Schema Pré-Definido:** em vez de permitir inferência de tipos em tempo de execução (operação cara que varre todos os arquivos), foi definido programaticamente um `StructType` estrito em `self.schema_eventos`. Isso blinda o pipeline contra quebras estruturais.
- **Flattening:** no método `processa_eventos`, os dados foram desaninhados explicitamente via notação de ponto (ex: `context.device` → coluna `device`), gerando colunas de primeiro nível limpas e prontas para indexação analítica.
- **Chave Temporal:** o campo `event_timestamp` foi truncado em data pura com `F.to_date("event_timestamp").alias("event_date")`, que serve de base para o particionamento físico.

#### B. Padronização e Tratamento da Tabela de Produtos

A análise inicial identificou incompatibilidade de tipos no `product_id` entre as tabelas.

- **Casting de Alinhamento de Chaves:** no método `ler_produtos`, a chave primária foi forçada via `.cast(StringType())`, garantindo correspondência com o tipo definido na tabela de eventos e evitando nulos artificiais por incompatibilidade entre `long` e `string` no JOIN.
- **Seleção de Atributos:** apenas as colunas analíticas necessárias foram selecionadas (`product_id`, `category`, `subcategory`, `brand`, `price`), reduzindo a largura física da tabela e economizando memória e rede.

#### C. Isolamento de Registros Rejeitados (Qualidade de Dados)

- **Validação de Chaves:** eventos sem correspondência nas dimensões de produtos ou usuários (registros órfãos) são identificados pela condicional:

  ```python
  valido = F.col("category").isNotNull() & F.col("country").isNotNull()
  ```

- **Fluxo de Desvio:** no método `escreve_tabelas_silver`, o DataFrame é bifurcado com filtros inversos — registros íntegros vão para `eventos_enriquecidos/` e os órfãos/corrompidos para `eventos_rejeitados/`. O pipeline não é interrompido (tolerância a falhas).

---

### 2.2 Mitigação do Small File Problem

Os eventos na camada Bronze sofrem severamente com o *Small File Problem* devido à ingestão contínua por micro-batches. Três estratégias correlacionadas foram aplicadas:

#### A. Ajuste do Paralelismo de Shuffle

O padrão do Spark (`shuffle.partitions = 200`) geraria até 200 arquivos pequenos por partição diária. O valor foi reduzido no construtor da classe:

```python
.config("spark.sql.shuffle.partitions", "30")
```

#### B. Ativação do Adaptive Query Execution (AQE)

```python
.config("spark.sql.adaptive.enabled",                    "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

Com `coalescePartitions` ativo, o Spark mescla partições resultantes pequenas automaticamente após etapas de Join ou Shuffle, antes da escrita em disco.

#### C. Escrita Coesa com Repartition Controlado

- **Tabelas estáticas:** a tabela de produtos é consolidada em um único arquivo — `n_files=1`.
- **Tabelas particionadas:** as tabelas finais da Silver são escritas com `.repartition(partition_col)` antes do `partitionBy`, garantindo **exatamente 1 arquivo Parquet por partição de data**, eliminando definitivamente o problema de arquivos pequenos na entrega.

---

### 2.3 Estratégia de Joins e Otimizações de Performance

#### A. Eliminação de Network Shuffle via Broadcast Join

Em um cenário padrão, cruzar eventos com usuários e produtos exigiria um *Shuffle Hash Join*, movendo muitos gigabytes entre nós do cluster.

Como as tabelas dimensionais são significativamente menores que a tabela de eventos, aplicamos `broadcast()`, que além de resolver o shuffle, nos ajuda a resolver o problema de Data Skew que temos no DF de Produtos:

```python
eventos.alias("e")
    .join(broadcast(produtos), "product_id", "left")
    .join(broadcast(usuarios), "user_id",    "left")
```

O Spark envia cópias integrais das tabelas menores para a memória de cada executor. O cruzamento ocorre puramente em memória (*Map-Side Join*), zerando o tráfego de rede e eliminando o impacto do skew do `product_id = 6253`.

#### B. Gerenciamento do Ciclo de Vida da Memória (Persist & Unpersist)

| DataFrame | Estratégia | Justificativa |
|---|---|---|
| `eventos_flat` | `DISK_ONLY` | Lido uma única vez; preserva lineage sem consumir RAM |
| Inputs após join | `unpersist()` imediato | Libera blocos de memória nos executores antes das agregações finais |

```python
eventos_flat.unpersist()

```

---

### 2.4 Idempotência e Testes Mínimos

- **Idempotência:** o pipeline aceita o parâmetro `data_referencia`. As escritas em modo `"overwrite"` e o cálculo de `dias_desde_signup` via `F.datediff` garantem que o script possa ser reexecutado infinitas vezes para a mesma data de corte sem duplicar linhas ou corromper partições históricas.

- **Testes Mínimos Automatizados:** o método `testes_minimos()` exibe ao final da execução as métricas vitais de controle de qualidade — contagem total, nulos estruturais e duplicatas de `event_id` — comprovando empiricamente a conformidade das transformações antes da entrega ao time de Analytics.

### 2.5 Otimização com SparkUI
- **Spark UI:** na pipeline, eu lia os arquivos JSON, escrevia como parquet e depois lia denovo, porque achava que seria mais rapido. Mas observando o SparkUI vi que não fazia muito sentido fazer isso, ainda mais porque eu poderia fazer o repartition e depois persistir o DF. Reduzi o tempo de exec do Job de 9.5 minutos para 8.1 minutos
---

## 3. Resumo das Decisões Técnicas

| # | Decisão | Impacto |
|---|---|---|
| 1 | Schema pré-definido nos eventos | Evita inferência cara; pipeline resiliente a mudanças estruturais |
| 2 | Flattening de campos aninhados | Colunas de primeiro nível prontas para uso analítico |
| 3 | Cast de `product_id` para `StringType` | Elimina nulos artificiais por incompatibilidade de tipos no JOIN |
| 4 | Isolamento de rejeitados | Qualidade de dados sem interromper a execução |
| 5 | `shuffle.partitions = 30` | Reduz fragmentação na escrita |
| 6 | AQE + `coalescePartitions` | Mescla partições pequenas dinamicamente |
| 7 | `repartition(partition_col)` | 1 arquivo Parquet por data — resolve o Small File Problem |
| 8 | `broadcast()` nas dimensões | Elimina Shuffle Join; mitiga Data Skew do produto popular |
| 9 | Ciclo `persist` / `unpersist` | Controle fino de memória; previne OOM |
| 10 | `data_referencia` + `overwrite` | Idempotência total — pipeline re-executável sem efeitos colaterais |
