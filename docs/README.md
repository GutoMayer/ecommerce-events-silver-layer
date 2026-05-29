# Case de Engenharia de Dados — Camada Silver de E-commerce

## Contexto

Você foi contratado como engenheiro de dados em uma empresa de e-commerce. O time de analytics precisa de uma **tabela consolidada de eventos enriquecidos** para alimentar dashboards de funil de conversão e modelos de churn. Hoje os dados ficam espalhados em três fontes brutas e ninguém ainda construiu o pipeline que junta tudo.

**Sua missão:** construir, do zero, em **PySpark**, a camada Silver que entrega uma tabela única, limpa, otimizada e pronta para consumo analítico.

---

## Os dados de origem

Você recebe um script `gerar_dados.py` que materializa três fontes em disco, simulando um data lake real:

### 1. `eventos/` — eventos de navegação (JSON Lines)

Particionado por dia (`dt=YYYY-MM-DD`), com **vários arquivos pequenos por dia** (simulando ingestão por micro-batches de um stream).

Cada linha é um JSON com schema **semi-estruturado** — alguns campos são aninhados, outros são opcionais:

```json
{
  "event_id": "e3b39de7c8c04251ad81254086ac1417",
  "event_timestamp": "2025-03-20T13:03:54",
  "event_type": "view",
  "user_id": 36871,
  "product_id": 5960,
  "session_id": "sess_0000000000000000",
  "context": {
    "device": "tablet",
    "user_agent": "Mozilla/5.0 ...",
    "ip_hash": "876b2c66cf3f6a00"
  },
  "marketing": {
    "utm_source": "google",
    "utm_campaign": "camp_012"
  },
  "quantity": 2,
  "price_paid": 89.90
}
```

> ⚠️ Nem todos os campos aparecem em todos os eventos. Cabe a você descobrir quais são opcionais e por quê.

### 2. `produtos/` — catálogo (Parquet)

Catálogo de produtos com `product_id`, `category`, `subcategory`, `brand`, `price`. Volume na casa de dezenas de milhares de produtos.

### 3. `usuarios/` — cadastro (Parquet)

Cadastro de usuários com `user_id`, `country`, `signup_date`, `age_band`. Volume na casa de centenas de milhares de usuários.

---

## Setup

```bash
pip install pyspark numpy pandas pyarrow

# Escolha a escala de acordo com sua máquina:
python gerar_dados.py --output ./dados_brutos --escala pequena   # ~2M eventos
python gerar_dados.py --output ./dados_brutos --escala media     # ~20M eventos (recomendada)
python gerar_dados.py --output ./dados_brutos --escala grande    # ~50M eventos
```

A geração leva alguns minutos. O resultado fica em `./dados_brutos/{eventos,produtos,usuarios}`.

---

## O que você precisa entregar

### 1. Pipeline `silver.py`

Um script PySpark que lê as três fontes brutas e produz **uma única tabela Parquet** chamada `eventos_enriquecidos`, com pelo menos as seguintes colunas:

| Coluna | Origem |
|---|---|
| `event_id`, `event_timestamp`, `event_type`, `event_date` | eventos |
| `user_id`, `country`, `age_band`, `dias_desde_signup` | eventos × usuários |
| `product_id`, `category`, `subcategory`, `brand`, `price` | eventos × produtos |
| `device`, `utm_source`, `utm_campaign` | eventos (achatados) |
| `quantity`, `price_paid` (quando aplicável) | eventos |
| `session_id` | eventos |

**Regras de qualidade:**
- Eventos com `product_id` ou `user_id` que não existem nas tabelas de referência devem ser **isolados em uma tabela `eventos_rejeitados`** (não descartados silenciosamente).
- Tipos devem ser consistentes na saída final (cuidado com campos que podem chegar em formatos inesperados).
- A tabela final deve estar particionada de forma que consultas por intervalo de datas sejam eficientes.

### 2. Relatório `RELATORIO.md`

Documento curto (2 a 4 páginas) cobrindo:

**a) Exploração inicial.** Antes de codar o pipeline, o que você descobriu olhando os dados? Volume, distribuições, anomalias, qualidade. Inclua os comandos/queries que você usou.

**b) Decisões de design.** Para cada escolha não-trivial do pipeline, justifique:
- Como você lidou com a leitura dos arquivos JSON?
- Como você fez os joins entre eventos × produtos × usuários? Por quê dessa forma?
- Como você particionou e escreveu a saída? Por quê?
- Que estratégia de cache/persist você usou (se usou)?

**c) Análise de performance.** Rode o pipeline e analise via **Spark UI**:
- Quanto tempo cada stage levou?
- Há tasks com duração muito acima da mediana? O que isso indica?
- Qual o tamanho do shuffle? Dá pra reduzir?
- Faça **ao menos uma otimização** baseada no que você viu na UI, e mostre o antes/depois com números.

---

## Critérios de avaliação

| Peso | Critério |
|---|---|
| 25% | **Correção** — pipeline roda fim-a-fim, schema bate, contagens fazem sentido, rejeitados isolados corretamente |
| 25% | **Qualidade do código** — organização, modularização, nomes, testes mínimos, idempotência |
| 30% | **Decisões de engenharia** — você percebeu os problemas dos dados de origem? Tratou cada um deles de forma justificada? |
| 20% | **Análise de performance** — você sabe ler a Spark UI e propor otimizações com base em evidência? |

---

## Dicas (sem entregar a resposta)

- **Olhe os dados antes de codar.** Conte arquivos. Veja o tamanho médio. Faça `groupBy().count()` em colunas-chave. As decisões boas vêm da exploração.
- **Spark UI é seu melhor amigo.** Rode com `spark.sparkContext.setLogLevel("WARN")` e abra `localhost:4040` enquanto o job roda. Olhe Stages, Tasks e SQL.
- **Nem todo join precisa de shuffle.** Pense no tamanho relativo das tabelas.
- **Distribuições enviesadas têm consequências.** Se uma chave de join concentra muita coisa, alguma task vai sofrer. O que você faz?
- **Particionar errado é pior que não particionar.** Particionamento serve pra acelerar leitura por filtro — não pra organizar bonitinho no disco.

---

## Stack obrigatória

- Python 3.10+
- PySpark 3.4+
- Roda local (modo `local[*]`) — **não precisa de cluster**

Boa sorte. 🚀
