# 🎬 Pipeline de Filmes — Airflow + PySpark

Pipeline de dados orquestrado pelo **Apache Airflow** que consome uma API de filmes, processa os dados com **PySpark** e os organiza em camadas (RAW → SILVER).

---

## 📐 Arquitetura

```
API de Filmes
     │
     ▼
┌─────────────────┐
│  validar_dados  │  ← BranchPythonOperator
│  (Airflow DAG)  │     Verifica se a API retornou dados
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
sem_dados   extrair_dados_api        ← Camada RAW
(skip)      (BashOperator)
                 │
                 ▼
         transformar_silver          ← Camada SILVER
         (BashOperator + PySpark)
```

---

## 🗂️ Estrutura do Projeto

```
pipeline_filmes/
│
├── dags/
│   └── pipeline_filmes.py       # DAG principal do Airflow
│
├── scripts/
│   ├── extracao.py              # Extração da API → camada RAW
│   └── transformacao_silver.py  # Transformação com PySpark → camada SILVER
│
├── data/
│   ├── raw/                     # Dados brutos (JSON/Parquet)
│   └── silver/                  # Dados tratados (Parquet particionado)
│
├── docker-compose.yml
└── README.md
```

---

## ⚙️ Pré-requisitos

| Ferramenta | Versão recomendada |
|---|---|
| Python | 3.10+ |
| Apache Airflow | 2.8+ |
| Apache Spark / PySpark | 3.5+ |
| Docker + Docker Compose | Latest |

---

## 🚀 Como executar

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/pipeline-filmes.git
cd pipeline-filmes
```

### 2. Configure as variáveis do Airflow

No painel do Airflow (**Admin → Variables**), cadastre:

| Chave | Valor |
|---|---|
| `url_filmes` | URL da API de filmes |

### 3. Suba o ambiente com Docker

```bash
docker-compose up -d
```

### 4. Acesse o Airflow

```
http://localhost:8080
```

Ative a DAG `pipeline_filmes` e ela rodará automaticamente às **18h (horário de Brasília)**.

---

## 🔄 Fluxo da DAG

### `validar_dados` — BranchPythonOperator
- Faz uma requisição `GET` para a API configurada na variável `url_filmes`
- Se a API retornar dados → segue para `extrair_dados_api`
- Se a API retornar vazio → segue para `sem_dados` (pipeline encerrado)
- Se a API retornar erro → lança exceção e aciona as regras de retry

### `extrair_dados_api` — Camada RAW
- Executa `scripts/extracao.py`
- Consome a API e persiste os dados brutos em `/data/raw/`

### `transformar_silver` — Camada SILVER
- Executa `scripts/transformacao_silver.py` com PySpark
- Limpeza e padronização dos dados
- Persistência em Parquet particionado em `/data/silver/`

---

## 🔁 Política de Retry

Configurada em `default_args` da DAG:

```python
"retries": 5,
"retry_delay": timedelta(minutes=1),
"retry_exponential_backoff": True,
"max_retry_delay": timedelta(minutes=5),
```

Em caso de falha, o Airflow realiza até **5 tentativas** com backoff exponencial, aguardando no máximo **5 minutos** entre cada uma.

---

## 🛠️ Transformações PySpark (Camada SILVER)

Operações aplicadas sobre os dados brutos:

- Remoção de registros duplicados
- Tratamento de valores nulos
- Padronização de tipos (datas, numéricos)
- Renomeação e seleção de colunas relevantes
- Escrita em formato **Parquet** com particionamento por `ano_lancamento`

---

## 📅 Schedule

```
0 18 * * *  →  Todos os dias às 18:00 (America/Sao_Paulo)
```

---

## 📌 Observações

- `catchup=False` — a DAG não executa períodos históricos ao ser ativada
- O `BranchPythonOperator` garante que o pipeline não falhe quando a API não retornar dados
- Toda a comunicação com a API é validada antes de iniciar qualquer processamento
