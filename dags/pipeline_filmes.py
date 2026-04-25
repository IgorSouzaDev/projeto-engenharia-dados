from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
from airflow.models import Variable
import requests

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

# Função para verificar se a API tem dados
def verificar_api():
    url = Variable.get("url_filmes")  

    try:
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            raise Exception("Erro na API")

        dados = response.json()

        if not dados:
            return "sem_dados"

        return "extrair_dados_api"

    except Exception as e:
        raise Exception(f"Falha ao chamar API: {e}")

with DAG(
    dag_id='pipeline_filmes',
    start_date=datetime(2026, 4, 1, tzinfo=local_tz),
    schedule='0 18 * * *',  # Roda às 18:00 horário de Brasília
    catchup=False
) as dag:

    # 1. Agora sim, criamos a Task de validação
    validar_dados = BranchPythonOperator(
        task_id='validar_dados',
        python_callable=verificar_api
    )

    # 2. Task para o caso de não haver dados (caminho alternativo)
    sem_dados = EmptyOperator(
        task_id='sem_dados'
    )

    # Tarefa 1: Camada RAW (Extração)
    extrair_dados = BashOperator(
        task_id='extrair_dados_api',
        bash_command='python3 /opt/airflow/scripts/extracao.py',
        
    )

    # Tarefa 2: Camada SILVER (Transformação/Limpeza)
    transformar_dados = BashOperator(
        task_id='transformar_silver',
        bash_command='python3 /opt/airflow/scripts/transformacao_silver.py',
        
    )

    validar_dados >> [extrair_dados, sem_dados]

    # Definindo a dependência: a Silver só roda se a Extração terminar com sucesso
    extrair_dados >> transformar_dados