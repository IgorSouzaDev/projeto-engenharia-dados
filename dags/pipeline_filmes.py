from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id='pipeline_filmes',
    start_date=datetime(2026, 4, 1, tzinfo=local_tz),
    schedule='0 18 * * *',  # Roda às 18:00 horário de Brasília
    catchup=False
) as dag:

    # Tarefa 1: Camada RAW (Extração)
    extrair_dados = BashOperator(
        task_id='extrair_dados_api',
        bash_command='python3 /opt/airflow/scripts/extracao.py'
    )

    # Tarefa 2: Camada SILVER (Transformação/Limpeza)
    transformar_dados = BashOperator(
        task_id='transformar_silver',
        bash_command='python3 /opt/airflow/scripts/transformacao_silver.py'
    )

    # Definindo a dependência: a Silver só roda se a Extração terminar com sucesso
    extrair_dados >> transformar_dados