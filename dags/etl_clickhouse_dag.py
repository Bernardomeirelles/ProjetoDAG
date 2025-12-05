# dags/etl_clickhouse_dag.py
from datetime import datetime, timedelta
import json

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajusta o path pra conseguir importar src.*
import sys
from pathlib import Path

PROJECT_ROOT = Path("/opt/airflow")  # dentro do container
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from extract import extrair_dados       # src/extract.py
from transform import transformar_dados # src/transform.py
from load import load_to_clickhouse     # src/load.py


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def extract_task(**context):
    """
    Task de extract:
    - Lê o CSV local via extrair_dados()
    - Envia o DataFrame em formato JSON via XCom
    """
    df = extrair_dados()
    json_str = df.to_json(orient="records", date_format="iso")
    context["ti"].xcom_push(key="raw_data", value=json_str)


def transform_task(**context):
    """
    Task de transform:
    - Recebe JSON da task de extract
    - Reconstrói o DataFrame
    - Aplica transformar_dados()
    - Devolve JSON via XCom
    """
    ti = context["ti"]
    raw_json = ti.xcom_pull(key="raw_data", task_ids="extract_customers")
    records = json.loads(raw_json)
    df_raw = pd.DataFrame.from_records(records)

    df_transformed = transformar_dados(df_raw)
    json_str = df_transformed.to_json(orient="records", date_format="iso")
    ti.xcom_push(key="transformed_data", value=json_str)


def load_task(**context):
    """
    Task de load:
    - Recebe JSON da task de transform
    - Reconstrói o DataFrame
    - Chama load_to_clickhouse()
    """
    ti = context["ti"]
    transformed_json = ti.xcom_pull(
        key="transformed_data",
        task_ids="transform_customers",
    )
    records = json.loads(transformed_json)
    df_transformed = pd.DataFrame.from_records(records)

    total_rows = load_to_clickhouse(df_transformed, table_name="customers")
    print(f"[DAG] Carga finalizada. Total de linhas na tabela: {total_rows}")


with DAG(
    dag_id="etl_customers_clickhouse",
    default_args=default_args,
    description="ETL simples: CSV -> Pandas -> ClickHouse usando PythonOperator",
    schedule_interval=None,  # ou cron, ex: "0 3 * * *"
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "clickhouse", "csv"],
) as dag:

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_task,
        provide_context=True,
    )

    transform_customers = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_task,
        provide_context=True,
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_task,
        provide_context=True,
    )

    # Orquestração: extract -> transform -> load
    extract_customers >> transform_customers >> load_customers
