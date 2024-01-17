import pendulum
import json
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


jf = [
  {
    "schema": "ods",
    "table": "operations_b2c_w_finance",
    "column_to_check": "date_uz",
    "target_actuality": 1
  },
  {
    "schema": "raw",
    "table": "currency_rates_official",
    "column_to_check": "source_dt",
    "target_actuality": 0
  },
  {
    "schema": "marts",
    "table": "uzumbank_overview_report",
    "column_to_check": "date",
    "target_actuality": 1
  }
]

table_list = jf


def insert_sandbox(**kwargs) -> None:
    table_name = kwargs["table_name"]
    for tab in table_list:
        print(tab)


default_args = {
    'owner': 'd.osinin',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
        'raw__dq_data_relevance',
        description='data quality date checks',
        default_args=default_args,
        start_date=pendulum.datetime(2023, 11, 30, tz="Asia/Tashkent"),
        schedule_interval='05 8-19 * * *',
        max_active_runs=1,
        max_active_tasks=1,
        catchup=False,
        tags=['osinin']
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    insert_sandbox = PythonOperator(
        task_id="get_dag_runs",
        python_callable=insert_sandbox,
        dag=dag,
        op_kwargs={"table_name": "dq_data_relevance"}
    )

start >> insert_sandbox >> finish
