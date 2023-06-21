from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator


def print_context(**kwargs):
    kwargs['ti'].xcom_push(key='context_len', value=len(kwargs))
    print(f"context: {kwargs}")
    print(f"context['ti']: {kwargs['ti']}")

default_args = {
    'owner': 'u_m1dxa'
}

with DAG (
    'xcom_push_test',
    default_args=default_args,
    start_date=datetime(2023, 1, 10),
    schedule_interval='@once',
    tags=['stepik']
) as dag:

    t = PythonOperator(
        task_id='xcom_push',
        python_callable=print_context,
        dag=dag
    )

t




