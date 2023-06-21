from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def python_example(**context):
    print("Контекст", context)
    print("ds:", context['ds'])

default_args = {
    'owner': 'u_m1dxa',
    'retries': 0
}

with DAG (
    'task_instanse_context_test',
    default_args=default_args,
    start_date=datetime(2023, 1, 10),
    schedule_interval="@once",
    tags=['stepik']
) as dag:

    pyth = PythonOperator(
        task_id='python_instance',
        python_callable=python_example,
        dag=dag
    )

    sh = BashOperator(
        task_id='bash_instance',
        bash_command='echo "Context: \'$message\'"',
        env={
            'message': '{{ execution_date }}'
        },
        dag=dag
    )

pyth >> sh