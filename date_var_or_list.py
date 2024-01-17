import ast

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime


def my_func(**kwargs):
    partitions = kwargs["partitions"]

    if isinstance(partitions, str) and len(partitions) == 8 and partitions.isdigit():
        print(f"Using default execution date: {partitions}, partition: {partitions[:6]}")
        print(isinstance(partitions, str))
        partitions = list(str(partitions[:6]).split(','))
    else:
        try:
            partitions = ast.literal_eval(partitions)
            print(f"Custom partitions provided: {partitions}")
        except (ValueError, SyntaxError):
            print(f"Received unexpected partition format: {partitions}")
    return partitions


with DAG('date_var_or_list',
         schedule_interval='@daily',
         start_date=datetime(2023, 1, 1),
         catchup=False,
         tags=['u_m1dxa']) as dag:

     get_ds = PythonOperator(
        task_id="get_ds",
        python_callable=my_func,
        dag=dag,
        op_kwargs={"partitions": "{{ dag_run.conf.get('partitions', ds_nodash) }}"}
    )

get_ds