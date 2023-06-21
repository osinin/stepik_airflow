import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


git_url = 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/'
api_url = 'https://api.exchangerate.host/timeseries?'


def push(**kwargs):
    ds = kwargs['ds']
    print(f'logical date is {ds}')
    # reading from github csv
    df = pd.read_csv(f'{git_url}{ds}.csv')
    df_key = str(df['date'][1])
    df_val = str(df['value'][1])
    kwargs['ti'].xcom_push(key=df_key, value=df_val)
    # get from API
    response = requests.get(f'{api_url}start_date={ds}&end_date={ds}')
    response_data = response.json()
    response_key = f'response_{ds}'
    response_val = str(response_data['rates'][ds]['RUB'])
    kwargs['ti'].xcom_push(key=response_key, value=response_val)


def pull(**kwargs):
    ds = kwargs['ds']
    print(f'logical date is {ds}')
    print(f"git: {kwargs['ti'].xcom_pull(key=ds, task_ids='push')}")
    print(f"api: {kwargs['ti'].xcom_pull(key=f'response_{ds}', task_ids='push')}")


args = {'owner': 'airflow',
        'start_date': datetime(2020, 1, 1),
        'end_date': datetime(2025, 1, 4),
        'provide_context': True}

with DAG('url_xcom',
          schedule_interval='@once',
          default_args=args,
          tags=['stepik']
         ) as dag:

    push1 = PythonOperator(
        task_id='push',
        dag=dag,
        python_callable=push)
    pull1 = PythonOperator(
        task_id='pull',
        dag=dag,
        python_callable=pull)

    push1 >> pull1



