import pandas as pd
import sqlite3

CON = sqlite3.connect('example.db')


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator


def extract_data(url, tmp_file, **context) -> pd.DataFrame:
    """ Extract CSV
    """
    pd.read_csv(url).to_csv(tmp_file) # Изменение to_csv
    print(f'tmp_file_name: {tmp_file}')


def transform_data(group, agreg, tmp_file, tmp_agg_file, **context) -> pd.DataFrame:
    """ Group by data
    """
    data = pd.read_csv(tmp_file) # Изменение read_csv
    data.groupby(group).agg(agreg).reset_index().to_csv(tmp_agg_file) # Изменение to_csv


def load_data(tmp_file, table_name, conn=CON, **context) -> None:
    """ Load to DB
    """
    data = pd.read_csv(tmp_file)# Изменение read_csv
    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists='replace', index=False)


with DAG(dag_id='oprator_email_csv',
         default_args={'owner': 'u_m1dxa'},
         schedule_interval='30 22 * * *',
         start_date=days_ago(1),
         tags=['stepik']
    ) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
            'url': 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/data.csv',
            'tmp_file': '/outputs/lesson_1.csv'},
        dag=dag
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
        op_kwargs={
            'tmp_file': '/outputs/lesson_1.csv',
            'tmp_agg_file': '/outputs/lesson_1_agg.csv',
            'group': ['A', 'B', 'C'],
            'agreg': {"D": sum}}
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag,
        op_kwargs={
            'tmp_file': '/outputs/lesson_1_agg.csv',
            'table_name': 'table'
        }
    )

    email_op = EmailOperator(
        task_id='send_email',
        to="osinin.dm@yandex.ru",
        subject="Test Email Please Ignore",
        html_content=None,
        files=['/outputs/lesson_1_agg.csv']
    )


    extract_data >> transform_data >> [load_data, email_op]

    