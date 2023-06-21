from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def update_tokens() -> None:
    d = {
        "access_token": "test_tok", 
        "token_type": "bearer", "expires_in": 604800, 
        "refresh_token_expires_in": 1209600, 
        "refresh_token": "123"
    }
    access_token = d["access_token"]
    refresh_token = d["refresh_token"]
    Variable.set("huntflow_access_token", access_token)
    Variable.set("huntflow_refresh_token", refresh_token)


def get_vars() -> None:
    access_token = Variable.get("huntflow_access_token")
    refresh_token = Variable.get("huntflow_refresh_token")
    print(f'new access token is: {access_token}')
    print(f'new refresh token is: {refresh_token}')


with DAG(dag_id='variables_testing',
         default_args={'owner': 'u_m1dxa'},
         schedule_interval='30 22 * * *',
         start_date=days_ago(1),
         tags = ['stepik']
    ) as dag:

    refresh_tokens = PythonOperator(
    task_id='refresh_tokens',
    python_callable=update_tokens,
    dag=dag
    )

    get_tokens = PythonOperator(
        task_id='get_tokens',
        python_callable=get_vars,
        dag=dag
    )

refresh_tokens >> get_tokens