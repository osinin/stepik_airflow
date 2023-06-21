import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.dag import get_last_dagrun
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session
from airflow.models.dagrun import DagRun

from datetime import datetime


def get_dag_runs(**kwargs):
    dag_id = kwargs["dag_id"]
    dag_runs = DagRun.find(dag_id=dag_id)
    columns = ['dag_id', 'start_date', 'end_date', 'execution_date', 'state', 'conf']
    df = pd.DataFrame([], columns=columns)
    for dag_run in dag_runs:
        data = [dag_run.dag_id, str(dag_run.start_date), str(dag_run.end_date), str(dag_run.execution_date),
                dag_run.state, dag_run.conf]
        print(data)
        df = df.append(pd.DataFrame([data], columns=columns))
    print(df.head(5))

    last_dag_run = get_last_dagrun(dag_id=dag_id, session=Session, include_externally_triggered=True)
    if last_dag_run is None:
        raise AirflowException("No previous run")
    else:
        print(f"last_dag_run: {last_dag_run.end_date}")
        #return [last_dag_run.end_date, last_dag_run.state]

with DAG('last_dag_run',
         schedule_interval='@daily',
         start_date=datetime(2023, 1, 1),
         tags=['u_m1dxa']) as dag:

     last_dag = PythonOperator(
        task_id="last_dag",
        python_callable=get_dag_runs,
        dag=dag,
        op_kwargs={"ds": "{{ ds }}",
         "dag_id": "logical_date"}
    )

last_dag