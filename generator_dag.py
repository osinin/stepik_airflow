from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator


def create_dag(dag_id,
               default_args,
               schedule='@daily'):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = DummyOperator(task_id=f'task', dag=dag)

    return dag


# build a dag for each number in range(10)
for n in range(1, 4):
    dag_id = f'generator_dag_{n+65}'

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1)
                    }

    globals()[dag_id] = create_dag(dag_id, default_args)