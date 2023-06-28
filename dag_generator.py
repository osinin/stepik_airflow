from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


# Функция создания Дагов
def create_dag(dag_id, dag_number, default_args, schedule='@daily'):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['stepik'])

    with dag:
        t1 = DummyOperator(task_id=f'task', dag=dag)

    return dag


# Код для генерации дагов через range()
for dag_number in range(1, 4):
    dag_id = f'generator_dag_{dag_number}'

    # Настройки по умолчанию
    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1)
                    }

    # globals возвращает словарь с глобальной таблицей объектов — словарь текущего модуля.
    globals()[dag_id] = create_dag(dag_id, dag_number, default_args)