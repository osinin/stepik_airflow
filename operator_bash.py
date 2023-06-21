from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def simple_python(yest):
    print(f"Hello! yesterday date: {yest}")


with DAG(dag_id='operator_bash_simple_python',
         default_args={'owner': 'u_m1dxa'},
         schedule_interval='0 10 * * *',
         start_date=days_ago(1),
         tags=['stepik']
         ) as dag:
    bash_op_1 = BashOperator(
        task_id='FirstBashTask',
        bash_command='echo "It is first command"',
        dag=dag
    )
    pyth_op_1 = python_task = PythonOperator(
        task_id="FirstPythonTask",
        python_callable=simple_python,
        op_kwargs={
            'yest': days_ago(1)
        }
    )
    bash_op_1 >> pyth_op_1