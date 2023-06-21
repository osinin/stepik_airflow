from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.telegram.operators.telegram import TelegramOperator


def success():
    pass


def skip():
    raise AirflowSkipException


def failed():
    raise AirflowFailException


def tg_message(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_id',
        chat_id='-1001839105898',
        text='Task failed!',
        dag=dag)
    return send_message.execute(context=context)


with DAG(dag_id='tg_notification',
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         on_failure_callback=tg_message,
         tags=['stepik']
         ) as dag:

    task_0 = PythonOperator(
      task_id='success',
      python_callable=success,
      dag=dag
    )

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=skip,
        dag=dag
    )

    task_2 = PythonOperator(
      task_id='failed',
      python_callable=failed,
      dag=dag
    )

task_0 >> task_1 >> task_2
