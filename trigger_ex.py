from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from datetime import timedelta


dag = DAG(
    dag_id = 'trigger_ex',
    start_date = datetime(2022,8,24), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

def print_start():
    print("triger start")
    return "triger start"

print_hello = PythonOperator(
    task_id = 'print_start',
    python_callable = print_start,
    dag = dag)


trigger = TriggerDagRunOperator(
    task_id='trigger_example_dag',
    trigger_dag_id='HelloWorld',
    dag = dag
)


print_hello >> trigger
