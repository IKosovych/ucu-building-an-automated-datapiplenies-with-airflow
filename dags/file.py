from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="first_dag", schedule_interval="@daily", start_date=days_ago(2)) as dag:
	b = BashOperator(task_id="simple_command", bash_command='echo "Hello World!"')