from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
#from batch import pull_data
import requests


def pull_data():
	url = "https://api.openweathermap.org/data/2.5/weather?q=Lviv&appid=d8169ca78476c40e04b22cdccd099fab"
	payload={}
	headers = {}

	response = requests.request("GET", url, headers=headers, data=payload)
	return response.text



with DAG(dag_id="weather", schedule_interval="@daily", start_date=days_ago(2)) as dag:
	'''create_table_sqlite_task = SqliteOperator(
		task_id="create_table_sqlite",
		sqlite_conn_id="measurements_db",
		sql=r"""
		    CREATE TABLE IF NOT EXISTS measurements (
			execution_time TIMESTAMP NOT NULL,
			temperature FLOAT
		);
		""",
	)'''
	extract_data = SimpleHttpOperator(
		http_conn_id="weather_api_con",
		endpoint="data/2.5/weather",
		data={"appid": "d8169ca78476c40e04b22cdccd099fab", "q": "Lviv"},
		method="GET",
		task_id="extract_data",
		log_response=True
	)
	extract_data
	#python_task1 = PythonOperator(task_id='python_task', python_callable=pull_data)
	#python_task1
	"""all_tasks = []
	for counter in range(2):
		python_task = PythonOperator(task_id=f'python_task_{counter}', python_callable=pull_data)
		all_tasks.append(python_task)
		python_task1 >> python_task
	#python_task1 = PythonOperator(task_id='python_task1', python_callable=pull_data)
	#python_task2 = PythonOperator(task_id='python_task2', python_callable=pull_data)
	python_task3 = PythonOperator(task_id='python_task3', python_callable=pull_data)
	
	all_tasks >> python_task3"""


	#requests.get("https://api.openweathermap.org/data/2.5/weather", data={"appid": "d8169ca78476c40e04b22cdccd099fab", "q": "Lviv"})
