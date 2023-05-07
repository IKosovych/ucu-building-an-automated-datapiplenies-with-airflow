import os
import logging

import requests
from geopy.geocoders import Nominatim

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


def get_city_coordinates(city_name: str):
	logging.info('get_city_coordinates')
	geolocator = Nominatim(user_agent='myapplication')
	location = geolocator.geocode(city_name)
	logging.info(location)
	return location[1]

def pull_weather_data(**kwargs):
	logging.info("pull_data")
	api_key = os.getenv('OPEN_WEATHER_API_KEY')
	url = "https://api.openweathermap.org/data/3.0/onecall?lat={}&lon={}&appid={}"
	payload={}
	headers = {}
	city_coordinates = get_city_coordinates(kwargs['city_name'])

	response = requests.request(
		"GET", url.format(city_coordinates[0], city_coordinates[1], api_key), headers=headers, data=payload
	)
	json_obj = response.json() if response.ok else {}
	temp = json_obj.get('current', {}).get('temp')
	humidity = json_obj.get('current', {}).get('humidity')
	clouds = json_obj.get('current', {}).get('clouds')
	wind_speed = json_obj.get('current', {}).get('wind_speed')
	logging.info(f'{temp}, {humidity}, {clouds}, {wind_speed}')
	return temp, humidity, clouds, wind_speed


with DAG(dag_id="weather", schedule_interval="@daily", start_date=days_ago(2)) as dag:
	start = DummyOperator(task_id = 'start')
	pull_operators = []
	values = ['Lviv', 'Kyiv']
	for counter, city_name in enumerate(values):
		logging.info("@@@@@@@")
		logging.info(counter)
		logging.info(type(counter))
		python_task = PythonOperator(
			task_id=f'pull_providers_{counter + 1}',
			python_callable=pull_weather_data,
			op_kwargs={'city_name': city_name},
		)
		pull_operators.append(python_task)
		start >> python_task

	end = DummyOperator(task_id = 'end')
	pull_operators >> end
