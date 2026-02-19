from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import json
import pandas as pd

def kelvin_to_fahrenheit(temp_kelvin):
    temp_in_fahrenheit = (9/5)*(temp_kelvin - 273) + 32
    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data['name']
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data["dt"] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temperature (f)": min_temp_fahrenheit,
        "Maximum Temperature (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise Time": sunrise_time,
        "Sunset Time": sunset_time

    }

    df_transformed = [transformed_data]
    df = pd.DataFrame(df_transformed)
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    dt_string = 'current_weather_data_jakarta'  + now
    aws_credentials = {
        "key": "<XXXXXXXXXXXXXXXX>",
        "secret": "<XXXXXXXXXXXXXXXX>",
        "token": "<XXXXXXXXXXXXXXXX>"
    }
    df.to_csv(f's3://weather-api-airflow-9/{dt_string}.csv', index=False, storage_options=aws_credentials)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 19),
    'email': ['<XXXXXXXX@gmail.com>'],
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

        is_weather_api_ready = HttpSensor(
            task_id='is_weather_api_ready',
            http_conn_id='weathermap_api',
            endpoint='/data/2.5/weather?q=Jakarta&appid=<type your_api_key_here>'
        )

        extract_weather_data = SimpleHttpOperator(
            task_id='extract_weather_data',
            http_conn_id='weathermap_api',
            endpoint='/data/2.5/weather?q=Jakarta&appid=<type your_api_key_here>',
            method='GET',
            response_filter=lambda r:json.loads(r.text),
            log_response=True
        )

        transform_load_weather_data = PythonOperator(
            task_id='transform_load_weather_data',
            python_callable=transform_load_data #panggil function transform_load_data di sini
        )
        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data