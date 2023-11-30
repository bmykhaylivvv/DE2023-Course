import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

cities_coordinates = {
    "lviv":      ("49.8397", "24.0297"),
    "kyiv":      ("50.4501", "30.5234"),
    "kharkiv":   ("49.9935", "36.2304"),
    "odesa":     ("46.4825", "30.7233"),
    "zhmerynka": ("49.0384", "28.1056")
}

def _transform_api_data(ti, city, extract_data_task_id):
    extracted_data = ti.xcom_pull(extract_data_task_id)

    timestamp = extracted_data["data"][0]["dt"]
    temp = extracted_data["data"][0]["temp"]
    hum = extracted_data["data"][0]["humidity"]
    clouds = extracted_data["data"][0]["clouds"]
    wind_speed = extracted_data["data"][0]["wind_speed"]

    timestamp_int = int(timestamp)
    dt_object = datetime.utcfromtimestamp(timestamp_int)
    formatted_date = dt_object.strftime("%m/%d/%Y")

    return city, formatted_date, temp, hum, clouds, wind_speed


def convert_data_for_api_query(date_string):
    date_format = "%Y-%m-%d"
    datetime_object = datetime.strptime(date_string, date_format)
    timestamp = int(datetime.timestamp(datetime_object))

    return timestamp


with DAG(
    dag_id="DE_HW1_OpenWeatherMap_Bohdan_Mykhailiv",
    schedule_interval="@daily",
    start_date=datetime(2023, 11, 28),
    catchup=True,
    user_defined_macros={"convert_data_for_api_query": convert_data_for_api_query},
) as dag:
    create_data_table = PostgresOperator(
        task_id="create_data_table",
        postgres_conn_id="postgres_homework_1",
        sql="""
        CREATE TABLE IF NOT EXISTS WeatherMeasures (
            id  SERIAL PRIMARY KEY,
            city VARCHAR(128),
            timestamp TIMESTAMP,
            temp FLOAT,
            humidity FLOAT,
            clouds FLOAT,
            windSpeed FLOAT
        );
        """,
    )

    for city, coordinates in cities_coordinates.items():
        test_api = HttpSensor(
            task_id=f"test_api_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            request_params={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": coordinates[0],
                "lon": coordinates[1],
                "dt": "{{ convert_data_for_api_query(ds) }}",
            },
        )
        extract_api_data = SimpleHttpOperator(
            task_id=f"extract_api_data_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": coordinates[0],
                "lon": coordinates[1],
                "dt": "{{ convert_data_for_api_query(ds) }}",
            },
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True,
        )
        transform_api_data = PythonOperator(
            task_id=f"transform_api_data_{city}",
            python_callable=_transform_api_data,
            provide_context=True,
            op_kwargs={"city": city, "extract_data_task_id": f"extract_api_data_{city}"},
        )
        load_api_data_to_db = PostgresOperator(
            task_id=f"load_api_data_to_db_{city}",
            postgres_conn_id="postgres_homework_1",
            sql="""
                INSERT INTO WeatherMeasures (city, timestamp, temp, humidity, clouds, windSpeed) 
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
            parameters=[
                "{{ti.xcom_pull(task_ids='transform_api_data_" + city + "')[0]}}",
                "{{ti.xcom_pull(task_ids='transform_api_data_" + city + "')[1]}}",
                "{{ti.xcom_pull(task_ids='transform_api_data_" + city + "')[2]}}",
                "{{ti.xcom_pull(task_ids='transform_api_data_" + city + "')[3]}}",
                "{{ti.xcom_pull(task_ids='transform_api_data_" + city + "')[4]}}",
                "{{ti.xcom_pull(task_ids='transform_api_data_" + city + "')[5]}}",
            ],
        )

        create_data_table >> test_api >> extract_api_data >> transform_api_data >> load_api_data_to_db
