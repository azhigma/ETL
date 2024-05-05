from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import json
import sqlalchemy.orm
import psycopg2

lat = '56.50' #latitude
lon = '60.35' #longitude
TOKEN = '' #your unique API key
user = '' # Location your database
password = ''
host = ''
database = ''

# dag = DAG(
#     dag_id='weather_etl',
#     start_date=datetime(2024,4,25),
#     schedule_interval='@daily'
# )

def get_data_from_api():
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={TOKEN}'
    response = requests.get(url)
    data = response.json()

    weather = []
    temp = []
    wind_speed = []

    for i in data:
        weather.append(data['weather']['main'])
        temp.append(data['main']['temp'])
        wend_speed(data['wind']['speed'])
    
    weather_dict = {
        'description': weather,
        'temperature': temp,
        'wind_speed': wind_speed
    }

    df = pd.DateFrame(weather_dict, columns=['descriprion', 'temperature', 'wind_speed'])
    print(df)
    
def create_table():
    
    engine = sqlalhemy.create_engine(DATABASE_LOCATION)
    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()

    sql_query = """ 
    CREATE TABLE IF NOT EXISTS weather_ekateriburg(
        description VARCHAR()
        temperature 
        wind_speed 
        CONSTRAINT primary_key_constraind PRIMARY KEY ()
    )
    """

    cursor.execute(sql_query)
    print('Opened database successfully')
    
    try:
        df.to_sql('weather_ekaterinburg', engine, index=False, if_axists='append')
    except:
        print('Data lready exists in the database')

    conn.close()
    print('Close database successfully')    

    stage1 = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_data_from_api,
        dag=dag
    )

    stage2 = PythonOperator(
        task_id = 'create_table', 
        python_callable=create_table,
        dag=dag
    )

stage1 >> stage2
