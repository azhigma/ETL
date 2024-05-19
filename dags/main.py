from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import requests
import json
import sqlalchemy

dag = DAG(
    dag_id='weather_etl',
    start_date=datetime(2024,4,25),
    schedule_interval='@hourly'
 )


create_table = PostgresOperator(
        task_id='create_table',
        sql='sql/create_table.sql',
        postgres_conn_id='my_postgres'
    )

# Получение данных о погоде с api
def get_load_data():
    # Необходимые данные для работы с api и базой данных 
    lat = '56.50' #latitude
    lon = '60.35' #longitude
    TOKEN = '' #your unique API key

    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={TOKEN}'
    response = requests.get(url)
    data = response.json()

# Создание списков
    weather = [] 
    temp = []
    wind_speed = []
    date = []
    time = []
  
    today = datetime.now() 
    date.append(today.date()) # извлечение даты и добавление в список
    time.append(today.time()) # извлечение времени и добавление в список

    weather.append(data['weather'][0]['main'])
    temp.append(data['main']['temp'])
    wind_speed.append(data['wind']['speed'])
    
 # создание словаря на основе имеющихся списков   
    weather_dict = {
        'date': date,
        'time': time,
        'description': weather,
        'temperature': temp,
        'wind_speed': wind_speed
    }

# создание dataframe  на основе словаря
    df = pd.DataFrame(weather_dict, columns=['date', 'time', 'description', 'temperature', 'wind_speed'])
    print(df)

    df['temperature'] = df['temperature'].astype('float') - 273.15 # приводим к формату float from string, переводи из kernel в цельсий
    df['wind_speed'] = df['wind_speed'].astype('float') # приводим к формату float from string 
    
    user = 'postgres' 
    password = ''
    host = 'localhost'
    port = '5432'
    db = 'ekat'
    
    # подключаемся к базе данных
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df.to_sql(name='weather_ekat', con=engine, index=False, if_exists='append')

get_load = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api,
    dag=dag
)

create_table >> get_load

