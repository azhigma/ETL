from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import json
import sqlalchemy
import psycopg2


# Необходимые данные для работы с api и базой данных 
lat = '56.50' #latitude
lon = '60.35' #longitude
TOKEN = '' #your unique API key
user = 'postgres' 
password = '12345'
host = 'localhost'
port = '5438'
db = 'postgres'

dag = DAG(
    dag_id='weather_etl',
    start_date=datetime(2024,4,25),
    schedule_interval='@hourly'
 )

# Получение данных о погоде с api
def get_data_from_api():
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

# цикла для добавления в листы данных об описании погоды, температуры и скорости ветра
    for i in data:
        weather.append(data['weather'][0]['main'])
        temp.append(data['main']['temp'])
        wend_speed.append(data['wind']['speed'])
    
 # создание словаря на основе имеющихся списков   
    weather_dict = {
        'date': date,
        'time': time,
        'description': weather,
        'temperature': temp,
        'wind_speed': wind_speed
    }

# создание dataframe  на основе словаря
    df = pd.DateFrame(weather_dict, columns=['date', 'time', 'descriprion', 'temperature', 'wind_speed'])
    print(df)
    return df
    

def transform_data(df):
    df['temperature'] = df['temperature'].astype('float') - 273.15 # приводим к формату float from string, переводи из kernel в цельсий
    df['wind_speed'] = df['wind_speed'].astype('float') # приводим к формату float from string 
    return df


def load_data(df):
    
    # подключаемся к базе данных
    engine = sqlalhemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()

    # создаем таблицу
    sql_query = """  
    CREATE TABLE IF NOT EXISTS weather_ekat(
        id SERIAL PRIMARY KEY,
        date DATE,
        time TIME,
        description VARCHAR(50),
        temperature REAL,
        wind_speed REAL
    )
    """

    cursor.execute(sql_query)
    print('Opened database successfully')
    
    try:
        df.to_sql('weather_ekat', engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')

    conn.close()
    print('Close database successfully')    

    stage1 = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_data_from_api,
        dag=dag
    )

    stage2 = PythonOperator(
        task_id = "transform_data", 
        python_callable=transform_data,
        dag=dag
    )

    stage3 = PythonOperator(
        task_id = 'load_data', 
        python_callable=create_table,
        dag=dag
    )

stage1 >> stage2 >> stage3
