from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import sqlalchemy

@dag(
    dag_id='weather_ekat',
    start_date=datetime(2024,4,25),
    schedule_interval='@hourly'
)

def get_weather():

    create_table = PostgresOperator(
        task_id='create_table',
        sql='sql/create_table.sql',
        postgres_conn_id='my_postgres'
    )

    @task
    def extract_data(): 
        lat = '56.50' #latitude
        lon = '60.35' #longitude
        TOKEN = '1e89b2fd03d6387a5479864d6c246237' #your unique API key
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={TOKEN}'
        response = requests.get(url)
        data = response.json()
        
        # Создание списков
        weather = [] 
        temp = []
        wind_speed = []
        date = []
  
  
        today = datetime.now() 
        date.append(today.date()) # извлечение даты и добавление в список
        weather.append(data['weather'][0]['main'])
        temp.append(data['main']['temp'])
        wind_speed.append(data['wind']['speed'])
    
 # создание словаря на основе имеющихся списков   
        weather_dict = {
            'date': date,
            'description': weather,
            'temperature': temp,
            'wind_speed': wind_speed
        }

# создание dataframe  на основе словаря
        df = pd.DataFrame(weather_dict, columns=['date', 'description', 'temperature', 'wind_speed'])
        print(df)

        df['temperature'] = df['temperature'].astype('float') - 273.15 # приводим к формату float from string, переводи из kernel в цельсий
        df['wind_speed'] = df['wind_speed'].astype('float') # приводим к формату float from string 

        return df

        @task
        def load_data(df):
              user = 'postgres' 
        password = '12345'
        host = 'localhost'
        port = '5438'
        db = 'postgres'
    
    # подключаемся к базе данных
        engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        df.to_sql(name='weather_ekat', con=engine, index=False, if_exists='append')


    create_table >> get_data >> load_data
