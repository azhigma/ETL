FROM apache/airflow:latest

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && install --no-cacke-dir -r /requirements.txt

USER root

RUN apt-get update && apt-get install -y wget

USER airflow