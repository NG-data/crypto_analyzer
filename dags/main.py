#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
# Устанавливает соединение с postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Устанавливает соединение с S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import logging
from datetime import datetime, timedelta


# In[15]:


# Функция загружает данные в S3, data-файл для загрузки, bucket - имя бакета, key - путь и название файла
def upload_to_s3(data, key, bucket='data'):
    logging.info('Создание подключения к S3')
    hook = S3Hook(aws_conn_id='minio')  # Установка подключения к S3

    try:
        # Проверяет есть ли бакет с таким именем
        if not hook.check_for_bucket(bucket):
            hook.create_bucket(bucket_name=bucket)
        logging.info('Подключение установлено')
    except Exception as e:  # Логирование ошибки
        logging.error(f'Ошибка подключения к minio, {e}')

    logging.info('Загрузка данных в S3')

    hook.load_string(  # Загружает строку в S3
        string_data=data,  # Сериализованные в строку данные
        key=key,  # Путь до файла в заданном бакете
        bucket_name=bucket,  # Название бакета
        replace=True
    )

    logging.info('Загрузка завершена успешно')

    # Возвращает путь до файла и название бакета
    return {'bucket': bucket, 'key': key}


# In[ ]:


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
with DAG(dag_id='ETL_crypto_data', default_args=default_args, schedule_interval='@hourly', catchup=False, tags=['cost']) as dag:

    # In[ ]:

    @task
    def conn_to_spark():
        spark = SparkSession \
            .builder \
            .appName("crypto_analyzer_local") \
            .master("spark://spark-master:7077") \
            .getOrCreate()
        try:
            test = spark.range(100)
            test.count()
            print('Соединение с кластером Spark установлено успешно')
        except Exception as e:
            print(f'Ошибка соединения {e}')


# In[16]:

    @task
    def extract_transform():
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": "BTCUSDT",
            "interval": "15",
            "limit": 5
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data_json = response.json()
            try:
                df = spark.createDataFrame(data_json['result']['list'], schema=['start_time', 'open', 'high', 'low', 'close', 'volume', 'usdt_volume']).withColumn('open', F.col('open').cast('float'))\
                    .withColumn('close', F.col('close').cast('float'))\
                    .withColumn('high', F.col('high').cast('float'))\
                    .withColumn('low', F.col('low').cast('float'))\
                    .withColumn('volume', F.col('volume').cast('float'))\
                    .withColumn('usdt_volume', F.col('usdt_volume').cast('float'))\
                    .withColumn('start_time', F.to_timestamp(F.col('start_time') / 1000))\
                    .orderBy(F.col('start_time'))
                logging.info(
                    'Формат данных успешно преобразован, начинаю загрузку в S3')
                df = df.toJSON()
                upload_to_s3(
                    df, f'transist_data/data_{datetime.date(datetime.now())}_')
            except:
                logging.error('Ошибка преобразование данных')
        else:
            logging.error('Ошибка соединения с API')


# In[ ]:

    conn_to_spark()
    extract_transform()
