import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


spark = SparkSession \
    .builder \
    .appName("crypto_analyzer_local") \
    .getOrCreate()
try:
    test = spark.createDataFrame(
        [(1, 2, 3), (4, 5, 6)], ['a', 'b', 'c'])
    test.show()
    print('Соединение с кластером Spark установлено успешно')
except Exception as e:
    print(f'Ошибка соединения {e}')
