# Устанавливает соединение с кластером Spark
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import dag, task
import logging
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='Spark_job',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['cost']
)
def new_dag():
    var = SparkSubmitOperator(
        task_id='test_op',
        application='/opt/workspace/get_data.py',
        conn_id='spark_conn',
        verbose=True
    )
    return var


dag = new_dag()
