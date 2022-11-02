import pendulum
import requests
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.sql import SQLValueCheckOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'Bmk',
    'depends_on_past': False,
    'email': ['Elfuses@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def extract(**context):
    """
    Connect to api
    Load data to postgresql (jsonb type)
    """

    # API parameters
    headers = {
        "X-RapidAPI-Key": Variable.get("API_KEY"),
        'X-RapidAPI-Host': 'covid-19-statistics.p.rapidapi.com'
    }

    url = 'https://covid-19-statistics.p.rapidapi.com/reports'

    parameters = {'date': context['ds'],
                  'iso': context['templates_dict']['iso']
                  }

    response = requests.request('GET', url, headers=headers, params=parameters, timeout=60)

    if response.status_code == 200:
        response = response.text

        pg_hook = PostgresHook.get_hook('postgres_stage')
        pg_hook.run(f"INSERT INTO covid19_stage(date_of_data, iso_country, json_data) "
                    f"VALUES ('{parameters['date']}', '{parameters['iso']}', $${response}$$)")

        print(f'{parameters["iso"]} data for {parameters["date"]} loaded!')
    else:
        print(f'WARNING! {parameters["iso"]} data for {parameters["date"]} not loaded!')


def on_failure_telegram_message(context):
    """ When dag fail sending message to telegram """
    fail_telegram_message = TelegramOperator(
        task_id='one_fail_telegram_message',
        telegram_conn_id='telegram',
        chat_id='471613323',
        text=f'TASK FAILED!! \n '
             f'dag_id: {context.get("task_instance").dag_id} \n'
             f'execution_date: {context.get("task_instance").execution_date}',
        dag=dag
    )

    return fail_telegram_message.execute(context=context)


with DAG(
        'COVID19_daily_transform_spark',
        default_args=default_args,
        start_date=pendulum.today('UTC').add(days=-1),
        schedule_interval=timedelta(days=1),
        catchup=True,
        max_active_runs=1,
        on_failure_callback=on_failure_telegram_message,
        tags=['covid19', 'ETL', 'spark']
) as dag:

    extract_data = [PythonOperator(
        task_id='extract_api_' + iso,
        python_callable=extract,
        templates_dict={'iso': iso},
        dag=dag
    ) for iso in ['CHN', 'RUS', 'USA', 'IND', 'BRA']]

    spark_transform_data = SparkSubmitOperator(
        task_id='spark_transform',
        conn_id='spark_standalone',
        name='covid19_transform',
        conf={'spark.driver.airflow_jinja_ds': '{{ ds }}'},
        application='/opt/airflow/spark/apps/spark_transform_data.py',
        total_executor_cores=2,
        executor_memory='512M',
        jars='/opt/airflow/spark/jars/postgresql-42.3.6.jar',
        dag=dag)

    data_quality = SQLValueCheckOperator(
        task_id='data_quality',
        sql=f"SELECT COUNT(1) FROM covid19_warehouse where date = '{ '{{ ds }}' }' ",
        pass_value=240,
        tolerance=0.02,
        conn_id='postgres_warehouse'
    )
    # success_telegram_message = TelegramOperator(
    #     task_id='success_telegram_message',
    #     telegram_conn_id='telegram',
    #     chat_id=Variable.get("chat_id"),
    #     text='COVID19 data for {{ ds }} loaded!',
    #     dag=dag
    # )

    # extract_data >> spark_transform_data >> data_quality >> success_telegram_message
    extract_data >> spark_transform_data >> data_quality

