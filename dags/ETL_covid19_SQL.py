import pendulum
import requests
import pandas as pd
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.sql import SQLValueCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'Bmk',
    'depends_on_past': False,
    'email': ['Elfuses@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


def extract(**context):
    """
    Connect to api
    Load data to postgresql (jsonb type)
    """

    # API parameters
    headers = {
        "X-RapidAPI-Key": Variable.get("api_key"),
        'X-RapidAPI-Host': 'covid-19-statistics.p.rapidapi.com'
        }

    url = 'https://covid-19-statistics.p.rapidapi.com/reports'

    parameters = {'date': context['ds'],
                  'iso': context['templates_dict']['iso']
                  }

    response = requests.request('GET', url, headers=headers, params=parameters, timeout=60)

    if response.status_code == 200:
        response = response.text

        pg_hook = PostgresHook.get_hook('postgres_db')
        pg_hook.run(f"INSERT INTO public.covid19_json(date_of_data, iso_country, json_data) "
                    f"VALUES ('{parameters['date']}', '{parameters['iso']}', $${response}$$)")

        print(f'{parameters["iso"]} data for {parameters["date"]} loaded!')
    else:
        print(f'WARNING! {parameters["iso"]} data for {parameters["date"]} not loaded!')


def on_failure_telegram_message(context):
    """ When dag fail sending message to telegram """
    fail_telegram_message = TelegramOperator(
        task_id='one_fail_telegram_message',
        telegram_conn_id='telegram',
        token=Variable.get('telegram_api_token'),
        chat_id=Variable.get('telegram_chat_id'),
        text=f'TASK FAILED!! \n '
             f'dag_id: {context.get("task_instance").dag_id} \n'
             f'execution_date: {context.get("task_instance").execution_date}',
        dag=dag
    )

    return fail_telegram_message.execute(context=context)


with DAG(
        'COVID19_daily_transform_SQL',
        default_args=default_args,
        start_date=dt.datetime(2023, 0o1, 0o1),
        end_date=dt.datetime(2023, 0o1, 0o5),
        schedule_interval=dt.timedelta(days=1),
        catchup=True,
        on_failure_callback=on_failure_telegram_message,
        max_active_runs=1,
        tags=['covid19', 'ETL', 'pandas']
) as dag:

    extract_data = [PythonOperator(
        task_id='extract_api_' + iso,
        python_callable=extract,
        templates_dict={'iso': iso},
        dag=dag
    ) for iso in ['CHN', 'RUS', 'USA', 'IND', 'BRA']]

    transform_data = PostgresOperator(
        task_id='clear_and_transform_data',
        postgres_conn_id='postgres_db',
        sql=f"CALL insert_in_covid19_table('{ '{{ ds }}' }');"
            f"DELETE from public.covid19_table ct where region_name IN ('Unknown', 'Recovered');"
    )

    data_quality = SQLValueCheckOperator(
        task_id='data_quality',
        sql=f"SELECT COUNT(1) FROM covid19_table where day_of_data = '{ '{{ ds }}' }';",
        pass_value=240,
        tolerance=0.02,
        conn_id='postgres_db'
    )

    load_data = PostgresOperator(
        task_id='load_to_data_mart',
        postgres_conn_id='postgres_db',
        sql=f"call insert_fk_in_covid19_table('{ '{{ ds }}' }');"
            f"CALL insert_in_fact_covid19('{ '{{ ds }}' }');"
    )


    # success_telegram_message = TelegramOperator(
    #     task_id='success_telegram_message',
    #     telegram_conn_id='telegram',
    #     token=Variable.get('telegram_api_token'),
    #     chat_id=Variable.get('telegram_chat_id'),
    #     text='COVID19 data for {{ ds }} loaded!',
    #     dag=dag
    # )

    # extract_data >> transform_data >> data_quality >> load_data >> success_telegram_message
    extract_data >> transform_data >> data_quality >> load_data
