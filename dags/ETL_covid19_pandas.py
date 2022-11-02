import pendulum
import requests
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.sql import SQLValueCheckOperator


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

        pg_hook = PostgresHook.get_hook('postgres_stage')
        pg_hook.run(f"INSERT INTO covid19_stage(date_of_data, iso_country, json_data) "
                    f"VALUES ('{parameters['date']}', '{parameters['iso']}', $${response}$$)")

        print(f'{parameters["iso"]} data for {parameters["date"]} loaded!')
    else:
        print(f'WARNING! {parameters["iso"]} data for {parameters["date"]} not loaded!')


def transform_load(**context):
    """
    Transform json data from postgresql in dataframes.
    concat dataframes and drop columns.
    Load new dataframe in postgres.
    """
    pg_hook = PostgresHook.get_hook('postgres_stage')

    records = pg_hook.get_records(f"SELECT json_data -> 'data' FROM covid19_stage WHERE date_of_data = '{ context['ds'] }'")

    total_df = pd.DataFrame()

    for row in records:
        df = pd.json_normalize(row[0])
        total_df = pd.concat([total_df, df], ignore_index=True)

    # transform
    total_df.drop(['region.cities', 'last_update', 'region.iso'], inplace=True, axis=1)
    total_df.drop(total_df[(total_df['region.province'] == 'Recovered') |
                           (total_df['region.province'] == 'Unknown')].index, inplace=True)

    target_fields = ['date', 'confirmed', 'deaths', 'recovered', 'confirmed_diff', 'deaths_diff', 'recovered_diff',
                     'active', 'active_diff', 'fatality_rate', 'name', 'region.province', 'region.lat',
                     'region.long']

    total_df = total_df.reindex(columns=target_fields)

    # load
    rows = list(total_df.itertuples(index=False, name=None))
    postgres_columns_name = ['date_day', 'confirmed', 'confirmed_diff', 'deaths', 'deaths_diff', 'recovered',
                              'recovered_diff', 'active', 'active_diff', 'fatality_rate', 'country', 'province',
                              'region_lat', 'region_long']

    pg_hook = PostgresHook.get_hook('postgres_warehouse')
    pg_hook.insert_rows(table='covid19_warehouse', rows=rows, target_fields=postgres_columns_name)


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
        'COVID19_daily_transform_pandas',
        default_args=default_args,
        start_date=pendulum.today('UTC').add(days=-3),
        schedule_interval=timedelta(days=1),
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

    transform_and_load_data = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_load,
        provide_context=True,
        dag=dag
    )

    data_quality = SQLValueCheckOperator(
        task_id='data_quality',
        sql=f"SELECT COUNT(1) FROM covid19_warehouse where date_day = '{ '{{ ds }}' }' ",
        pass_value=240,
        tolerance=0.02,
        conn_id='postgres_warehouse'
    )

    # success_telegram_message = TelegramOperator(
    #     task_id='success_telegram_message',
    #     telegram_conn_id='telegram',
    #     token=Variable.get('telegram_api_token'),
    #     chat_id=Variable.get('telegram_chat_id'),
    #     text='COVID19 data for {{ ds }} loaded!',
    #     dag=dag
    # )

    # extract_data >> transform_and_load_data >> data_quality >> success_telegram_message
    extract_data >> transform_and_load_data >> data_quality
