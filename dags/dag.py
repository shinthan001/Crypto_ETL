
import os,json
from datetime import datetime
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from src import extract, transform, load
from src.utils.helpers import configure_root_logger

with DAG(dag_id = "Crypto_ETL",
    start_date=datetime(2025,2,1),
    schedule_interval="@monthly",
    catchup=False)as dag:

    load_dotenv('env/.env')
    configure_root_logger()
    meta_data_dir, data_dir = os.getenv('META_DATA_DIR'), os.getenv('DATA_SRC_DIR')
    
    extracted_dir = f'{data_dir}/extracted_data'
    endpoints_info = json.load(open(f'{meta_data_dir}/endpoints.json', 'r'))
    
    extract_coins = PythonOperator(
        task_id = 'extract_coins',
        python_callable=extract.process_coins,
        op_args=[endpoints_info, extracted_dir]
    )

    extract_candlesticks = PythonOperator(
        task_id = 'extract_candlesticks',
        python_callable=extract.process_candlesticks,
        op_args=[endpoints_info, extracted_dir]
    )

    extract_news = PythonOperator(
        task_id = 'extract_news',
        python_callable=extract.process_news,
        op_args=[endpoints_info, extracted_dir]
    )

    transformed_dir = f'{data_dir}/transformed_data'
    columns_map = json.load(open(f'{meta_data_dir}/columns_map.json', 'r'))

    transform_coins = PythonOperator(
        task_id = 'transform_coins',
        python_callable=transform.process_coins,
        op_args=[extracted_dir, transformed_dir, columns_map]
    )

    transform_candlesticks = PythonOperator(
        task_id = 'transform_candlesticks',
        python_callable=transform.process_candlesticks,
        op_args=[extracted_dir, transformed_dir, columns_map]
    )

    transform_news = PythonOperator(
        task_id = 'transform_news',
        python_callable=transform.process_news,
        op_args=[extracted_dir, transformed_dir, columns_map]
    )

    transform_timestamps = PythonOperator(
        task_id = 'transform_timestamps',
        python_callable=transform.process_timestamps,
        op_args=[transformed_dir, transformed_dir, columns_map]
    )

    sql = open(os.getenv('SQL_CREATE_TABLE')).read()
    conn_id = os.getenv('POSTGRES_CONN_ID')
    schemas = json.load(open(f'{meta_data_dir}/schemas.json'))
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=conn_id,
        sql=sql)

    load_coins = PythonOperator(
        task_id = 'load_coins',
        python_callable=load.load_bulk_data,
        op_args=['coins',schemas, f'{transformed_dir}/coins/coins.csv',conn_id]
    )

    load_candlesticks = PythonOperator(
        task_id = 'load_candlesticks',
        python_callable=load.load_bulk_data,
        op_args=['candlesticks',schemas, f'{transformed_dir}/candlesticks/candlesticks.csv',conn_id]
    )

    load_news = PythonOperator(
        task_id = 'load_news',
        python_callable=load.load_bulk_data,
        op_args=['news',schemas, f'{transformed_dir}/news/news.csv',conn_id]
    )

    load_timestamps = PythonOperator(
        task_id = 'load_timestamps',
        python_callable=load.load_bulk_data,
        op_args=['timestamps',schemas, f'{transformed_dir}/timestamps/timestamps.csv',conn_id]
    )
    
    extract_coins >> [extract_candlesticks,extract_news,transform_coins]
    extract_candlesticks >> transform_candlesticks >> transform_timestamps
    extract_news >> transform_news
    [transform_coins, transform_candlesticks, transform_news, transform_timestamps] >> create_table
    create_table >> [load_coins, load_candlesticks, load_news, load_timestamps]