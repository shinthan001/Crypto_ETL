import os,logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def load_bulk_data(table, schemas, src_file, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    columns = _get_column_names(schemas, table)
    columns = ','.join(columns)
    logging.info(f"Loading data from {src_file} to {table}")
    
    sql = f"COPY {table}({columns}) FROM stdin WITH DELIMITER as ','"    
    hook.copy_expert(
        sql=sql,
        filename=src_file
    )
