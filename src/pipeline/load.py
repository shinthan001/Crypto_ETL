import os, json, logging
import pandas as pd
from utils.timer import timer

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_transformed_data(path, schemas):
    file_name = os.path.basename(path)
    ds_name = file_name.split('.')[0]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(path, names=columns, chunksize=10000)
    return df_reader

def to_sql(df:pd.DataFrame, db_conn_uri:str, ds_name:str, mode:str):
    df.to_sql(
        name=ds_name,
        con = db_conn_uri,
        if_exists=mode,
        index=False
    )

def load_data_to_db(src_dir, ds_name, schemas, db_conn_uri, mode):
    path = f'{src_dir}/{ds_name}/{ds_name}.csv'
    df_reader = read_transformed_data(path, schemas)
    for _,df in enumerate(df_reader):
        to_sql(df, db_conn_uri, ds_name, mode)

@timer
def load(ds_names:str=None):
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

    data_src_dir = os.getenv('DATA_SRC_DIR')
    schemas = json.load(open(f'{data_src_dir}/schemas.json'))
    src_dir = f'{data_src_dir}/transformed_data'

    if(not ds_names): ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            logging.info(f"Procesing {ds_name}")
            mode = 'replace' if(ds_name == 'coins') else 'append' 
            load_data_to_db(src_dir, ds_name, schemas, db_conn_uri, mode)
        except NameError as ne:
            logging.error(ne); pass
        except Exception as e:
            logging.error(e); pass
