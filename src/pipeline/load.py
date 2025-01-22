import os, json
import pandas as pd


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

def to_sql(df:pd.DataFrame, db_conn_uri:str, ds_name:str):
    df.to_sql(
        name=ds_name,
        con = db_conn_uri,
        if_exists='replace',
        index=False
    )




def load():
    data_src_dir = os.getenv('DATA_SRC_DIR')
    schemas = json.load(open(f'{data_src_dir}/schemas.json'))
    transformed_dir = f'{data_src_dir}/transformed_data'

    path = f'{transformed_dir}/news/news.csv'
    read_transformed_data(path, schemas)