import os
import json
import pandas as pd


def read_columns_map(path:str)->dict:
    try:
        with open(path, 'r') as f:
            json_doc = json.load(f)
    except FileNotFoundError as err:
        raise(err)
    return json_doc

def rename_columns(df:pd.DataFrame, column_map:dict)->pd.DataFrame:
    original_cols = column_map.keys()
    new_df = df[original_cols]
    new_df.rename(columns=column_map, inplace=True)
    return new_df

# def transform_to_coins(column_map:dict):
#     extracted_dir = os.getenv('EXTRACTED_DIR')
#     tgt_dir = os.getenv('TRANSFORMED_DIR')
#     df = to_dataframe(extracted_dir, 'coinslist', 'coinslist')
#     df = rename_columns(df, column_map)
#     return to_json(df, tgt_dir, 'coins', 'coins')

def transform_candlesticks(base_dir, tgt_dir, sub_dir, fname):
    pass
