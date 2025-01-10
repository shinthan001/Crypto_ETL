import json
import pandas as pd


def to_dataframe(base_dir, sub_dir, fname):
    file_path = f'{base_dir}/{sub_dir}/{fname}.json'
    try:
        df = pd.read_json(file_path)
    except FileNotFoundError as err:
        raise (err)
    return df

def transform_coinslist():
    pass