import os
import sys
import json
import logging
import threading
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta


def threaded(fn):
    def wrapper(*args, **kwargs):
        try:
            thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
            thread.start()
            return thread
        except Exception as err:
            logging.exception(f"Error occured in thread: {err}")
    return wrapper

def get_date(days=0, format="%Y-%m-%d"):
    now = datetime.now()
    date = (now + timedelta(days=days))
    date = date.strftime(format)
    return date

def to_dataframe(text:str)->pd.DataFrame:
    df = pd.read_json(StringIO(text))
    return df

def to_json(df:pd.DataFrame, file_to_save:str):
    if(len(df) == 0): return
    parent_dir = Path(file_to_save).parent.absolute()
    os.makedirs(f'{parent_dir}', exist_ok=True)
    try:
        df.to_json(file_to_save ,orient='records', lines=True)
    except Exception as err:
        raise(err)
    
    print(f"Successfully saved to {file_to_save}.\n")

def load_json_to_df(path:str)->pd.DataFrame:
    try:
        df = pd.read_json(path, orient='records',lines=True)
    except Exception as e:
        raise(e)
    return df
