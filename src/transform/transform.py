import json
import logging
import threading
import os, glob
import pandas as pd
from utils.helpers import threaded

@threaded
def transform_coins(src_dir:str, tgt_dir:str, columns_map:dict)->threading.Thread:
    # retrieve column names
    column_map = columns_map['coins']
    # load source file
    src_path = f'{src_dir}/coinslist/coinslist.json'
    df = pd.read_json(src_path, orient='records', lines=True)
    df = df[column_map.keys()]
    # transform column names
    df.rename(column_map, inplace=True)
    # save to target location
    tgt_path = f'{tgt_dir}/coins/coins.csv'
    os.makedirs(tgt_dir, exist_ok=True)
    df.to_csv(tgt_path, index=False)
    logging.info(f"Successfully transformed {src_path} to {tgt_path}.\n")

@threaded
def transform_candlesticks(src_dir:str, tgt_dir:str, columns_map:dict)->threading.Thread:
    column_map = columns_map['candlesticks']
    # define target location
    os.makedirs(f'{tgt_dir}/candlesticks/', exist_ok=True)
    tgt_path = f'{tgt_dir}/candlesticks/candlesticks.csv'
    # get all files required to transform
    files = glob.glob(f'{src_dir}/candlesticks/*.json')
    for idx,file in enumerate(files):
        df = pd.read_json(file, orient='records', lines=True)
        df.rename(column_map, inplace=True)
        # insert coin id column at front
        coin_id = os.path.basename(file)[:-5]
        df.insert(loc=0,column='coin_id',value=coin_id)
        # append data at target file
        write_header = False if idx > 0 else True
        df.to_csv(tgt_path, mode='a', header=write_header, index=False)
    logging.info(f"Successfully transformed {idx+1} files to {tgt_path}.\n")


def transform_timestamp(src_dir:str, tgt_dir:str):
    os.makedirs(f'{tgt_dir}/time/', exist_ok=True)
    tgt_path = f'{tgt_dir}/time/time.csv'
    # src_paths = [f'{src_dir}/candlesticks/candlesticks.json', f'{src_dir}/news/news.json']
    src_paths = [f'{src_dir}/candlesticks/candlesticks.json']
    for src in src_paths:
        data = pd.read_json(src, chunksize=1000)
        for chunk in data:
            time_data = data['timestamp']


@threaded
def transform_news(src_dir:str, tgt_dir:str, columns_map:dict)->threading.Thread:
    column_map = columns_map['candlesticks']
    os.makedirs(f'{tgt_dir}/news/', exist_ok=True)
    tgt_path = f'{tgt_dir}/news/monthly_sentiments.csv'