import json
import logging
import os, glob
import pandas as pd
from utils.helpers import threaded, multiprocessed

def read_column_maps(path:str)->dict:
    with open(path, 'r') as fh: 
        columns_map = json.load(fh)
    return columns_map

# @multiprocessed
@threaded
def transform_coins(src_dir:str, tgt_dir:str, columns_map:dict):
    # retrieve column names
    column_map = columns_map['coins']
    
    # load source file
    src_path = f'{src_dir}/coinslist/coinslist.json'
    df = pd.read_json(src_path, orient='records', lines=True)
    df = df[column_map.keys()]
    
    # transform column names
    df.rename(columns=column_map, inplace=True)

    # save to target location
    tgt_path = f'{tgt_dir}/coins/coins.csv'
    if(os.path.exists(tgt_path)): os.remove(tgt_path)
    os.makedirs(f'{tgt_dir}/coins', exist_ok=True)
    df.to_csv(tgt_path, index=False)
    logging.info(f"Successfully transformed {src_path} to {tgt_path}.")

# @multiprocessed
@threaded
def transform_candlesticks(src_dir:str, tgt_dir:str, columns_map:dict):
    column_map = columns_map['candlesticks']
    
    # define target location
    os.makedirs(f'{tgt_dir}/candlesticks/', exist_ok=True)
    tgt_path = f'{tgt_dir}/candlesticks/candlesticks.csv'
    if(os.path.exists(tgt_path)): os.remove(tgt_path)

    # get all files required to transform
    files = glob.glob(f'{src_dir}/candlesticks/*.json')
    for idx,file in enumerate(files):
        df = pd.read_json(file, orient='records', lines=True)
        df.columns = df.columns.map(str)
        df.rename(columns=column_map, inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # insert coin id column at front
        coin_id = os.path.basename(file)[:-5]
        df.insert(loc=0,column='coin_id',value=coin_id)
        
        # append data at target file
        write_header = False if idx > 0 else True
        df.to_csv(tgt_path, mode='a', header=write_header, index=False)
    logging.info(f"Successfully transformed {idx+1} files to {tgt_path}.")

@threaded
def transform_news(src_dir:str, tgt_dir:str, columns_map:dict):
    column_map = columns_map['candlesticks']
    os.makedirs(f'{tgt_dir}/news/', exist_ok=True)
    tgt_path = f'{tgt_dir}/news/monthly_sentiments.csv'
    if(os.path.exists(tgt_path)): os.remove(tgt_path)

def transform_timestamp(src_dir:str, tgt_dir:str):
    os.makedirs(f'{tgt_dir}/time/', exist_ok=True)
    tgt_path = f'{tgt_dir}/time/time.csv'
    if(os.path.exists(tgt_path)): os.remove(tgt_path)

    # src_paths = [f'{src_dir}/candlesticks/candlesticks.csv', f'{src_dir}/news/news.csv']
    src_paths = [f'{src_dir}/candlesticks/candlesticks.csv']

    write_header = True
    new_columns = ['timestamp', 'day', 'month', 'year', 'weekday']

    for src in src_paths:
        data = pd.read_csv(src, chunksize=1000)
        timestamp_df = pd.DataFrame(columns=['timestamp'])
        for idx, chunk in enumerate(data):
            temp = pd.to_datetime(chunk['timestamp'].unique())
            temp = temp.to_frame(name='timestamp').dropna()
            if(timestamp_df.empty):
                timestamp_df = temp
                pass
            timestamp_df = pd.concat((timestamp_df, temp), ignore_index=True).drop_duplicates()

        timestamp_df[new_columns] = timestamp_df['timestamp'].apply(
                lambda x: pd.Series([x, x.day, x.month, x.year, x.day_of_week],
                                    index=new_columns)
            )
        timestamp_df.to_csv(tgt_path, mode='a', header=write_header, index=False)
        write_header = False
        logging.info(f"Successfully transformed {src} to {tgt_path}.")

    
    