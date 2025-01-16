import json
import logging
import os, glob
import pandas as pd
from pathlib import Path
from utils.helpers import threaded, multiprocessed

def read_column_maps(path:str)->dict:
    with open(path, 'r') as fh: 
        columns_map = json.load(fh)
    return columns_map

def transform_col_names(df:pd.DataFrame, column_map:dict)->pd.DataFrame:
    df.columns = df.columns.map(str)
    df = df[column_map.keys()]
    df.rename(columns=column_map, inplace=True)
    return df

def save_df_2_csv(df:pd.DataFrame, tgt_path:str, mode='w', header=True):
    # save to target location
    tgt_dir = Path(tgt_path).parent.absolute()
    os.makedirs(f'{tgt_dir}', exist_ok=True)
    df.to_csv(tgt_path, mode=mode, header=header, index=False)



@threaded
# @multiprocessed
def transform_coins(src_dir:str, tgt_dir:str, columns_map:dict):
    
    # load source file
    src_path = f'{src_dir}/coinslist/coinslist.json'
    df = pd.read_json(src_path, orient='records', lines=True)
    
    # retrieve column names
    column_map = columns_map['coins']
    df = transform_col_names(df, column_map)

    # save to target location
    tgt_path = f'{tgt_dir}/coins/coins.csv'
    save_df_2_csv(df, tgt_path)
    logging.info(f"Successfully transformed {src_path} to {tgt_path}.")


@threaded
# @multiprocessed
def transform_candlesticks(src_dir:str, tgt_dir:str, columns_map:dict):
    column_map = columns_map['candlesticks']
    
    # define target location
    tgt_path = f'{tgt_dir}/candlesticks/candlesticks.csv'
    write_header = True

    # get all files required to transform
    files = glob.glob(f'{src_dir}/candlesticks/*.json')
    for idx,file in enumerate(files):
        df = pd.read_json(file, orient='records', lines=True)
        df = transform_col_names(df, column_map)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # insert coin id column at front
        coin_id = os.path.basename(file)[:-5]
        df.insert(loc=0,column='coin_id',value=coin_id)
        
        # append data at target file
        save_df_2_csv(df, tgt_path, mode='w' if write_header else 'a', 
                      header=write_header)
        write_header = False   
    logging.info(f"Successfully transformed {idx+1} files to {tgt_path}.")



@threaded
def transform_news(src_dir:str, tgt_dir:str, columns_map:dict):
    column_map = columns_map['candlesticks']
    os.makedirs(f'{tgt_dir}/news/', exist_ok=True)
    tgt_path = f'{tgt_dir}/news/monthly_sentiments.csv'
    if(os.path.exists(tgt_path)): os.remove(tgt_path)



def transform_timestamp(src_dir:str, tgt_dir:str):
    """
    This module transforms timestamps from ./transformed_data/candlesticks.csv and 
    ./transformed_data/news.csv. Hence, it doesn't have standalone extracted source 
    and need column map.
    """
    tgt_path = f'{tgt_dir}/time/time.csv'

    # src_paths = [f'{src_dir}/candlesticks/candlesticks.csv', f'{src_dir}/news/news.csv']
    src_paths = [f'{src_dir}/candlesticks/candlesticks.csv']

    new_columns = ['timestamp', 'day', 'month', 'year', 'weekday']
    write_header = True

    for src in src_paths:
        data = pd.read_csv(src, chunksize=1000)
        timestamp_df = pd.DataFrame(columns=['timestamp'])

        for _,chunk in enumerate(data):
            temp = pd.to_datetime(chunk['timestamp'].unique())
            temp = temp.to_frame(name='timestamp').dropna()
            
            if(not timestamp_df.empty):
                timestamp_df = pd.concat((timestamp_df, temp), ignore_index=True).drop_duplicates()
                pass
            
            timestamp_df = temp # assigned first chunk to empty df
            
        # breakdown timestamp to day, month, year, weekday
        timestamp_df[new_columns] = timestamp_df['timestamp'].apply(
                lambda x: pd.Series([x, x.day, x.month, x.year, x.day_of_week],
                                    index=new_columns)
            )
        
        save_df_2_csv(timestamp_df,tgt_path,mode='a' if write_header else 'w', index = False)
        write_header = False
        logging.info(f"Successfully transformed {src} to {tgt_path}.")

    
    