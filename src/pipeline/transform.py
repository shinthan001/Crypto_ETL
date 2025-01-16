import json
import logging
import os, glob
import pandas as pd
from pathlib import Path
from datetime import datetime

from utils.timer import timer
from utils.helpers import threaded, multiprocessed
from utils.text_processing import sentiment_pipeline

def read_column_maps(path:str)->dict:
    with open(path, 'r') as fh: 
        columns_map = json.load(fh)
    return columns_map

def transform_col_names(df:pd.DataFrame, column_map:dict)->pd.DataFrame:
    df.columns = df.columns.map(str)
    df = df[column_map.keys()]
    df = df.rename(columns=column_map)
    return df

def save_df_2_csv(df:pd.DataFrame, tgt_path:str, mode='a', header=True):
    # save to target location
    if(header): mode = 'w'
    tgt_dir = Path(tgt_path).parent.absolute()
    os.makedirs(f'{tgt_dir}', exist_ok=True)
    df.to_csv(tgt_path, mode=mode, header=header, index=False)


def insert_coinid_2_df(df:pd.DataFrame, path:str, loc=0):
    # insert coin id column
    file = os.path.basename(path)
    coin_id = file.split('.')[0]
    df.insert(loc=loc,column='coin_id',value=coin_id)
    return df


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

    # get all files required to transform
    files = glob.glob(f'{src_dir}/candlesticks/*.json')
    for idx,file in enumerate(files):
        df = pd.read_json(file, orient='records', lines=True)
        df = transform_col_names(df, column_map)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # insert coin id column at front
        df = insert_coinid_2_df(df, file)
        
        # append data at target file
        save_df_2_csv(df, tgt_path,header=True if idx==0 else False)
    logging.info(f"Successfully transformed {idx+1} files to {tgt_path}.")


@threaded
def transform_news(src_dir:str, tgt_dir:str, columns_map:dict):
    column_map = columns_map['news']
    tgt_path = f'{tgt_dir}/news/monthly_sentiments.csv'
    time_columns = ['month', 'year']

    files = glob.glob(f'{src_dir}/news/*.json')
    for idx, file in  enumerate(files):
        df = pd.read_json(file, orient='records', lines=True)

        # get articles
        articles = pd.json_normalize(df['articles']) 
        articles = articles[articles['title'] != '[Removed]']
        articles = insert_coinid_2_df(articles, file)

        # get month, year from timestamp
        articles['publishedAt'] = pd.to_datetime(articles['publishedAt'],format='%Y-%m-%dT%H:%M:%SZ')
        articles[time_columns] = articles['publishedAt'].apply(lambda x: pd.Series([x.month, x.year],
                                                          index=time_columns))
        
        # get polarity score
        articles['polarity'] = articles['content'].apply(sentiment_pipeline)

        # aggregate by coin_id, month, year
        agg_news = articles.groupby(by=['coin_id','month', 'year']
                 ).agg({'polarity': 'mean',
                        'source.name': 'nunique',
                        'url': 'nunique'
                 }).reset_index()
        
        agg_news = transform_col_names(agg_news, column_map)

        agg_news['timestamp'] = pd.to_datetime(agg_news.apply(
            lambda x: datetime(x['year'],x['month'], 1),axis=1),utc=True)

        save_df_2_csv(agg_news,tgt_path,header=True if idx==0 else False)
    logging.info(f"Successfully transformed {idx+1} files to {tgt_path}.")


def transform_timestamp(src_dir:str, tgt_dir:str):
    """
    This module transforms timestamps from ./transformed_data/candlesticks/candlesticks.csv and 
    ./transformed_data/news/monthly_sentiments.csv. Hence, it doesn't have standalone extracted 
    source and need column map.
    """
    
    tgt_path = f'{tgt_dir}/time/time.csv'

    src_paths = [f'{src_dir}/candlesticks/candlesticks.csv', 
                 f'{src_dir}/news/monthly_sentiments.csv']

    new_columns = ['timestamp', 'day', 'month', 'year', 'weekday']

    for idx,src in enumerate(src_paths):
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
            lambda x: pd.Series([x, x.day, x.month, x.year, x.day_of_week], index=new_columns))
        
        save_df_2_csv(timestamp_df,tgt_path, header=True if idx==0 else False)
        logging.info(f"Successfully transformed {src} to {tgt_path}.")


@timer
def transform():
    extracted_dir = os.getenv('EXTRACTED_DIR')
    transformed_dir = os.getenv('TRANSFORMED_DIR')
    col_map_path = f'./{transformed_dir}/columns_map.json'
    col_map = transform.read_column_maps(col_map_path)

    thread_list = []

    thread_list.append(transform.transform_coins(extracted_dir, transformed_dir, col_map))
    thread_list.append(transform.transform_candlesticks(extracted_dir, transformed_dir, col_map))
    thread_list.append(transform.transform_news(extracted_dir,transformed_dir, col_map))
    
    for t in thread_list: t.join()
    transform.transform_timestamp(transformed_dir, transformed_dir)