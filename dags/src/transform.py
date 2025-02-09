import pandas as pd
from pathlib import Path
import json, logging, os, glob
from src.utils.text_processing import SentimentPipeline

def _transform_col_names(df:pd.DataFrame, column_map:dict)->pd.DataFrame:
    df.columns = df.columns.map(str)
    return df[column_map.keys()].rename(columns=column_map)

def _save_df_2_csv(df:pd.DataFrame, tgt_path:str, mode='a'):
    tgt_dir = Path(tgt_path).parent.absolute()
    os.makedirs(f'{tgt_dir}', exist_ok=True)
    df.to_csv(tgt_path, mode=mode, header=False, index=False)

def _insert_coinid_2_df(df:pd.DataFrame, path:str, loc=0)->pd.DataFrame:
    file = os.path.basename(path)
    coin_id = file.split('.')[0]
    df.insert(loc=loc,column='coin_id',value=coin_id)
    return df
    
def _read_raw_data(file, format):
    chunksize = os.getenv('CHUNKSIZE')
    if(format == 'json'): 
            return pd.read_json(file, orient='records', lines=True, chunksize=chunksize)
    elif(format == 'csv'):
            return pd.read_csv(file, index_col=False, chunksize=chunksize, iterator=True)

def process_coins(src_dir:str, tgt_dir:str, columns_map:dict):
    src_path = f'{src_dir}/coins/coins.json'
    tgt_path = f'{tgt_dir}/coins/coins.csv'
    df = _read_raw_data(src_path, 'json')    
    df = _transform_col_names(df, columns_map['coins'])
    _save_df_2_csv(df, tgt_path, mode='w')
    logging.info(f"Successfully transformed files from {src_path} to {tgt_path}.")

def process_candlesticks(src_dir:str, tgt_dir:str, columns_map:dict):
    src_path = f'{src_dir}/candlesticks'
    tgt_path = f'{tgt_dir}/candlesticks/candlesticks.csv'

    for idx, src_file in enumerate(glob.glob(f'{src_path}/*.json')):
        df = _read_raw_data(src_file, 'json')
        df[0] = pd.to_datetime(df[0], unit='ms')
        df = _insert_coinid_2_df(df, src_file)
        df = _transform_col_names(df, columns_map['candlesticks'])
        _save_df_2_csv(df, tgt_path,mode='w' if idx==0 else 'a')
        logging.info(f"Successfully transformed files from {src_file} to {tgt_path}.")

def process_news(src_dir:str, tgt_dir:str, columns_map:dict):
    src_path = f'{src_dir}/news'
    tgt_path = f'{tgt_dir}/news/news.csv'
    sentimentpipeline = SentimentPipeline()

    time_columns=['month', 'year']
    for idx, src_file in enumerate(glob.glob(f'{src_path}/*.json')):
        df_reader = _read_raw_data(src_file, 'json')
        if(type(df_reader) == pd.DataFrame): df_reader = [df_reader]
        for chunk in df_reader:
            articles = pd.json_normalize(chunk['articles']) 
            articles = articles[articles['title'] != '[Removed]']
            
            articles = _insert_coinid_2_df(articles, src_file)
            articles['publishedAt'] = pd.to_datetime(articles['publishedAt'],format='%Y-%m-%dT%H:%M:%SZ')
            
            articles[time_columns] = articles['publishedAt'].apply(
                lambda x: pd.Series([x.month, x.year],index=time_columns))
            articles['polarity'] = articles['content'].apply(lambda x: sentimentpipeline.get_polarity_score(x))
            
            # aggregate by coin_id, month, year
            agg_news = articles.groupby(by=['coin_id','month', 'year']).agg(
                {'polarity': 'mean','source.name': 'nunique','url': 'nunique'})
            agg_news = _transform_col_names(agg_news.reset_index(), columns_map['news'])
            
            _save_df_2_csv(agg_news, tgt_path,mode='w' if idx==0 else 'a')
        logging.info(f"Successfully transformed files from {src_file} to {tgt_path}.")

def process_timestamps(src_dir:str, tgt_dir:str, columns_map:dict):
    src_path = f'{src_dir}/candlesticks/candlesticks.csv'
    tgt_path = f'{tgt_dir}/timestamps/timestamps.csv'
    columns = ['timestamp', 'hour', 'day', 'month', 'year', 'weekday']
    df_reader = _read_raw_data(src_path, 'csv')
    if(type(df_reader) == pd.DataFrame): df_reader = [df_reader]
    
    for idx,chunk in enumerate(df_reader):
        timestamps = pd.DataFrame(pd.to_datetime(chunk.iloc[:,1].unique())).dropna()
        df = pd.concat((df,timestamps), ignore_index=True) if idx>0 else timestamps

    df.drop_duplicates(inplace=True)
    df[columns] = df[0].apply(lambda x: pd.Series([x,x.hour,x.day,x.month,x.year,x.day_of_week], index=columns))
    df = _transform_col_names(df, columns_map['timestamps'])
    _save_df_2_csv(df[columns], tgt_path, mode='w')
    logging.info(f"Successfully transformed files from {src_path} to {tgt_path}.")