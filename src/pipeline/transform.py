import pandas as pd
from pathlib import Path
import json, logging, os, glob
from utils.helpers import threaded
from utils.text_processing import sentiment_pipeline
from utils.timer import timer

def transform_col_names(df:pd.DataFrame, column_map:dict)->pd.DataFrame:
    df.columns = df.columns.map(str)
    df = df[column_map.keys()]
    df = df.rename(columns=column_map)
    return df

def save_df_2_csv(df:pd.DataFrame, tgt_path:str, mode='a'):
    tgt_dir = Path(tgt_path).parent.absolute()
    os.makedirs(f'{tgt_dir}', exist_ok=True)
    df.to_csv(tgt_path, mode=mode, header=False, index=False)


def insert_coinid_2_df(df:pd.DataFrame, path:str, loc=0):
    file = os.path.basename(path)
    coin_id = file.split('.')[0]
    df.insert(loc=loc,column='coin_id',value=coin_id)
    return df
    
def read_raw_data(file, format):
    chunksize = 10000
    if(format == 'json'): 
            df_reader = pd.read_json(file, orient='records', lines=True, chunksize=chunksize)
    elif(format == 'csv'):
            df_reader = pd.read_csv(file, index_col=False, chunksize=chunksize, iterator=True)
    return df_reader

@threaded
def process_extracted_files(src_dir:str, tgt_path:str, column_map:dict=None,
                            src_format='json', transform_fn:classmethod=None):

    src_files = glob.glob(f'{src_dir}/*.{src_format}')
    for idx, src_file in enumerate(src_files):
        df_reader = read_raw_data(src_file, src_format)
        for _,chunk in enumerate(df_reader):
            if(transform_fn): chunk = transform_fn(chunk, src_file)
            if(column_map): chunk = transform_col_names(chunk,column_map)
            save_df_2_csv(chunk, tgt_path,mode='w' if idx==0 else 'a')
        logging.info(f"Processing {src_file}.")

    logging.info(f"Successfully transformed files from {src_dir} to {tgt_path}.")

### Customized transform functions. Pass transform function to process_extracted_files
def candlesticks_transform_fn(df:pd.DataFrame, src_file:str)->pd.DataFrame:
    df[0] = pd.to_datetime(df[0], unit='ms') 
    df = insert_coinid_2_df(df, src_file) # insert coin id column at front
    return df


def news_transform_fn(df:pd.DataFrame, src_file:str)->pd.DataFrame:
    articles = pd.json_normalize(df['articles']) 
    articles = articles[articles['title'] != '[Removed]']
    articles = insert_coinid_2_df(articles, src_file)
    
    # breakdown timestamp to month and year
    time_columns=['month', 'year']
    articles['publishedAt'] = pd.to_datetime(articles['publishedAt'],
                                             format='%Y-%m-%dT%H:%M:%SZ')
    articles[time_columns] = articles['publishedAt'].apply(
        lambda x: pd.Series([x.month, x.year],index=time_columns))

    articles['polarity'] = articles['content'].apply(sentiment_pipeline)

    # aggregate by coin_id, month, year
    agg_news = articles.groupby(by=['coin_id','month', 'year']
            ).agg({'polarity': 'mean',
                    'source.name': 'nunique',
                    'url': 'nunique'}).reset_index()
    return agg_news


def timestamp_transform_fn(df:pd.DataFrame, src_file:str)->pd.DataFrame:
    
    timestamp_df = pd.DataFrame(columns=['timestamp'])
    
    temp = pd.to_datetime(df.iloc[:,1].unique())
    temp = temp.to_frame(name='timestamp').dropna()
    
    if(not timestamp_df.empty): timestamp_df = pd.concat(
         (timestamp_df, temp), ignore_index=True).drop_duplicates()
        
    timestamp_df = temp # assigned first chunk to empty df
    
    new_columns = ['timestamp', 'hour', 'day', 'month', 'year', 'weekday']
    # breakdown timestamp to day, month, year, weekday
    timestamp_df[new_columns] = timestamp_df['timestamp'].apply(
    lambda x: pd.Series([x, x.hour, x.day, x.month, x.year, x.day_of_week]
                        , index=new_columns))
    return timestamp_df

@timer
def transform():
    """
    Main transform function:
    - Remapped extracted_data using columns_map.json and transformed using 
    customized function for each dataset.
    """
    data_src_dir = os.getenv('DATA_SRC_DIR')
    extracted_dir = f'{data_src_dir}/extracted_data'
    transformed_dir = f'{data_src_dir}/transformed_data'
    col_map = json.load(open(f'{data_src_dir}/columns_map.json'))

    thread_list = []
    # add customized transform fn in the future.
    transform_fn_map = {
        "coins" : None,
        "candlesticks": candlesticks_transform_fn,
        "news": news_transform_fn,
        "time": timestamp_transform_fn
    }

    sub_dirs = glob.glob(f'{extracted_dir}/*')
    for _,sub_dir in enumerate(sub_dirs):
        if(os.path.isfile(sub_dir)): continue
        dir_name = os.path.basename(sub_dir)
        tgt_path = f'{transformed_dir}/{dir_name}/{dir_name}.csv'

        t = process_extracted_files(sub_dir, tgt_path, column_map=col_map[dir_name],
                                transform_fn= transform_fn_map[dir_name])

        if(dir_name == 'candlesticks'): 
            # transform time only after completed candelsticks
            t.join()
            tgt_path = f'{transformed_dir}/time/time.csv'
            new_src_dir = f'{transformed_dir}/{dir_name}'
            t = process_extracted_files(new_src_dir, tgt_path, src_format='csv',
                                    transform_fn=transform_fn_map['time'])
        thread_list.append(t)    
    for t in thread_list: t.join()