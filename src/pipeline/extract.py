import numpy as np
import pandas as pd
from io import StringIO 
from pathlib import Path
import time, os, logging, json, requests
from utils.helpers import threaded

def random_sleep():
    # add this to prevent requesting too much in a short time
    time.sleep(np.random.randint(1,3))

def save_df_2_json(df:pd.DataFrame, path:str):
    if(len(df) == 0): 
        logging.info(f"No data. Dataframe Length: {len(df)}")
        return
    parent_dir = Path(path).parent.absolute()
    os.makedirs(f'{parent_dir}', exist_ok=True)
    df.to_json(path, orient='records', lines=True)
    logging.info(f"Successfully saved to {path}.\n")

def request_url(url:str, headers:dict=None)->requests.models.Response:
    logging.info(f"Sending Request to URL: {url}")
    response = requests.get(url,headers=headers)
    response.raise_for_status()
    return response

def build_header(endpoint:dict)->dict:
    headers = endpoint['headers']
    apiKey_key_env = endpoint['apikey_in_headers']
    headers[apiKey_key_env] = os.getenv(headers[apiKey_key_env])
    return headers

def build_url(endpoint:str)->str:
    url = endpoint['url']
    if 'params' in endpoint:
        url = url + '&'.join([f'{k}={v}' for k,v in 
                              endpoint['params'].items()])
    return url

def process_endpoint(url:str, headers:dict, tgt_path:str):
    response = request_url(url, headers)
    df = pd.read_json(StringIO(response.text))
    save_df_2_json(df,tgt_path)
    random_sleep()

### customized extract modules for particular APIs
@threaded
def extract_coinslist(endpoint_info:dict, coins_list:np.array, tgt_dir:str):
    url, headers = build_url(endpoint_info), build_header(endpoint_info)
    tgt_path = f'{tgt_dir}/coinslist/coinslist.json'
    url = url.replace('[COINS]', ','.join(coins_list))
    process_endpoint(url, headers, tgt_path)

@threaded
def extract_candlesticks(endpoint_info:dict, coins_list:np.array, tgt_dir:str):
    url, headers = build_url(endpoint_info), build_header(endpoint_info)
    for _, coin_id in enumerate(coins_list):
        tgt_path = f'{tgt_dir}/candlesticks/{coin_id}.json'
        new_url = url.replace('[COIN_ID]', coin_id)
        process_endpoint(new_url, headers, tgt_path)

@threaded
def extract_news(endpoint_info:dict, coins_list:np.array, tgt_dir:str):
    url, headers = build_url(endpoint_info), build_header(endpoint_info)
    for _, coin_id in enumerate(coins_list):
        tgt_path = f'{tgt_dir}/news/{coin_id}.json'
        new_url = url.replace('[COIN_NAME]', f'crypto {coin_id}')
        process_endpoint(new_url, headers, tgt_path)

def extract():
    """
    Main extract function
    - Process extraction from APIs stored in endpoints.json.
    """
    data_dir = os.getenv('DATA_SRC_DIR')
    tgt_dir = f'{data_dir}/extracted_data'
    coins_list = pd.read_csv(f'{data_dir}/coins.csv', 
                             header=None).to_numpy().flatten()
    endpoints_info = json.load(open(f'{data_dir}/endpoints.json', 'r'))

    # Append fun_map with customized API extract modules in the future
    fun_map = {'coinslist' : extract_coinslist,
               'candlesticks': extract_candlesticks,
               'news': extract_news,
    }

    threads = []
    for name,endpoint_info in endpoints_info.items():
        extract_fn = fun_map[name]
        threads.append(extract_fn(endpoint_info, coins_list, tgt_dir))
    for t in threads: t.join()
