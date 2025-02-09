import numpy as np
import pandas as pd
from io import StringIO 
from pathlib import Path
import json, time, os, logging, requests
from requests.exceptions import HTTPError,ConnectionError,Timeout,RequestException

def _build_url(endpoint:str)->str:
    return endpoint['url'] + '&'.join([f'{k}={v}' for k,v in endpoint['params'].items()])

def _build_headers(endpoint:dict)->dict:
    if('headers' not in endpoint): return None
    headers = endpoint['headers']
    apiKey_key_env = endpoint['apikey_in_headers']
    headers[apiKey_key_env] = os.getenv(headers[apiKey_key_env])
    return headers

def _save_df_2_json(df:pd.DataFrame, path:str):
    parent_dir = Path(path).parent.absolute()
    os.makedirs(f'{parent_dir}', exist_ok=True)
    df.to_json(path, orient='records', lines=True)
    logging.info(f"Successfully saved to {path}.\n")

def _request_endpoint(url:str, headers:dict, tgt_path:str):
    logging.info(f"Sending Request to URL: {url}")
    try:
        response = requests.get(url,headers)
        return response
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}"); raise(http_err)
    except ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err}"); raise(conn_err)
    except Timeout as timeout_err:
        logging.error(f"Timeout error occurred: {timeout_err}"); raise(timeout_err)
    except RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}"); raise(req_err)

def _validate_response(response:requests.models.Response):
    doc = json.loads(response.text)
    if 'status' in doc: 
        err = None
        # news api
        if('error_code' in doc['status'] and doc['status']['error_code']==429): 
            err=doc['status']['error_message'] 
        # coingecko api
        elif(doc['status'] == 'error'):
            err=doc['message']
        else: return
        logging.error(err)
        raise(HTTPError(err))

def _process_endpoint(url:str, headers:dict, tgt_path:str):
    response = _request_endpoint(url, headers, tgt_path)
    _validate_response(response)
    df = pd.read_json(StringIO(response.text))
    if(len(df) == 0): logging.info(f"No data. Dataframe Length: {len(df)}");return
    _save_df_2_json(df,tgt_path)

    #add this to prevent blocking due to frequent requests
    time.sleep(np.random.randint(10,20))

### customized extract modules for particular APIs
def process_coins(endpoints_info:dict, tgt_dir:str):
    url, headers = _build_url(endpoints_info['coins']), _build_headers(endpoints_info['coins'])
    tgt_path = f'{tgt_dir}/coins/coins.json'
    _process_endpoint(url, headers, tgt_path)

def _get_coins(dir:str):
    path = f'{dir}/coins/coins.json'
    df = pd.read_json(path, orient='records', lines=True)
    return df['id'].to_list()

def process_candlesticks(endpoints_info:dict, tgt_dir:str):
    url, headers = _build_url(endpoints_info['candlesticks']), _build_headers(endpoints_info['candlesticks'])
    coins_list = _get_coins(tgt_dir)
    for _, coin_id in enumerate(coins_list):
        tgt_path = f'{tgt_dir}/candlesticks/{coin_id}.json'
        new_url = url.replace('[COIN_ID]', coin_id)
        _process_endpoint(new_url, headers, tgt_path)
        

def process_news(endpoints_info:dict, tgt_dir:str):
    url, headers = _build_url(endpoints_info['news']), _build_headers(endpoints_info['news'])
    url = url.replace('[API_KEY]',os.getenv('APIKEY_NEWSAPI'))
    coins_list = _get_coins(tgt_dir)
    for _, coin_id in enumerate(coins_list):
        tgt_path = f'{tgt_dir}/news/{coin_id}.json'
        new_url = url.replace('[COIN_NAME]', f'crypto%20{coin_id}')
        _process_endpoint(new_url, headers, tgt_path)
