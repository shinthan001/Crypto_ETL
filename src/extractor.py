import os
import json
import requests
import pandas as pd
from requests.exceptions import HTTPError, RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

def request_api(url:str, headers:dict=None):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except HTTPError as http_err:
        raise (http_err)
    except RequestException as err:
        raise SystemExit(err)
    
    return response

def save_to_json(json_src_dir:str, fname:str, df:pd.DataFrame):
    file_path = f'{json_src_dir}/{fname}'
    os.makedirs(json_src_dir, exist_ok=True)
    
    try:    
        df.to_json(
            file_path,
            orient='records',
            lines=True
        )
        print(f'Completed saving to {file_path}.\n')
    except FileNotFoundError as err:
        raise(err)
    
def extract_data(url:str, fname:str, headers:dict=None):
    response = request_api(url, headers)
    json_doc = pd.read_json(response.text)
    json_src_dir = os.getenv('JSON_SRC_DIR')
    save_to_json(json_src_dir, fname, json_doc)

def extract_data_mt(url_args: tuple):
    processes = []
    with ThreadPoolExecutor(max_workers=len(url_args)) as executor:
        for url in url_args:
            processes.append(executor.submit(extract_data, url_args))
            
    for task in as_completed(processes):
        print(task.result())

def extract():
    
    coingecko_header = {
        "accept": "application/json",
        "x-cg-demo-api-key": os.getenv('API_KEY_CONINGECKO')
    }

    coinlists_api = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
    fname = 'coin_list.json'
    extract_data(coinlists_api,fname,coingecko_header)

if __name__ == '__main__':
    load_dotenv('../env')
    extract()
