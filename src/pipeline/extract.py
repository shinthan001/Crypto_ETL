import os
import time
import requests
import threading
import pandas as pd
from io import StringIO 
from pathlib import Path
from utils.helpers import threaded, get_date, to_dataframe, to_json

def get_proxies()->dict:
    http_proxy = os.getenv('HTTP_PROXY')
    https_proxy = os.getenv('HTTPS_PROXY')

    return {
        "http" : http_proxy, 
        "https" : https_proxy
    }

def get_ssl_cert()->str:
    return os.getenv('SSL_VERIFY')

def request_api(url:str, headers:dict=None)->requests.models.Response:
    proxies = get_proxies()
    ssl_cert = get_ssl_cert()
    print(f"Sending Request to URL: {url}")
    response = requests.get(url,headers=headers, proxies=proxies, verify=ssl_cert)
    response.raise_for_status()
    return response
    
    print(f"Successfully saved to {file_to_save}.\n")

def build_header(endpoint:dict)->dict:
    headers = None
    if 'headers' in endpoint:
        headers = endpoint['headers']
        apiKey_key = endpoint['apikey_in_headers']
        apiKey_value = os.getenv(headers[apiKey_key])
        headers[apiKey_key] = apiKey_value
    return headers

def build_url(endpoint:str)->str:
    url = endpoint['url']
    if 'params' in endpoint:
        params = endpoint['params']
        params_str = '&'.join([f'{k}={v}' for k,v in params.items()])
        url = url + params_str
    return url

@threaded
def extract(url:str, headers:dict, file_to_save:str)->threading.Thread:
    response = request_api(url, headers=headers)
    df = to_dataframe(response.text)
    to_json(df,file_to_save)

def extract_coinslist(endpoint:dict, tgt_dir:str)->str:
    url = build_url(endpoint)
    headers = build_header(endpoint)
    file_to_save = f'{tgt_dir}/coinslist/coinslist.json'
    t = extract(url, headers,file_to_save)
    t.join()
    return file_to_save

def extract_candlesticks(endpoint:dict, tgt_dir:str, coin_ids:list):
    url = build_url(endpoint)
    headers = build_header(endpoint)
    
    threads = []
    for coin_id in coin_ids:
        file_to_save = f'{tgt_dir}/candlesticks/{coin_id}.json'
        new_url = url.replace('[coin_id]', coin_id)
        print(new_url, file_to_save)
        t = extract(new_url, headers,file_to_save)
        threads.append(t)

    for t in threads: t.join()

def extract_news(endpoint:dict, tgt_dir:str, coin_names:list):
    url = build_url(endpoint)
    headers = build_header(endpoint)

    if('params_to_replace' in endpoint):
        params_to_replace = endpoint['params_to_replace']
        if('from' in params_to_replace): 
            new_param = get_date(params_to_replace['from'])
            url = url + f'&from={new_param}'

    threads = []
    for coin_name in coin_names:
        file_to_save = f'{tgt_dir}/news/{coin_name}.json'
        new_url = url.replace('[coin_name]', coin_name)
        t = extract(new_url, headers,file_to_save)
        time.sleep(2)
        threads.append(t)

    for t in threads: t.join()
