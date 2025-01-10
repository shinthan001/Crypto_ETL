import os
import json
import requests
import pandas as pd
from io import StringIO 
from dotenv import load_dotenv
from requests.exceptions import RequestException

# from requests.utils import DEFAULT_CA_BUNDLE_PATH
# import certifi
# print(certifi.where())

def get_proxies():
    http_proxy = os.getenv('HTTP_PROXY')
    https_proxy = os.getenv('HTTPS_PROXY')

    return {
        "http" : http_proxy, 
        "https" : https_proxy
    }

def get_ssl_cert():
    return os.getenv('SSL_VERIFY')

def request_api(url:str, headers:dict=None):
    proxies = get_proxies()
    ssl_cert = get_ssl_cert()

    try:
        response = requests.get(url,headers=headers, proxies=proxies, verify=ssl_cert)
    except RequestException as err:
        raise(err)
    return response

def to_dataframe(response:requests.models.Response):
    df = pd.read_json(StringIO(response.text))
                    #   ,orient='records', lines=True)
    return df

def to_json(df:pd.DataFrame, tgt_dir:str, sub_dir:str, fname:str):
    file_path = f'{tgt_dir}/{sub_dir}/{fname}.json'
    os.makedirs(f'{tgt_dir}/{sub_dir}', exist_ok=True)
    try:
        df.to_json(file_path)
                #    ,orient='records', lines=True)
    except FileNotFoundError as err:
        raise(err)

    print(f"Successfully saved to {file_path}.\n")

# def extract_api(url:str, tgt_dir:str, fname:str, headers:dict=None):
#     response = request_api(url, headers)
#     df = to_dataframe(response)
#     to_json(df,tgt_dir,fname)

def extract_news(url:str, coin:str, tgt_dir:str, sub_dir:str):
    url = url.replace('[coin_symbol]', coin)
    response = request_api(url)
    
    # check if any error exists
    json_doc = json.loads(response.text)
    if (json_doc['status'] == 'error'): 
        raise(f'{json_doc['code']} : {json_doc['message']}\n')
    # no article found
    if json_doc['totalResults'] == 0:
        print("no article found!")
        return

    df = to_dataframe(response)
    to_json(df,tgt_dir,sub_dir,coin)

