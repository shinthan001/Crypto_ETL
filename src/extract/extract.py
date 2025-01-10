import os
import requests
import pandas as pd
from io import StringIO 
from dotenv import load_dotenv
from requests.exceptions import RequestException
from datetime import datetime, timedelta

# from requests.utils import DEFAULT_CA_BUNDLE_PATH
# import certifi
# print(certifi.where())

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

    try:
        response = requests.get(url,headers=headers, proxies=proxies, verify=ssl_cert)
    except RequestException as err:
        raise(err)
    return response

def to_dataframe(response:requests.models.Response)->pd.DataFrame:
    df = pd.read_json(StringIO(response.text))
                    #   ,orient='records', lines=True)
    return df

def to_json(df:pd.DataFrame, tgt_dir:str,sub_dir:str, fname:str)->str:
    file_path = f'{tgt_dir}/{sub_dir}/{fname}.json'
    os.makedirs(f'{tgt_dir}/{sub_dir}', exist_ok=True)
    try:
        df.to_json(file_path, index=False)
                #    ,orient='records', lines=True)
    except FileNotFoundError as err:
        raise(err)

    print(f"Successfully saved to {file_path}.\n")
    return file_path

def extract(endpoint:dict, tgt_dir:str, sub_dir:str, 
                         fname:str, coin_id:str=None, coin_name:str=None):
    
    url = endpoint['url']

    headers = None
    if 'headers' in endpoint:
        headers = endpoint['headers']
        apiKey_key = endpoint['apikey_in_headers']
        apiKey_value = os.getenv(headers[apiKey_key])
        headers[apiKey_key] = apiKey_value

    if 'params' in endpoint:
        params = endpoint['params']
        if('apiKey' in params): params['apiKey'] = os.getenv(params['apiKey'])
        params_str = '&'.join([f'{k}={v}' for k,v in params.items()])
        url = url + params_str

    if 'to_replace' in endpoint:
        str_to_replace = endpoint['to_replace']
        replacement = coin_id if(str_to_replace == '[coin_id]') else coin_name 
        url = url.replace(str_to_replace,replacement)

    # print(url)
    response = request_api(url, headers=headers)
    df = to_dataframe(response)

    return to_json(df,tgt_dir,sub_dir,fname)

