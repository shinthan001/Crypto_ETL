import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv      
import pandas as pd      

def relocate_exec_path():
    current_path = os.path.abspath(os.path.dirname(__file__))
    parent_path = Path(current_path).parent.absolute()
    print("Current Path:", current_path)
    print("Parent Path:", parent_path)
    sys.path.append(str(parent_path))

def remove_file(path):
    try:
        os.remove(path)
    except:
        pass

def remove_dir(dir):
    try:
        os.removedirs(dir)
    except:
        pass

def test_extract_coinslist(endpoint,tgt_dir):
    from extract import extract_coinslist

    remove_file(f'{tgt_dir}/coinslist/coinslist.json')
    coinslist_path = extract_coinslist(endpoint,tgt_dir)
    assert os.path.exists(coinslist_path) == True
    return coinslist_path

def test_extract_candlesticks(endpoint,tgt_dir, coin_ids):
    from extract import extract_candlesticks
    remove_dir(f'{tgt_dir}/candlesticks')
    extract_candlesticks(endpoint, tgt_dir, coin_ids)

def test_extract_news(endpoint,tgt_dir,coin_lists):
    from extract import extract_news
    remove_dir(f'{tgt_dir}/news')
    extract_news(endpoint, tgt_dir, coin_lists)

if __name__ == '__main__':
    
    relocate_exec_path()
    load_dotenv('env')
    tgt_dir = os.getenv('EXTRACTED_DIR')
    with open(f'./{tgt_dir}/endpoints.json', 'r') as fh: 
        endpoints = json.load(fh)

    coinlists = test_extract_coinslist(endpoints['coinslist'],tgt_dir)
    # coinlists = f'{tgt_dir}/coinslist/coinslist.json'
    coinlists = pd.read_json(coinlists, orient='records', lines=True)

    coin_ids = coinlists['id'].to_list()
    test_extract_candlesticks(endpoints['candlesticks'],tgt_dir,coin_ids)

    coin_names = coinlists['name'].to_list()
    test_extract_news(endpoints['news'],tgt_dir,coin_names)
