import os
import json
from extract import extract
from dotenv import load_dotenv

def test_extract():
    tgt_dir = os.getenv('EXTRACTED_DIR')

    with open('./data/extracted_data/endpoints.json', 'r') as fh: 
        endpoints = json.load(fh)
        for ep_name in endpoints:
            extract(endpoints[ep_name],tgt_dir,
                    sub_dir=ep_name,fname=ep_name,
                    coin_id='ethereum', coin_name='Ethereum')

if __name__ == '__main__':
    load_dotenv()
    test_extract()
