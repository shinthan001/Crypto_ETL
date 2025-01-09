import os
from extractor import extract_data_mt

from dotenv import load_dotenv
load_dotenv('../env')

# def extract():

#     coingecko_headers = {
#         "accept": "application/json",
#         "x-cg-demo-api-key": os.getenv('API_KEY_COINGECKO')
#     }

#     urls = [
#         (f'https://api.coingecko.com/api/v3/coins/list?include_platform=true')
#     ]



if __name__ == '__main__':
    print(os.getenv('API_OHLC'))