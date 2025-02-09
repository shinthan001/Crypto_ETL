import json, os
from dotenv import load_dotenv

from src import extract,transform
from src.utils.helpers import configure_root_logger


if __name__ == '__main__':
    load_dotenv('env/.env')
    configure_root_logger()
    meta_data_dir, data_dir = os.getenv('META_DATA_DIR'), os.getenv('DATA_SRC_DIR')

    extracted_dir = f'{data_dir}/extracted_data'
    transformed_dir = f'{data_dir}/transformed_data'
    columns_map = json.load(open(f'{meta_data_dir}/columns_map.json', 'r'))

    endpoints_info = json.load(open(f'{meta_data_dir}/endpoints.json', 'r'))
    # extract.process_candlesticks(endpoints_info, extracted_dir)
    # extract.process_news(endpoints_info, extracted_dir)
    # transform.process_coins(extracted_dir,transformed_dir,columns_map)
    # transform.process_candlesticks(extracted_dir,transformed_dir,columns_map)
    # transform.process_news(extracted_dir,transformed_dir,columns_map)
    # transform.process_timestamps(transformed_dir,transformed_dir)

    sql = open(os.getenv('SQL_CREATE_TABLE')).read()
    print(sql)
