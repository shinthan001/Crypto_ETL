import os
import sys
import json
from dotenv import load_dotenv
from pipeline import transform
from utils.helpers import configure_root_logger
from utils.timer import timer

@timer
def test_transform():
    extracted_dir = os.getenv('EXTRACTED_DIR')
    transformed_dir = os.getenv('TRANSFORMED_DIR')

    col_map_path = f'./{transformed_dir}/columns_map.json'
    col_map = transform.read_column_maps(col_map_path)
    p = transform.transform_coins(extracted_dir, transformed_dir, col_map)
    p = transform.transform_candlesticks(extracted_dir, transformed_dir, col_map)
    p.join(), p.join()
    transform.transform_timestamp(transformed_dir, transformed_dir)


if __name__ == '__main__':
    configure_root_logger()
    load_dotenv('env')
    test_transform()

