import os, sys, json
from pathlib import Path 
from dotenv import load_dotenv      


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

def test_transform_coins(src_dir:str, tgt_dir:str, columns_map:dict):
    from transform import transform_coins
    file = f'{tgt_dir}/coins/coins.csv'
    remove_file(file)
    t = transform_coins(src_dir,tgt_dir,columns_map)
    t.join()
    assert os.path.exists(file) == True

def test_transform_candlesticks(src_dir:str, tgt_dir:str, columns_map:dict):
    from transform import transform_candlesticks
    file = f'{tgt_dir}/candlesticks/candlesticks.csv'
    remove_file(file)
    t = transform_candlesticks(src_dir,tgt_dir,columns_map)
    t.join()
    assert os.path.exists(file) == True

if __name__ == '__main__':
    relocate_exec_path()
    load_dotenv('env')
    src_dir = os.getenv('EXTRACTED_DIR')
    tgt_dir = os.getenv('TRANSFORMED_DIR')

    with open(f'./{tgt_dir}/columns_map.json', 'r') as fh: 
        columns_map = json.load(fh)

    test_transform_coins(src_dir,tgt_dir,columns_map)
    test_transform_candlesticks(src_dir,tgt_dir,columns_map)