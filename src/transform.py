import os
import pandas as pd
from dotenv import load_dotenv

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


load_dotenv('.env')
df = pd.read_json(f'{os.getenv('EXTRACTED_DIR')}/coinslist.json', orient='records', lines=True)
