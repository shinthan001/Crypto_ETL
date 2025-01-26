from airflow.operators

from dotenv import load_dotenv
from pipeline import extract, load, transform
from utils.helpers import configure_root_logger

if __name__ == '__main__':
    configure_root_logger()
    load_dotenv('env')
    extract()
    transform()
    load()