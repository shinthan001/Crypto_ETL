import sys
import os
import glob
import json
import pandas as pd


def retrieve_db_connection_str():
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')
    db = os.environ.get('DB_NAME')
    user = os.environ.get('DB_USER')
    pwd = os.environ.get('DB_PASS')
    return f'postgresql://{user}:{pwd}@{host}:{port}/{db}'


def db_loade
        





