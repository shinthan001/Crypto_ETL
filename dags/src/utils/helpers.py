import os
import sys
import json
import logging
import threading
from multiprocessing import Process
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta


def multiprocessed(fn):
    def wrapper(*args, **kwargs):
        try:
            p = Process(target=fn, args=args, kwargs=kwargs)
            p.start()
            return p
        except Exception as err:
            logging.exception(f"Error occured in thread: {err}")
    return wrapper

def threaded(fn):
    def wrapper(*args, **kwargs):
        try:
            thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
            thread.start()
            return thread
        except Exception as err:
            logging.exception(f"Error occured in thread: {err}")
    return wrapper

def get_date(days=0, format="%Y-%m-%d"):
    now = datetime.now()
    date = (now + timedelta(days=days))
    date = date.strftime(format)
    return date

def configure_root_logger():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s] - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)