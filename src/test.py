import os
import sys, glob
import json
from dotenv import load_dotenv
from pipeline import transform, load
from utils.helpers import configure_root_logger
from utils.timer import timer

@timer
def test_transform():
    transform.transform()

def test_load():
    load.load()

if __name__ == '__main__':
    configure_root_logger()
    load_dotenv('env')
    test_transform()
    test_load()

