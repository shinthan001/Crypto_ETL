import time
import logging

def timer(func):

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logging.info(f"Function {func.__name__} Execution time: {duration:.3f}s.")

    return wrapper