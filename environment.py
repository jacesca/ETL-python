import os
import pandas as pd

from dotenv import load_dotenv
from functools import partial


load_dotenv()

DATA_SOURCE_PATH = os.getenv('DATA_SOURCE_PATH')

CRED = '\033[42m'
CEND = '\033[0m'

print = partial(print, sep='\n', end='\n\n')


def init_environment() -> None:
    """
    Global configuration of the environment.
    """
    pd.set_option('display.max_columns', None)


def hprint(msg: str) -> None:
    """Highlighted print"""
    print(CRED + msg + CEND)
