import pandas as pd
import sqlalchemy

from sqlalchemy import Engine
from environment import DATA_SOURCE_PATH, hprint, print, init_environment


init_environment()


def get_source_db_conn() -> Engine:
    """Return the DB connection"""
    # connection_uri = "postgresql+psycopg2://repl:password@localhost:5432/market"  # noqa
    connection_uri = f'sqlite:///{DATA_SOURCE_PATH}/raw_stock_data.db'
    db_engine = sqlalchemy.create_engine(connection_uri)
    return db_engine


def extract_from_parquet(file_name: str) -> pd.DataFrame:
    """Read from parquet file and return a dataframe"""
    hprint('Extracting from Parquet Files')
    df = pd.read_parquet(f"{DATA_SOURCE_PATH}{file_name}.parquet",
                         engine="fastparquet")

    print('Types:', df.dtypes)
    print('Info:', df.info())

    print('Shape:', df.shape)
    print('Head:', df.head())
    return df


def extract_from_db(db_engine: Engine, table_name: str):
    """Read from database and return a dataframe"""
    hprint('Extracting from Database')

    df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 10",
                     db_engine)
    print(df.dtypes)
    print(df.info())

    print(df.shape)
    print(df.head())


def extract_from_json(file_name: str) -> pd.DataFrame:
    """Read from json file and return a dataframe"""
    hprint('Extracting from Json Files')
    df = pd.read_json(f"{DATA_SOURCE_PATH}{file_name}.json",
                      orient="columns")

    print('Types:', df.dtypes)
    print('Info:', df.info())

    print('Shape:', df.shape)
    print('Head:', df.head())
    return df


if __name__ == '__main__':
    db_engine = get_source_db_conn()

    _ = extract_from_parquet('sales')
    _ = extract_from_db(db_engine, 'raw_stock_data')
    _ = extract_from_json('sales')
