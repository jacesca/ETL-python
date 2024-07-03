import pandas as pd
import sqlite3

from environment import DATA_SOURCE_PATH


FILES = ['raw_stock_data', 'sales']


for file in FILES:
    # Reading the csv data
    df = pd.read_csv(f'{DATA_SOURCE_PATH}{file}.csv')

    # Saving it as parquet file
    df.to_parquet(f'{DATA_SOURCE_PATH}{file}.parquet')

    # Saving it as json file
    df.to_json(f'{DATA_SOURCE_PATH}{file}.json', orient='columns')

    # Saving it as db table
    connection_uri = f'{DATA_SOURCE_PATH}/{file}.db'
    db_engine = sqlite3.connect(connection_uri)
    df.to_sql(file, db_engine, if_exists='replace', index=False)

    # Reading from sql table
    df_read = pd.read_sql(f"SELECT * FROM {file}", db_engine)
    print(df_read.head())
