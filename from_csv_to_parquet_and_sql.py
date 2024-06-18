import pandas as pd
import sqlite3


DATA_SOURCE_PATH = 'data-sources/'


# Reading the csv data
df = pd.read_csv(f'{DATA_SOURCE_PATH}raw_stock_data.csv')

# Saving it as parquet file
df.to_parquet(f'{DATA_SOURCE_PATH}raw_stock_data.parquet')

# Saving it as db table
conn = sqlite3.connect(f'{DATA_SOURCE_PATH}/raw_stock_data.db')
df.to_sql('raw_stock_data', conn, if_exists='replace', index=False)
