import pandas as pd
import os
import logging

from step1_extract_data import extract_from_parquet  # get_source_db_conn, extract_from_db  # noqa
from step2_transform_data import transform_sales
from environment import hprint, print


logging.basicConfig(format='%(levelname)s: %(message)s\n', level=logging.DEBUG)


def saving_data(df: pd.DataFrame, filename: str) -> bool:
    df.to_csv(f'data-stages/{filename}.csv', index=False, sep='|')

    # Check that the path exists
    file_exists = os.path.exists(f'data-stages/{filename}.csv')

    return file_exists


if __name__ == '__main__':
    # db_engine = get_source_db_conn()
    # _ = extract_from_db(db_engine, 'raw_stock_data')

    try:
        df_sales_raw = extract_from_parquet('sales')
        df_sales_kitchen = transform_sales(df_sales_raw)
        logging.info("Successfully filtered DataFrame by 'Total Price'")
    except KeyError as e:
        logging.warning(f"{e}: Cannot filter DataFrame by 'Total Price'")

        # Create the "total_price" column, transform the updated DataFrame
        df_sales_raw["total_price"] = df_sales_raw["price_each"] * df_sales_raw["quantity_ordered"]  # noqa
        df_sales_kitchen = transform_sales(df_sales_raw)
        hprint(f'Raw and kitchen data are equal? {df_sales_kitchen.equals(df_sales_raw)}')  # noqa

        # Validating
        hprint('Making some validations:')
        print('Min quantity_ordered (Should be greater than 1):',
              df_sales_kitchen.nsmallest(1, 'quantity_ordered'))
        print(' Max price_each (Should be lower than 10):',
              df_sales_kitchen.nlargest(1, 'price_each'))

        # Saving the stage
        saved_file = saving_data(df_sales_kitchen, 'sales_kitchen')
        if saved_file:
            print('Kitchen stage saved!')
        else:
            print('Error: kitchen stage could not be saved!')
    except FileNotFoundError as e:
        logging.error(e)
