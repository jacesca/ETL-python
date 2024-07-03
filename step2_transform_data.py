import pandas as pd
import logging

from step1_extract_data import extract_from_parquet  # get_source_db_conn, extract_from_db  # noqa
from environment import hprint, print


logging.basicConfig(format='%(levelname)s: %(message)s\n', level=logging.DEBUG)


def transform_sales(raw_data: pd.DataFrame) -> pd.DataFrame:
    hprint('Transforming Raw Sales Data')
    clean_data = raw_data.copy()

    # Convert the "Order Date" column to type datetime
    clean_data["order_date"] = pd.to_datetime(clean_data["order_date"],
                                              format="%m/%d/%y %H:%M")

    # Info log regarding transformation
    logging.info("Transformed 'Order Date' column to type 'datetime'.")

    # Only keep rows with `Quantity Ordered` greater than 1
    clean_data = clean_data.loc[clean_data['quantity_ordered'] > 1, :]

    # Only keep items under ten dollars
    clean_data = clean_data.loc[clean_data['price_each'] < 10, :]

    # Filter items with total price greater than 3
    clean_data = clean_data.loc[clean_data['total_price'] > 3, :]

    # Only keep columns "Order Date", "Quantity Ordered",
    # and "Purchase Address"
    clean_data = clean_data[["order_date", "quantity_ordered",
                             "purchase_address", "price_each"]]
    print('Clean Data Types:', clean_data.dtypes)

    # Debug-level logs for the DataFrame before and after transformation
    logging.debug(f"Shape of the DataFrame before transformation: {raw_data.shape}")   # noqa
    logging.debug(f"Shape of the DataFrame after transformation: {clean_data.shape}")  # noqa

    # Return the filtered DataFrame
    return clean_data


if __name__ == '__main__':
    # db_engine = get_source_db_conn()
    # _ = extract_from_db(db_engine, 'raw_stock_data')

    df_sales_raw = extract_from_parquet('sales')
    try:
        df_sales_clean = transform_sales(df_sales_raw)
        logging.info("Successfully filtered DataFrame by 'Total Price'")
    except KeyError as e:
        logging.warning(f"{e}: Cannot filter DataFrame by 'Total Price'")

        # Create the "total_price" column, transform the updated DataFrame
        df_sales_raw["total_price"] = df_sales_raw["price_each"] * df_sales_raw["quantity_ordered"]  # noqa
        df_sales_clean = transform_sales(df_sales_raw)

    # Validating
    hprint('Making some validations:')
    print('Min quantity_ordered (Should be greater than 1):',
          df_sales_clean.nsmallest(1, 'quantity_ordered'))
    print(' Max price_each (Should be lower than 10):',
          df_sales_clean.nlargest(1, 'price_each'))
