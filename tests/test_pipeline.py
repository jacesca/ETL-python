import pandas as pd
import pytest

from step1_extract_data import extract_from_parquet
from step2_transform_data import transform_sales
# from step3_savingdatastage import saving_data


# Build unit tests for the pipeline
def test_extracted_data():
    """Validate the extracted data type"""
    raw_df = extract_from_parquet('sales')
    assert isinstance(raw_df, pd.DataFrame), \
           "Returned data is not the same type"


class TestClass:
    @pytest.fixture(scope="class", autouse=True)
    def raw_data(self):
        raw_df = extract_from_parquet('sales')
        raw_df["total_price"] = raw_df["price_each"] * raw_df["quantity_ordered"]  # noqa
        return raw_df

    @pytest.fixture(scope="class", autouse=True)
    def clean_data(self, raw_data):
        return transform_sales(raw_data)

    def test_type_extracted_data(self, raw_data):
        """Validate the extracted data type"""
        assert isinstance(raw_data, pd.DataFrame), \
               "Returned data is not the same type"

    def test_shape_extracted_data(self, raw_data):
        """Validate the shaoe of the extracted data"""
        assert raw_data.shape[1] == 7

    def test_type_clean_data(self, clean_data):
        """Validate the clean data type"""
        assert isinstance(clean_data, pd.DataFrame), \
               "Cleaned data is not the same type"

    def test_rows_clean_data(self, clean_data, raw_data):
        """Validate the size of the clean data"""
        assert clean_data.shape[0] <= raw_data.shape[0], \
               "Cleaned data is bigger than extracted data"

    def test_quantity_ordered_clean_data(self, clean_data):
        """Validate that quantity ordered greater than 1"""
        assert (clean_data['quantity_ordered'] > 1).all(), \
               "There are quantity orders equal or lower than 1"

    def test_price_clean_data(self, clean_data):
        """Validate that price under $10"""
        assert (clean_data['price_each'] < 10).all(), \
               "There are items with price greater than 10 dollars"


# To avoid the warning "DeprecationWarning:
# datetime.datetime.utcfromtimestamp() is deprecated...",
# we need to create the pytest.ini in the app root folder with the code
#       [pytest]
#       addopts = -p no:warnings
# To execute the tests: in root run
#       `pytest`                            # Execute all
#       `pytest tests/test_pipeline.py`     # Execute specific file
#       `pytest -m tests.test_pipeline`     # Execute specific file
#       `pytest tests/test_pipeline.py::TestClass`      # Execute specific class  # noqa
#       `pytest tests/test_pipeline.py::TestClass::test_extracted_data`     # Execute specific method  # noqa
