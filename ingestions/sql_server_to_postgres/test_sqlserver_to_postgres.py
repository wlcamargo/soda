import pandas as pd
import numpy as np
import pytest
from numpy import nan
from sql_server_to_postgres.sqlserver_to_postgres import extract, load 


# get data
@pytest.fixture(scope='session', autouse=True)
def df():
    # Will be executed before the first test
    df, tbl = extract()
    yield df
    # Will be executed after the last test
    load(df, tbl)
#

# check if column exists
def test_col_exists(df):
    name="IdVenda"
    assert name in df.columns

# check for nulls
def test_null_check(df):
    assert df['IdVenda'].notnull().all()

# check values are unique
def test_unique_check(df):
    assert pd.Series(df['IdVenda']).is_unique

# check data type
def test_productkey_dtype_int(df):
    assert (df['IdVenda'].dtype == int or df['IdVenda'].dtype == np.int64)

