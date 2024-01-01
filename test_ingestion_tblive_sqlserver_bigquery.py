from datetime import datetime
from ingestions.sqlserver_to_bigquery.tb_live import execute_app_main
import pytest

id_column = 'id_cliente'

@pytest.fixture(scope='module')
def df_source():
    df = execute_app_main()
    return df

def test_df_not_empty(df_source):
    # Test if the DataFrame is not empty
    assert not df_source.empty, "The DataFrame is empty."

def test_df_expected_columns(df_source):
    # Test if the expected columns are present in the DataFrame
    expected_columns = [f'{id_column}', 'last_update', 'source', 'tool']
    assert all(col in df_source.columns for col in expected_columns), "Expected columns not found."

def test_no_null_values_in_critical_columns(df_source):
    # Test if there are no null values in critical columns
    critical_columns = [f'{id_column}', 'last_update']
    assert not df_source[critical_columns].isnull().values.any(), "Null values found in critical columns."

def test_no_duplicate_values_in_id_column(df_source):
    # Tests if there are no duplicate values in the 'id_cliente' column
    assert not df_source[f'{id_column}'].duplicated().any(),  "Duplicate values found in the 'id_cliente' column."

def test_date_format_in_last_update(df_source):
    # Test if all dates in the 'last_update' column are in the correct format
    assert df_source['last_update'].apply(lambda x: isinstance(x, datetime)).all(), "Invalid dates found in the 'last_update' column."

def test_specific_values_in_source_and_tool(df_source):
    # Test if the 'source' and 'tool' columns contain only specific values
    valid_sources = ['sql_server']
    valid_tools = ['python']
    assert df_source['source'].isin(valid_sources).all(), "Invalid values found in the 'source' column."
    assert df_source['tool'].isin(valid_tools).all(), "Invalid values found in the 'tool' column."

def test_no_duplicate_rows(df_source):
    # Test if there are no duplicate rows in the DataFrame
    assert not df_source.duplicated().any(), "Duplicate rows found in the DataFrame."

def test_id_column_type_is_integer(df_source):
    # Test if the 'id_cliente' column has the integer data type
    assert df_source[f'{id_column}'].dtype == 'int64', "The 'id_columns' column is not of integer type."