import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from datetime import datetime
import pandas as pd
import pyodbc
from google.cloud import bigquery
from google.oauth2 import service_account
from configs import conf_sql_server, conf_big_query
from functions.create_table_bigquery import create_table_if_not_exists
from functions.write_dataframe_bigquery import write_dataframe_to_bigquery
from functions.add_metadata import add_metadata
from functions.generate_schema_bigquery import generate_bigquery_schema
from functions.setup_logger import get_logger
from functions import read_and_concat_excel, read_and_concat_csv

#Future Scope
#Melhorar a criação da tabela com o schema correto
#Incluir logs

# Call the function that performs the concatenation of Excel files."
folder_source = 'C:/Users/User01/OneDrive - Educacional/Desktop/source_csv'
df_source = read_and_concat_csv.read_and_concat_csv(folder_source)

def create_table_sql_server(conn, table_name, df, primary_key=None):
    cursor = conn.cursor()

    # Extract column names and data types from the DataFrame
    columns = df.columns
    data_types = [df[col].dtype for col in columns]

    # Generate the CREATE TABLE statement
    create_table_sql = f"CREATE TABLE {table_name} ("

    for col, data_type in zip(columns, data_types):
        sql_type = get_sql_server_type(data_type)
        create_table_sql += f"{col} {sql_type}, "

    if primary_key:
        create_table_sql += f"PRIMARY KEY ({', '.join(primary_key)})"

    create_table_sql = create_table_sql.rstrip(', ') + ")"

    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")
    finally:
        cursor.close()

def get_sql_server_type(pandas_dtype):
    sql_server_type_mapping = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'NVARCHAR(MAX)',
        'datetime64[ns]': 'DATETIME'
    }
    return sql_server_type_mapping.get(str(pandas_dtype), 'NVARCHAR(MAX)')

def insert_data_into_sql_server(conn, table_name, df):
    cursor = conn.cursor()

    # Generate the INSERT INTO statement
    insert_into_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})"
    
    try:
        cursor.executemany(insert_into_sql, df.values.tolist())
        conn.commit()
        print(f"Data inserted into table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error inserting data into table '{table_name}': {e}")
    finally:
        cursor.close()

# Connection to SQL Server
connection_str = conf_sql_server.credential_sql_server_escola_analise_dados
conn = pyodbc.connect(connection_str)

# Define table name and primary key
table_name = 'geographic'
primary_key = ['IDTransacao']

# Create the table in SQL Server
create_table_sql_server(conn, table_name, df_source, primary_key)

# Insert data into the table
insert_data_into_sql_server(conn, table_name, df_source)

# Close the connection
conn.close()
