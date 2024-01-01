from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.engine import URL
import pyodbc

# get password from environment variable
pwd = 'mudar@123'
uid = 'python'

# SQL Server details
driver = "{SQL Server Native Client 11.0}"
server = "localhost"
database = "ESCOLA_ANALISE_DADOS"

# PostgreSQL details
postgres_user = "postgres"
postgres_password = "postgres"
postgres_host = "localhost"
postgres_port = "5433"
postgres_database = "Adventureworks"

# extract data from SQL Server
def extract():
    try:
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}'
        src_conn = pyodbc.connect(connection_string)
        tbl_name = "TB_VENDAS"
        # query and load save data to dataframe
        query = f'select * FROM {tbl_name}'
        df = pd.read_sql_query(query, src_conn)
        return df, tbl_name
    except Exception as e:
        print("Data extract error: " + str(e))
        return None, None

# load data to PostgreSQL
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to PostgreSQL
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

if __name__ == "__main__":
    data, table_name = extract()

    if data is not None:
        load(data, table_name)
    else:
        print("Extraction failed. Check the error message above for details.")
