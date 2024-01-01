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


connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}'

src_conn = pyodbc.connect(connection_string)

engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

source = pd.read_sql_query(""" SELECT
IdVenda, IdCliente, Carro, Cor, DataVenda, Vendedor, ValorBruto
FROM dbo.tb_vendas; """, src_conn)

# Save the data to destination as the intial load. On the first run we load all data.
tbl_name = "stg_IncrementalLoadTest"
source.to_sql(tbl_name, engine, if_exists='replace', index=False)

# Read Target data into a dataframe
#target = pd.read_sql('Select * from public."stg_IncrementalLoadTest"', engine)
#print(target)





'''

tbl_name = "TB_VENDAS"
        # query and load save data to dataframe
query = f'select * FROM {tbl_name}'
df = pd.read_sql_query(query, src_conn)

        
df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)

# Save the data to destination as the intial load. On the first run we load all data.
tbl_name = "stg_IncrementalLoadTest"
source.to_sql(tbl_name, engine, if_exists='replace', index=False)
'''