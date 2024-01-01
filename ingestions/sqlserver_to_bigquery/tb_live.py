import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
import pyodbc
from configs import conf_sql_server, conf_big_query
from functions.create_table_bigquery import create_table_if_not_exists
from functions.write_dataframe_bigquery import write_dataframe_to_bigquery
from functions.add_metadata import add_metadata
from functions.generate_schema_bigquery import generate_bigquery_schema
from functions.setup_logger import get_logger

app_name = 'ingestion-tb_live-sqlserver-bigquery'

# Configure the global logger
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)

# Include the program name and date in the log file
log_filename = f"{app_name}_{datetime.now().strftime('%Y%m%d')}.log"
log_file_path = os.path.join(log_folder, log_filename)

# Log config
logger = get_logger(f"{app_name}", level='INFO', log_file=log_file_path)

def log_message_error(message):
    # Helper function to log error messages
    logger.error(message)

# Main function
def execute_app_main():
    logger.info(f"Starting program execution: {app_name}")

    # Connect to the data source
    try:
        connection_source = pyodbc.connect(conf_sql_server.credential_sql_server_escola_analise_dados)
        logger.info("Connection to the data source successful.")
    except Exception as e:
        log_message_error(f"Error connecting to the data source: {e}")
        return

    # Execute the SQL query
    try:
        sql_file = 'query/sql_server/query_tb_live.sql'
        query = open(sql_file, 'r').read()
        df_source = pd.read_sql_query(query, connection_source)
        logger.info("SQL query executed successfully.")
    except Exception as e:
        log_message_error(f"Error executing the SQL query: {e}")
        return

    # Add metadata to the dataframe
    add_metadata(df_source, source="sql_server", tool="python")

    # Use to capture the schema of the dataframe.
    #generate_bigquery_schema(df_source)

    # Define schema
    table_schema = [
        bigquery.SchemaField('id_cliente', 'INTEGER', description='abc', ),
        bigquery.SchemaField('nome_cliente', 'STRING', description='client name', ),
        bigquery.SchemaField('last_update', 'DATETIME', description='date of the last table update', ),
        bigquery.SchemaField('source', 'STRING', description='data extraction source', ),
        bigquery.SchemaField('tool', 'STRING', description='used tool', )
    ]
 
    # Authenticate for BigQuery
    try:
        path_to_bigquery_credentials = conf_big_query.credential_big_query
        bigquery_credentials = service_account.Credentials.from_service_account_info(conf_big_query.credential_big_query)
        logger.info("Connection to BigQuery successful.")
    except Exception as e:
        log_message_error(f"Error connecting to BigQuery: {e}")
        return

    # Destination in BigQuery
    client_bigquery = bigquery.Client(credentials=bigquery_credentials)
    dataset_id_bigquery = 'bronze_dev'
    table_id_bigquery = 'tb_live'

    # Create table in BigQuery
    table_bigquery = create_table_if_not_exists(logger, client_bigquery, dataset_id_bigquery, table_id_bigquery, schema=table_schema)

    # Job configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    # Load data into BigQuery
    write_dataframe_to_bigquery(logger, client_bigquery, df_source, table_bigquery, job_config)

    #End Application
    logger.info(f"End program execution was successful: {app_name}")

    return df_source  # Return the DataFrame

if __name__ == "__main__":
   execute_app_main()