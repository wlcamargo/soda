import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
import pymongo
from configs import conf_mongodb, conf_big_query
from functions.create_table_bigquery import create_table_if_not_exists
from functions.write_dataframe_bigquery import write_dataframe_to_bigquery
from functions.add_metadata import add_metadata
from functions.generate_schema_bigquery import generate_bigquery_schema
from functions.setup_logger import get_logger

app_name = 'ingestion-youtube-videos-mongodb-bigquery'

# Configure the global logger
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)

# Include the program name and date in the log file
log_filename = f"{app_name}_{datetime.now().strftime('%Y%m%d')}.log"
log_file_path = os.path.join(log_folder, log_filename)

#Log config
logger = get_logger(f"{app_name}", level='INFO', log_file=log_file_path)

def log_message_error(message):
    # Helper function to log error messages
    logger.error(message)

# Main function
def execute():
    logger.info(f"Starting program execution: {app_name}")

    # Connect to the data source
    try:
        conection_source = pymongo.MongoClient(
            conf_mongodb.credential_mongodb_youtube["host"], 
            conf_mongodb.credential_mongodb_youtube["port"],
            username=conf_mongodb.credential_mongodb_youtube["username"],
            password=conf_mongodb.credential_mongodb_youtube["password"]   
        )
        db = conection_source[conf_mongodb.credential_mongodb_youtube["database"]]
        colecao_videos = db["videos"]
        logger.info("Connection to the data source successful.")
    except Exception as e:
        log_message_error(f"Error connecting to the data source: {e}")
        return

    #Run query
    try:
        documentos = colecao_videos.find()
        df_source = pd.DataFrame(list(documentos))
        df_source['_id'] = df_source['_id'].astype(str)
        logger.info("Query executed successfully.")
    except Exception as e:
        log_message_error(f"Error executing the query: {e}")
        return

    #Call the function that adds metadata to the dataframe.
    add_metadata(df_source, source="mongodb", tool="python")

    # Use to capture the schema of the dataframe.
    #generate_bigquery_schema(df_source)

    table_schema = [
        bigquery.SchemaField('_id', 'STRING'),
        bigquery.SchemaField('id_video', 'INTEGER'),
        bigquery.SchemaField('nome_video', 'STRING'),
        bigquery.SchemaField('categoria', 'STRING'),
        bigquery.SchemaField('visualizacoes', 'INTEGER'),
        bigquery.SchemaField('comentarios', 'INTEGER'),
        bigquery.SchemaField('tempo', 'STRING'),
        bigquery.SchemaField('curtidas', 'INTEGER'),
        bigquery.SchemaField('last_update', 'DATETIME'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('tool', 'STRING'),
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
    table_id_bigquery = 'youtube_videos'

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

if __name__ == "__main__":
    execute()
