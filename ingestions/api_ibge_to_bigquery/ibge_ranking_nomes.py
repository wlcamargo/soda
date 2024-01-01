import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import requests
from configs import conf_big_query
from functions.create_table_bigquery import create_table_if_not_exists
from functions.write_dataframe_bigquery import write_dataframe_to_bigquery
from functions.add_metadata import add_metadata
from functions.generate_schema_bigquery import generate_bigquery_schema
from functions.setup_logger import get_logger

app_name = 'ingestion-ibge-ranking-nomes-api-bigquery'

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
    
    url = "https://servicodados.ibge.gov.br/api/v2/censos/nomes/ranking"
        
    # Parâmetros da solicitação (exemplos)
    params = {"decada": 1980, "localidade": 33, "sexo": "M"}

    # Faz a solicitação à API
    try:
    # URL base da API do IBGE para o ranking de nomes
        response = requests.get(url, params=params)
        logger.info("Successfully connected to the API")
    except Exception as e:
        log_message_error(f"Error when attempting to connect to the API: {e}")
        return

    # Verifica se a solicitação foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        # Converte os dados JSON para um objeto Python
        data = response.json()

        # Verifica se a estrutura do JSON é como esperado
        if isinstance(data, list) and data:
            # Extrai as informações relevantes do JSON
            result_list = []
            for item in data:
                localidade = item.get("localidade", "")
                sexo = params.get("sexo", "")
                decada = params.get("decada", "")
                res_list = item.get("res", [])

                for res_item in res_list:
                    nome = res_item.get("nome", "")
                    frequencia = res_item.get("frequencia", "")
                    ranking = res_item.get("ranking", "")

                    result_list.append(
                        {
                            "Localidade": localidade, 
                            "Decada": decada, 
                            "Sexo": sexo, 
                            "Nome": nome, 
                            "Frequencia": frequencia, 
                            "Ranking": ranking
                        })

            # Cria um DataFrame com as informações extraídas
            df_source = pd.DataFrame(result_list)
            # Exibe o DataFrame
        else:
           log_message_error(f"JSON format is not as expected: {e}")
    else:
        log_message_error(f"Error when attempting to connect to the API: {e}")

    #Call the function that adds metadata to the dataframe.
    add_metadata(df_source, source="api_ibge", tool="python")

    #Call the function that generates the schema of the table to be created.
    #generate_bigquery_schema(df_source)

    #Example Schema
    table_schema = [
        bigquery.SchemaField('Localidade', 'STRING'),
        bigquery.SchemaField('Decada', 'INTEGER'),
        bigquery.SchemaField('Sexo', 'STRING'),
        bigquery.SchemaField('Nome', 'STRING'),
        bigquery.SchemaField('Frequencia', 'INTEGER'),
        bigquery.SchemaField('Ranking', 'INTEGER'),
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
    table_id_bigquery = 'api_ibge'

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
