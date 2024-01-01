import pandas as pd
from google.cloud import bigquery
from datetime import datetime

def generate_bigquery_schema(df_source):
    """
    Gera um esquema de tabela BigQuery a partir de um DataFrame.

    Parâmetros:
    - df_source: O DataFrame a partir do qual o esquema será gerado.

    Imprime:
    - O esquema da tabela no formato desejado.
    """
    for column_name, dtype in df_source.dtypes.iteritems():
        bq_type = "STRING"  # Tipo padrão é STRING

        # Mapeie os tipos do DataFrame para os tipos do BigQuery
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            bq_type = "DATETIME"

        # Imprima a linha correspondente ao esquema
        print(f"bigquery.SchemaField('{column_name}', '{bq_type}'),")



