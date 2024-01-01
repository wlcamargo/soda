import os
import pandas as pd

def read_and_concat_csv(folder_path):
    # Inicializa um DataFrame vazio para armazenar a união dos arquivos CSV
    df_source = pd.DataFrame()

    # Itera sobre cada arquivo CSV na pasta
    for arquivo_csv in os.listdir(folder_path):
        if arquivo_csv.endswith('.csv'):
            caminho_arquivo = os.path.join(folder_path, arquivo_csv)
            df = pd.read_csv(caminho_arquivo, sep=';')
            
            # Concatena o DataFrame atual com o DataFrame acumulado
            df_source = pd.concat([df_source, df], ignore_index=True)

    # Exporta a união dos arquivos CSV para um arquivo CSV
    
    return df_source
