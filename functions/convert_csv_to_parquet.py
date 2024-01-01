import pandas as pd

def csv_to_parquet(input_csv, output_parquet):
    # Carregar o CSV para um DataFrame
    df = pd.read_csv(input_csv, sep=',')
    # Salvar o DataFrame como arquivo Parquet
    df.to_parquet(output_parquet, index=False)
    print('arquivo convertido com sucesso!')

# Exemplo de uso
csv_input_file = 'C:/Users/User01/OneDrive - Educacional/Desktop/source_csv/geographic.csv'
parquet_output_file = 'C:/Users/User01/OneDrive - Educacional/Desktop/source_parquet/geographic.parquet'

def read_and_display_parquet(parquet_file):
    # Carregar o arquivo Parquet para um DataFrame
    df = pd.read_parquet(parquet_file)
    
    # Exibir o conteúdo do DataFrame
    print("Conteúdo do arquivo Parquet:")
    print(df)

# Exemplo de uso
parquet_file = 'C:/Users/User01/OneDrive - Educacional/Desktop/source_parquet/geographic.parquet'

if __name__ == "__main__":
    #csv_to_parquet(csv_input_file,parquet_output_file)
    read_and_display_parquet(parquet_file)