import pandas as pd
from pathlib import Path
import re
import logging

logger = logging.getLogger(__name__)

# O arquivo de origem na camada Bronze
BRONZE_SOURCE_PATH = Path(__file__).parent.parent / "datalake" / "bronze" / "movies.csv"

# O diretório de destino na camada Silver
SILVER_DIR = Path(__file__).parent.parent / "datalake" / "silver"

# Função para processar os dados
def processar_dados_bronze_to_silver_df(dados_str):
    """
    Processa a string de dados brutos e retorna um DataFrame Pandas.
    """
    padrao = r'(\d+);\(([^,]+), (\d{4})\)'
    matches = re.findall(padrao, dados_str)
    
    data = []
    for match in matches:
        data.append({
            'id': int(match[0]),
            'filme': match[1].strip(),
            'ano': int(match[2])
        })
    
    return pd.DataFrame(data)

# Função para salvar na camada silver 
def salvar_silver_dataframe(dataframe, diretorio_destino, nome_arquivo="movies_processed.parquet"):
    """
    Salva o DataFrame processado na camada Silver como Parquet.
    """
    diretorio_destino.mkdir(parents=True, exist_ok=True)
    caminho_arquivo = diretorio_destino / nome_arquivo
    dataframe.to_parquet(caminho_arquivo, index=False)
    
    logger.info(f"Arquivo salvo com sucesso em: {caminho_arquivo}")
    logger.info(f"Registros salvos na camada Silver: {len(dataframe)}")

def run_silver_layer():
    """
    Executa a lógica completa da Camada Silver:
    Lê o CSV da camada Bronze, processa os dados e salva em Parquet na camada Silver.
    """
    logger.info("Iniciando a Camada Silver: Limpeza e Transformação...")

    # Ler o conteúdo do arquivo CSV da camada Bronze
    try:
        with open(BRONZE_SOURCE_PATH, 'r', encoding='utf-8') as file:
            dados = file.read()
        logger.info(f"Dados brutos lidos de: {BRONZE_SOURCE_PATH}")
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo '{BRONZE_SOURCE_PATH}' não encontrado na camada Bronze. Certifique-se de que a camada Bronze foi executada.")
        raise
    except Exception as e:
        logger.error(f"Erro ao ler arquivo da Camada Bronze: {e}", exc_info=True)
        raise

    df = processar_dados_bronze_to_silver_df(dados)
    
    logger.info("DataFrame criado na camada Silver:")
    logger.info(f"Primeiras 5 linhas:\n{df.head().to_string()}") 
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Tipos de dados:\n{df.dtypes.to_string()}")

    salvar_silver_dataframe(df, SILVER_DIR)
    
    logger.info("Camada Silver concluída.")
    return df 

if __name__ == "__main__":
    
    run_silver_layer()