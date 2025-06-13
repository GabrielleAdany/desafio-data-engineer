import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys

logger = logging.getLogger(__name__)


# Caminhos
SILVER_SOURCE_PATH = Path(__file__).parent.parent / "datalake" / "silver" / "movies_processed.parquet"
GOLD_DEST_DIR = Path(__file__).parent.parent / "datalake" / "gold" 


# Função de categorização 
def categorizar_filmes_df(df):
    """Categoriza filmes por tipo e gênero e adiciona a coluna 'categoria' ao DataFrame."""
    
    # Detectar documentários
    df['is_documentary'] = df['filme'].str.contains(
        r'documentary|nature|review|live|bonus', case=False, na=False
    )
    
    # Detectar sequels/series
    df['is_sequel'] = df['filme'].str.contains(
        r'\b(?:2|3|4|II|III|IV)\b|part|volume', case=False, na=False
    )
    
    # Detectar animações
    df['is_animation'] = df['filme'].str.contains(
        r'man|cartoon|animated', case=False, na=False
    )
    
    # Categorias principais
    def get_categoria(row):
        if row['is_documentary']:
            return 'Documentário'
        elif row['is_animation']:
            return 'Animação'
        elif row['is_sequel']:
            return 'Sequel/Série'
        else:
            return 'Filme'
    
    df['categoria'] = df.apply(get_categoria, axis=1)
    
    return df

def run_gold_layer():
    """
    Executa a lógica completa da Camada Gold:
    Lê o Parquet da camada Silver, categoriza os filmes e salva em Parquet por categoria.
    """
    logger.info("Iniciando a Camada Gold: Métricas e Enriquecimento...")

    # Carregar dados da camada Silver
    try:
        df = pd.read_parquet(SILVER_SOURCE_PATH) 
        logger.info(f"Dados carregados da camada Silver: {len(df)} filmes.")
        logger.info(f"Período dos dados: {df['ano'].min()} - {df['ano'].max()}")
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo Parquet '{SILVER_SOURCE_PATH}' não encontrado na camada Silver. Certifique-se de que a camada Silver foi executada.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro ao carregar dados da camada Silver: {e}", exc_info=True)
        sys.exit(1)

    # Categorizar os filmes
    logger.info("Iniciando categorização dos filmes...")
    df = categorizar_filmes_df(df.copy())
    logger.info("Categorização concluída.")

    # Criar diretório de destino na Gold se não existir
    GOLD_DEST_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Salvando DataFrames categorizados em: {GOLD_DEST_DIR}")

    # Criar um DataFrame por categoria e salvar como Parquet
    categorias = df["categoria"].unique()
    
    for cat in categorias:
        df_cat = df[df["categoria"] == cat].copy()

        df_cat = df_cat[["id", "filme", "ano"]]

        filename = cat.lower().replace("/", "_").replace(" ", "_") + ".parquet" 
        filepath = GOLD_DEST_DIR / filename
        
        df_cat.to_parquet(filepath, index=False)
        logger.info(f"Arquivo salvo: {filepath} ({len(df_cat)} registros)")
    
    logger.info("Camada Gold concluída.")

if __name__ == "__main__":

    run_gold_layer()