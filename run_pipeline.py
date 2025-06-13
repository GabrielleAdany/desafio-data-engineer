import sys
from pathlib import Path
import logging
import os

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("pipeline.log"), # Salva logs em um arquivo
                        logging.StreamHandler(sys.stdout)   # Exibe logs no console
                    ])
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent # Diretório onde run_pipeline.py está 
sys.path.append(str(SCRIPT_DIR / "src"))

try:
    # Importa as funções principais de cada camada
    from bronze import run_bronze_layer
    from silver import run_silver_layer 
    from gold import run_gold_layer     
    from persistence import persist_gold_to_db 
except ImportError as e:
    logger.error(f"Erro ao importar módulos das camadas: {e}")

    sys.exit(1)

def execute_pipeline():
    """
    Orquestra a execução de todas as camadas do pipeline de dados:
    Bronze -> Silver -> Gold -> Persistência em Banco de Dados.
    """
    logger.info("Iniciando o Pipeline de Dados...")

    # --- Camada Bronze: Ingestão de Dados Brutos ---
    logger.info("--- EXECUTANDO CAMADA BRONZE (INGESTÃO) ---")
    try:
        run_bronze_layer() 
        logger.info("Camada Bronze concluída com sucesso.")
    except Exception as e:
        logger.error(f"Falha na Camada Bronze: {e}", exc_info=True)
        sys.exit(1)

    # --- Camada Silver ---
    logger.info("--- EXECUTANDO CAMADA SILVER (LIMPEZA E TRANSFORMAÇÃO) ---")
    try:
        run_silver_layer() 
        logger.info("Camada Silver concluída com sucesso.")
    except Exception as e:
        logger.error(f"Falha na Camada Silver: {e}", exc_info=True)
        sys.exit(1)

    # --- Camada Gold ---
    logger.info("--- EXECUTANDO CAMADA GOLD (MÉTRICAS E ENRIQUECIMENTO) ---")
    try:
        run_gold_layer() 
        logger.info("Camada Gold concluída com sucesso.")
    except Exception as e:
        logger.error(f"Falha na Camada Gold: {e}", exc_info=True)
        sys.exit(1)

    # --- Persistência em Banco Relacional ---
    logger.info("--- EXECUTANDO CAMADA DE PERSISTÊNCIA (GOLD -> DB) ---")
    try:
        persist_gold_to_db()
        logger.info("Camada de Persistência concluída com sucesso. Dados salvos no banco de dados.")
    except Exception as e:
        logger.error(f"Falha na Camada de Persistência: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Pipeline de Dados concluído com sucesso!")

if __name__ == "__main__":
    execute_pipeline()