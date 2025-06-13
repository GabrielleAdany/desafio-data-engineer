import shutil
from pathlib import Path
import logging
import sys

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "dados"
BRONZE_DIR = BASE_DIR / "datalake" / "bronze"

def run_bronze_layer():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Caminho RAW_DIR que o script está usando: {RAW_DIR.resolve()}")
    logger.info(f"Caminho BRONZE_DIR que o script está usando: {BRONZE_DIR.resolve()}")

    raw_files = list(RAW_DIR.glob("*.*"))

    if not raw_files:
        logger.warning(f"Nenhum arquivo encontrado na pasta de dados: {RAW_DIR.resolve()}.")
        return

    for file in raw_files:
        dest = BRONZE_DIR / file.name
        shutil.copy(file, dest)
        logger.info(f"Arquivo '{file.name}' copiado para '{dest}'")

if __name__ == "__main__":

    run_bronze_layer()