import pandas as pd
from pathlib import Path
import sqlite3

GOLD_DIR = Path(__file__).parent.parent / "datalake" / "gold"

DATABASE_DIR = Path(__file__).parent.parent / "database" 
DATABASE_DIR.mkdir(parents=True, exist_ok=True)

# SQLite
DATABASE_NAME_SQLITE = DATABASE_DIR / "movies.db"


def create_sqlite_connection():
    """Cria e retorna uma conexão com o banco de dados SQLite."""
    conn = sqlite3.connect(DATABASE_NAME_SQLITE)
    print(f"Conectado ao banco de dados SQLite: {DATABASE_NAME_SQLITE}")
    return conn

def persist_gold_to_db():
    """
    Carrega os arquivos Parquet da camada Gold e persiste-os em um banco de dados.
    """

    conn = create_sqlite_connection()

    try:
        # Lista todos os arquivos .parquet na pasta gold
        parquet_files = list(GOLD_DIR.glob("*.parquet"))

        if not parquet_files:
            print(f"Nenhum arquivo Parquet encontrado em: {GOLD_DIR}. Verifique se a camada Gold foi executada.")
            return

        for parquet_file in parquet_files:
            try:
                df = pd.read_parquet(parquet_file)
                table_name = parquet_file.stem 
                print(f"Carregando {table_name}.parquet e salvando na tabela '{table_name}'...")

                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"  {len(df)} registros salvos na tabela '{table_name}'.")

            except Exception as e:
                print(f"Erro ao processar o arquivo {parquet_file}: {e}")

    finally:

        if conn:
            conn.close()
            print("Conexão com SQLite fechada.")

if __name__ == "__main__":

    persist_gold_to_db()