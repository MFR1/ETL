from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from loguru import logger


# Postgres credentials
uid = 'postgres'
pwd = 'admin'
database = "postgres"
host_server = '127.0.0.1'
host_port = '5432'


# Extracting data from a source (Postgres in this case)
@logger.catch()
def extract():
    try:
        conn = psycopg2.connect(
            database=database, user=uid, password=pwd, host=host_server, port=host_port
        )

        cursor = conn.cursor()
        cursor.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")

        src_tables = cursor.fetchall()
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', conn)
            load(df, tbl[0])

    except Exception as e:
        logger.error("Data load error: " + str(e))


# Loading data into target (SQLITE DB)
@logger.catch()
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'sqlite:///users.db')
        logger.info(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        df.to_sql(f'{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)

        logger.info("Data loaded/imported Successfully")

    except Exception as e:
        logger.error("Data load error: " + str(e))


if __name__ == "__main__":
    logger.add("logging/bot_{time}.log", level="TRACE", rotation="100 MB")

    extract()
