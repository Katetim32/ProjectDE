import time
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import socket
from ETL import load_config, log_to_db


def extract_from_db(engine, table_name, schema):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    metadata = db.MetaData(schema=schema)
    table = db.Table(table_name, metadata, autoload_with=engine)
    query = db.select(table)
    result = session.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    session.close()

    return df


def export(df, file_name, engine):
    client_hostname = socket.gethostname()
    client_addr = socket.gethostbyname(client_hostname)
    log_to_db(f"Начало экспорта данных в csv-файл {file_name}", engine, 'dm_f101_round_f', 'Export', 'started',
              client_addr, client_hostname)
    time.sleep(5)
    try:
        df.to_csv(file_name, sep=';', index=False)
        log_to_db(f"Окончание экспорта данных в csv-файл {file_name}", engine, 'dm_f101_round_f', 'Export', 'succeed',
              client_addr, client_hostname)
    except Exception as e:
        log_to_db(f"Ошибка при экспорте данных в csv-файл{file_name}: {str(e)}", engine, 'dm_f101_round_f', 'Export', 'succeed',
                  client_addr, client_hostname)


password = load_config()["password"]
database_loc = f"postgresql://postgres:{password}@localhost:5432/postgres"
engine = create_engine(database_loc)

dm_f101_round_f = extract_from_db(engine, 'dm_f101_round_f', 'dm')
export(dm_f101_round_f, 'dm_f101_round_f.csv', engine)
