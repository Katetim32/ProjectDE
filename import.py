import pandas as pd
from ETL import extract, load_to_db, load_config
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

# для очистки таблицы dm_f101_round_f_v2 перед импортом данных
def connect_to_db():
    password = load_config()["password"]
    database_loc = f"postgresql://postgres:{password}@localhost:5432/postgres"
    engine = create_engine(database_loc)
    return engine

# Функция для очистки данных в таблице
def clear_table(engine, table_name, schema):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=schema, only=[table_name])
    table = Table(table_name, metadata, schema=schema)
    delete_stmt = table.delete()
    session.execute(delete_stmt)
    session.commit()
    session.close()

engine = connect_to_db()
clear_table(engine, 'dm_f101_round_f_v2', 'dm')
dm_f101_round_f_v2 = pd.DataFrame()
dm_f101_round_f_v2 = extract('dm_f101_round_f.csv', delimiter=';')
load_to_db(dm_f101_round_f_v2, 'dm_f101_round_f_v2', 'dm')