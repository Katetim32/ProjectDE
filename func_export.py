import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from ETL import load_config

# функция для вызова функции из базы данных
def get_min_max_credit_debet(engine, schema, function, oper_date):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    query = db.text(f"SELECT * FROM {schema}.{function}({oper_date})")
    result = session.execute(query)

    df = pd.DataFrame(result, columns=['oper_date', 'min_credit_amount', 'max_credit_amount',
                                       'min_debet_amount', 'max_debet_amount'])
    session.close()
    return df

password = load_config()["password"]
database_loc = f"postgresql://postgres:{password}@localhost:5432/postgres"
engine = create_engine(database_loc)

oper_date = '\'2018-01-09\''
file_name = 'min_max_sum_credit_debet.csv'
min_max_credit_debet = get_min_max_credit_debet(engine, 'ds', 'min_max_sum_credit_debet', oper_date)
min_max_credit_debet.to_csv(file_name, sep=';', index=False)



