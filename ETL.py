import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker


def extract(name, delimiter=';', encoding='utf-8'):  # функция для загрузки данных из файлов csv в датафреймы
    return pd.read_csv(name, delimiter=delimiter, encoding=encoding)


def custom_drop_duplicates(df, subset=None):  # функция для удаления дубликатов (при вызове фукнции сравниваю
    # все значения столбцов,
    # поскольку в некоторых таблицах составные ключи повторяются с разными данными в других столбцах).
    # Тк drop_duplicates срванивает NaN-значения как одинаковые, то сначала нахожу строки c NaN-значениями,
    # чтобы они не удалялись, затем в оставшейся части датафрейма удаляю дубликаты, и уже потом объединяю
    # строки с NaN и строки с удаленными дубликатами в один датафрейм, который возвращает функция
    if subset is None:
        subset = df.columns.tolist()

    mask = df[subset].isna().any(axis=1)
    non_nan_duplicates = df[~mask].drop_duplicates(subset=subset, keep='first')
    nan_rows = df[mask]

    return pd.concat([non_nan_duplicates, nan_rows], ignore_index=True)

def load_to_db(df, table_name, engine, schema):
    # подключиться к БД
    df.columns = df.columns.str.lower()
    meta = db.MetaData(schema)
    table = db.Table(table_name, meta, autoload_with=engine, schema=schema)
    with engine.connect() as conn:
        conn.execute(table.delete())
        conn.commit()
    df.to_sql(table_name, con=engine, schema=schema,if_exists='append', index=False)
    # прервать соединение с БД


md_ledger_account_s = pd.DataFrame()
md_account_d = pd.DataFrame()
ft_balance_f = pd.DataFrame()
ft_posting_f = pd.DataFrame()
md_currency_d = pd.DataFrame()
md_exchange_rate_d = pd.DataFrame()

md_ledger_account_s = extract('md_ledger_account_s.csv', delimiter=';', encoding='cp866')
ft_balance_f = extract('ft_balance_f.csv', delimiter=';')
ft_posting_f = extract('ft_posting_f.csv', delimiter =';')
md_account_d = extract('md_account_d.csv', delimiter=';')
md_currency_d = extract('md_currency_d.csv', delimiter=';', encoding='cp866')
md_exchange_rate_d = extract('md_exchange_rate_d.csv', delimiter=';')

# можно оставить цикл для удаления первой колонки у пяти фреймов: (тогда надо добавить лист выше)
md_ledger_account_s.drop(md_ledger_account_s.columns[[0]], axis= 1 , inplace= True)
ft_balance_f.drop(ft_balance_f.columns[[0]], axis= 1 , inplace= True)
md_account_d.drop(md_account_d.columns[[0]], axis= 1 , inplace= True)
md_currency_d.drop(md_currency_d.columns[[0]], axis= 1 , inplace= True)
md_exchange_rate_d.drop(md_exchange_rate_d.columns[[0]], axis= 1 , inplace= True)

ft_posting_f.columns.values[0] = 'id'

list_of_df = [md_ledger_account_s, md_account_d, ft_balance_f, ft_posting_f, md_currency_d, md_exchange_rate_d]

for i in range(len(list_of_df)):
    list_of_df[i] = custom_drop_duplicates(list_of_df[i])

list_of_df[0] = list_of_df[0].astype({'PAIR_ACCOUNT': 'Int64'})

# подключение убрать в функцию load_to_db
database_loc = f"postgresql://student12:pheih0Ee@adhcluster2.neoflex.ru:8000/postgres"
engine = create_engine(database_loc)
# вызов функции в цикле будет
load_to_db(list_of_df[3], 'ft_posting_f', engine, 'ds')
