import json
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import socket


def load_config():
    with open("config/config.json") as f:
        config = json.load(f)
    return config


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


# функция преобразования данных в датафреймах
def transform(list_of_df):
    for index, df in enumerate(list_of_df):
        if id(df) == id(ft_posting_f):
            df.columns.values[0] = 'id'
        else:
            if id(df) == id(md_ledger_account_s):
                df = df.astype(
                    {'PAIR_ACCOUNT': 'Int64'})  # в датафрейме был тип float64, преобразую в Int64
                # поскольку в базе этот столбец имеет тип varchar(5)
                list_of_df[index] = df
            df.drop(df.columns[[0]], axis=1, inplace=True)
    for el in range(len(list_of_df)):
        list_of_df[el] = custom_drop_duplicates(list_of_df[el])
    return list_of_df


# функция для логирования
def log_to_db(message, engine, tablename, operation, status, client_addr, client_hostname):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    meta = db.MetaData('logs')
    table_for_log = db.Table('logs', meta, autoload_with=engine, schema='logs')
    log_entry = {
        'message': message,
        'date_start': pd.Timestamp.now(),
        'tablename': tablename,
        'operation': operation,
        'status': status,
        'client_addr': client_addr,
        'client_hostname': client_hostname
    }
    session.execute(table_for_log.insert().values(log_entry))
    session.commit()
    session.close()


# функция для загрузки данных из датафреймов в базу данных
def load_to_db(df, table_name, schema):
    # подключение к базе данных на локальном компьютере
    password = load_config()["password"]
    database_loc = f"postgresql://postgres:{password}@localhost:5432/postgres"
    engine = create_engine(database_loc)

    client_hostname = socket.gethostname()
    client_addr = socket.gethostbyname(client_hostname)

    log_to_db(f"Начало загрузки данных в таблицу {table_name}", engine, table_name, 'UPSERT', 'started',
              client_addr, client_hostname)

    try:
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        df.columns = df.columns.str.lower()# преобразование полей датафреймов в нижний регистр (чтобы совпадали с полями в базе)
        df.replace({np.nan: None}, inplace=True)
        data_list = df.to_dict(orient='records')
        meta = db.MetaData(schema)
        table = db.Table(table_name, meta, autoload_with=engine, schema=schema)
        primary_keys = [key.name for key in table.primary_key] #первичные ключи таблицы
        # вставка данных в таблицу, или обновление при конфликте
        stmt = insert(table).values(data_list).on_conflict_do_update(
            index_elements=primary_keys,
            set_={col.name: col for col in insert(table).excluded if col.name not in primary_keys}
        )
        session.execute(stmt)
        session.commit()
        session.close()
        log_to_db(f"Окончание загрузки данных в таблицу {table_name}", engine, table_name, 'UPSERT', 'succeed',
                  client_addr, client_hostname)

    except Exception as e:
        log_to_db(f"Ошибка при загрузке данных в таблицу {table_name}: {str(e)}", engine, table_name, 'UPSERT',
                  'failed', client_addr, client_hostname)


# создание датафреймов
md_ledger_account_s = pd.DataFrame()
md_account_d = pd.DataFrame()
ft_balance_f = pd.DataFrame()
ft_posting_f = pd.DataFrame()
md_currency_d = pd.DataFrame()
md_exchange_rate_d = pd.DataFrame()

# процесс Extract - извлечение данных
md_ledger_account_s = extract('files/md_ledger_account_s.csv', delimiter=';', encoding='cp866')
ft_balance_f = extract('files/ft_balance_f.csv', delimiter=';')
ft_posting_f = extract('files/ft_posting_f.csv', delimiter =';')
md_account_d = extract('files/md_account_d.csv', delimiter=';')
md_currency_d = extract('files/md_currency_d.csv', delimiter=';', encoding='cp866')
md_exchange_rate_d = extract('files/md_exchange_rate_d.csv', delimiter=';')

# процесс Transform - преобразование данных
list_of_df = [md_ledger_account_s, md_account_d, ft_balance_f, ft_posting_f, md_currency_d, md_exchange_rate_d]
list_of_table_names = ['md_ledger_account_s', 'md_account_d', 'ft_balance_f', 'ft_posting_f', 'md_currency_d',
                      'md_exchange_rate_d']
list_of_df = transform(list_of_df)

# процесс Load - загрузка данных в базу
for i in range(len(list_of_df)):
    load_to_db(list_of_df[i], list_of_table_names[i], 'ds')
    if i == 0:
        time.sleep(5)
