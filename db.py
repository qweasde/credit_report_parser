import sqlite3
import pandas as pd

def infer_sqlite_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"
    else:
        return "TEXT"

def create_table_from_df(conn, table_name, df):
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        sql_type = infer_sqlite_type(dtype)
        col_escaped = f'"{col}"'
        columns.append(f"{col_escaped} {sql_type}")

    columns_sql = ", ".join(columns)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_sql});"

    cursor = conn.cursor()
    cursor.execute(create_table_sql)

    conn.commit()

def insert_df_to_table(conn, table_name, df):
    cursor = conn.cursor()

    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')

    # Получаем все уникальные номера договоров в текущем df
    if "Номер договора МКК" not in df_copy.columns:
        raise ValueError(f'В DataFrame для таблицы {table_name} отсутствует колонка "Номер договора МКК"')

    unique_contracts = df_copy["Номер договора МКК"].unique()

    # Получаем уже существующие номера договоров из таблицы
    cursor.execute(f"SELECT DISTINCT \"Номер договора МКК\" FROM {table_name}")
    existing_contracts = set(row[0] for row in cursor.fetchall())

    # Проверяем, есть ли пересечения
    for contract in unique_contracts:
        if contract in existing_contracts:
            print(f"[!] Пропуск вставки: договор {contract} уже есть в таблице {table_name}")
            return 0  # ничего не вставляем, если такой договор уже есть

    # Вставляем весь DataFrame
    columns = list(df_copy.columns)
    placeholders = ", ".join(["?"] * len(columns))
    columns_escaped = ", ".join([f'"{col}"' for col in columns])

    insert_sql = f"INSERT INTO {table_name} ({columns_escaped}) VALUES ({placeholders})"
    data = [tuple(row) for row in df_copy.itertuples(index=False, name=None)]

    cursor.executemany(insert_sql, data)
    conn.commit()

    print(f"[+] Добавлено строк: {cursor.rowcount} в таблицу {table_name}")
    return cursor.rowcount

def open_connection(db_path="data.db"):
    return sqlite3.connect(db_path)

def close_connection(conn):
    conn.close()