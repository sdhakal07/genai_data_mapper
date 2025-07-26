# snowflake_loader.py
import snowflake.connector
import pandas as pd
from config import SNOWFLAKE_CONFIG

def create_table_if_not_exists(cursor, table_name, df):
    columns = ", ".join([f'{col} STRING' for col in df.columns])
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(sql)

def load_to_snowflake(df: pd.DataFrame, table_name: str):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()
    try:
        create_table_if_not_exists(cs, table_name, df)
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cs.executemany(sql, df.values.tolist())
    finally:
        cs.close()
        conn.close()
