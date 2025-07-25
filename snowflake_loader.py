import re
import snowflake.connector
import pandas as pd
from config import SNOWFLAKE_CONFIG

def create_table_if_not_exists(cursor, table_name, df):
    columns = ", ".join([f'"{col}" STRING' for col in df.columns])
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(sql)

def load_raw_to_snowflake(df: pd.DataFrame, table_name: str):

    # Drop columns with missing/invalid headers
    df_clean = df.loc[:, ~df.columns.to_series().apply(
        lambda col: pd.isna(col) or str(col).strip() == "" or str(col).strip().lower() == "nan"
    )]

    # Drop known problematic system columns
    for col_to_drop in ['row_no', 'row_no_col']:
        if col_to_drop in df_clean.columns:
            df_clean = df_clean.drop(columns=[col_to_drop])

    # Replace NaN cell values with None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    # Clean column names for Snowflake (only basic cleaning: lowercase and replace spaces with _)
    def clean_columns(cols):
        cleaned = []
        seen = set()
        for idx, col in enumerate(cols):
            col_str = str(col).strip().lower()
            col_str = re.sub(r'\s+', '_', col_str)
            if col_str == "" or not col_str[0].isalpha():
                col_str = f"col_{idx}"
            while col_str in seen:
                col_str = f"{col_str}_{len(seen)}"
            seen.add(col_str)
            cleaned.append(col_str)
        return cleaned

    df_clean.columns = clean_columns(df_clean.columns)

    # Connect and insert into Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()
    try:
        # Create table all columns as VARCHAR(16777216)
        col_defs = ", ".join([f'"{col}" VARCHAR(16777216)' for col in df_clean.columns])
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})'
        cs.execute(create_sql)

        # Insert data row-wise
        placeholders = ", ".join(["%s"] * len(df_clean.columns))
        columns = ", ".join([f'"{col}"' for col in df_clean.columns])
        insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'

        cs.executemany(insert_sql, df_clean.values.tolist())
    finally:
        cs.close()
        conn.close()
