import pandas as pd

def read_csv(file):
    return pd.read_csv(file)

def preprocess_columns(columns):
    return [col.lower().strip().replace(" ", "_") for col in columns]
