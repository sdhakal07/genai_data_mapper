def read_csv(file):
    import pandas as pd
    return pd.read_csv(file)

def preprocess_columns(columns):
    return [col.lower().replace(" ", "_") for col in columns]