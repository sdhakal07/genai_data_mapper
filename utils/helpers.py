import json
import os
import pandas as pd

def read_csv(file):
    return pd.read_csv(file)

def preprocess_columns(columns):
    return [col.strip().lower().replace(" ", "_") for col in columns]

def save_mapping_to_db(mapping: dict, path: str = "mappings_db.json"):
    if os.path.exists(path):
        with open(path, "r") as f:
            existing = json.load(f)
    else:
        existing = []

    existing.append(mapping)
    with open(path, "w") as f:
        json.dump(existing, f, indent=2)