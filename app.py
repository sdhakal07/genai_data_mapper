import streamlit as st
import pandas as pd
import os
import json

from utils.helpers import read_csv, preprocess_columns
from agent.etl_agent import map_fields
from snowflake_loader import load_raw_to_snowflake

st.title("AI-powered Field Mapper")

uploaded_file = st.file_uploader("Upload a client CSV file", type=["csv"])
if uploaded_file:
    file_name = os.path.splitext(uploaded_file.name)[0].lower().replace(" ", "_")
    df = read_csv(uploaded_file)
    st.write("Preview of uploaded file:", df.head())

    # Normalize df.columns for reliable matching
    df.columns = preprocess_columns(df.columns.tolist())
    sample_columns = df.columns.tolist()

    if st.button("Map Columns using AI"):
        with st.spinner("Mapping fields..."):
            mapping_json = map_fields(sample_columns)
            st.session_state["mapping_json"] = mapping_json
            st.json(mapping_json)

    if "mapping_json" in st.session_state:
        try:
            mapping = json.loads(st.session_state["mapping_json"])
            print(mapping)
            print(df.columns)
            # Filter mapping keys to only columns present in df
            valid_mapping = {k: v for k, v in mapping.items() if v in df.columns}
            print(valid_mapping)
            if not valid_mapping:
                st.warning("No valid mapped columns found in the uploaded file.")
            else:
                mapped_df = df.rename(columns=valid_mapping)
                matched_cols = list(valid_mapping.values())
                st.write("Valid Mapping:", matched_cols)
                matched_df = mapped_df[matched_cols]
                matched_df = matched_df.loc[:, matched_df.columns.notnull()]
                print(matched_df)
                st.write("Mapped Data:", matched_df.head())
                if st.button("Load Both Tables to Snowflake"):
                    with st.spinner("Uploading both tables to Snowflake..."):
                        try:
                            load_raw_to_snowflake(df.assign(file_name=uploaded_file.name), table_name=f"raw_{file_name}")
                            load_raw_to_snowflake(matched_df, table_name=f"mapped_{file_name}")
                            st.success(" Raw and mapped data loaded to Snowflake successfully!")
                        except Exception as err:
                            st.error(f" Snowflake error: {err}")
        except Exception as e:
            st.error(f" Error parsing mapping: {e}")
