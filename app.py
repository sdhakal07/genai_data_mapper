import os
os.environ["ALLOW_DANGEROUS_DESERIALIZATION"] = "true"

import streamlit as st
import json
import os

from utils.helpers import read_csv, preprocess_columns, save_mapping_to_db
from agent.etl_agent import map_fields
from snowflake_loader import load_to_snowflake
from config import STANDARD_FIELDS
from rag.retriever import add_new_mapping

st.set_page_config(page_title="Retail Data Mapper", layout="wide")
st.title("AI-powered Data Mapper")

uploaded_file = st.file_uploader("Upload a client CSV file", type=["csv"])
if uploaded_file:
    file_name = os.path.splitext(uploaded_file.name)[0].lower().replace(" ", "_")
    df = read_csv(uploaded_file)
    st.write("Preview of uploaded file:")
    st.dataframe(df.head())

    # Normalize columns
    df.columns = preprocess_columns(df.columns.tolist())
    sample_columns = df.columns.tolist()

    if st.button("⚙️ Map Columns using AI"):
        with st.spinner("Mapping fields with AI..."):
            try:
                mapping_json = map_fields(sample_columns)
                mapping = json.loads(mapping_json)
                st.session_state["ai_mapping"] = mapping
                st.success("Mapping complete. Review below.")
            except Exception as e:
                st.error(f"Error parsing mapping: {e}")

if "ai_mapping" in st.session_state:
    st.subheader("🧾 Review & Adjust Mappings")

    mapping = st.session_state["ai_mapping"]
    user_mapping = {}

    for client_col in sample_columns:
        suggested = mapping.get(client_col, "Ignore")
        if suggested not in STANDARD_FIELDS:
            suggested = "Ignore"

        user_mapping[client_col] = st.selectbox(
            f"Map `{client_col}` to:",
            options=["Ignore"] + STANDARD_FIELDS,
            index=(STANDARD_FIELDS.index(suggested) + 1) if suggested in STANDARD_FIELDS else 0,
        )

    # Final mapping: drop ignored
    final_mapping = {
        k: v for k, v in user_mapping.items() if v != "Ignore"
    }

    # Deduplicate: no same standard field twice
    seen = set()
    deduped_mapping = {}
    for k, v in final_mapping.items():
        if v not in seen:
            deduped_mapping[k] = v
            seen.add(v)

    if deduped_mapping:
        st.markdown("###Final Mapped Table")
        mapped_df = df.rename(columns=deduped_mapping)
        matched_df = mapped_df[list(deduped_mapping.values())]
        st.dataframe(matched_df.head())

        if st.button("Load Both Raw and Mapped Data to Snowflake"):
            with st.spinner("Uploading to Snowflake..."):
                try:
                    load_to_snowflake(df.assign(file_name=uploaded_file.name), table_name=f"raw_{file_name}")
                    load_to_snowflake(matched_df, table_name=f"mapped_{file_name}")
                    st.success("Raw and mapped data loaded to Snowflake!")

                    # Save mappings for future RAG use (JSON + Chroma DB)
                    save_mapping_to_db(deduped_mapping)
                    add_new_mapping(deduped_mapping)

                except Exception as err:
                    st.error(f"Snowflake error: {err}")
    else:
        st.warning("No valid mappings selected. Please adjust above.")
