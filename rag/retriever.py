import os
from langchain.vectorstores import Chroma
from langchain.embeddings import OpenAIEmbeddings
from config import OPENAI_API_KEY

embedding_model = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)

CHROMA_DB_DIR = "chroma_db"
_db = None

def get_vector_store():
    global _db
    if _db is None:
        if os.path.exists(CHROMA_DB_DIR):
            _db = Chroma(
                persist_directory=CHROMA_DB_DIR,
                embedding_function=embedding_model
            )
    return _db

def retrieve_similar_mappings(query):
    db = get_vector_store()
    if db is None:
        return []
    return db.similarity_search(query, k=2)

def add_new_mapping(new_mapping_dict):
    from langchain.schema import Document
    global _db
    docs = []
    for client_col, standard_col in new_mapping_dict.items():
        content = f"client_col: {client_col} --> standard_col: {standard_col}"
        docs.append(Document(page_content=content))
    if _db is None:
        _db = Chroma.from_documents(
            documents=docs,
            embedding=embedding_model,
            persist_directory=CHROMA_DB_DIR
        )
    else:
        _db.add_documents(docs)
    _db.persist()  # Save changes
