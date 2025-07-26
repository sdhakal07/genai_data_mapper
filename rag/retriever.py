import os
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from config import OPENAI_API_KEY

embedding_model = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)

FAISS_INDEX_PATH = "faiss_index"
_db = None

def get_vector_store():
    global _db
    if _db is None:
        if os.path.exists(FAISS_INDEX_PATH):
            _db = FAISS.load_local(
                FAISS_INDEX_PATH,
                embedding_model,
                allow_dangerous_deserialization=True
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
        _db = FAISS.from_documents(docs, embedding_model)
    else:
        _db.add_documents(docs)
    _db.save_local(FAISS_INDEX_PATH)
