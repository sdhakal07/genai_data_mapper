# rag/retriever.py
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from langchain.schema import Document
from config import OPENAI_API_KEY

embedding_model = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)

def get_vector_store():
    docs = [
        Document(page_content="client_col: cust_id --> standard_col: customer_id"),
        Document(page_content="client_col: customer number --> standard_col: customer_id"),
        Document(page_content="client_col: customer id --> standard_col: customer_id"),
        Document(page_content="client_col: email_address --> standard_col: email"),
        Document(page_content="client_col: date_of_order --> standard_col: order_date"),
    ]
    return FAISS.from_documents(docs, embedding_model)

def retrieve_similar_mappings(query):
    db = get_vector_store()
    return db.similarity_search(query, k=2)
