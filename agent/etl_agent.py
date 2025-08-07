from langchain.chains import LLMChain
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from config import OPENAI_API_KEY, STANDARD_FIELDS, OPENAI_MODEL
from rag.retriever import retrieve_similar_mappings

llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY, model_name=OPENAI_MODEL)

standard_fields_str = ", ".join(STANDARD_FIELDS)

prompt_template = PromptTemplate(
    input_variables=["sample_cols", "examples", "standard_fields"],
    template="""
        You are a smart data field mapper. Your job is to map client-provided column names to the closest matching standard fields.
        
        Standard fields:
        {standard_fields}
        
        Client sample columns:
        {sample_cols}
        
        Here are some examples of correct mappings:
        {examples}
        
        Now, generate the mapping between client columns and standard fields based on the above information.
        
        Return the output in **pure JSON format only**, like:
        {{"client_col_1": "standard_field_1", "client_col_2": "standard_field_2"}}
        
        Do not include any explanation or commentary. Only return the JSON.
        """.strip().replace("{standard_fields}", standard_fields_str)
)

chain = LLMChain(llm=llm, prompt=prompt_template)

def map_fields(sample_columns):
    examples = "\n".join([doc.page_content for doc in retrieve_similar_mappings(", ".join(sample_columns))])
    print("Examples",examples)
    return chain.run(sample_cols=", ".join(sample_columns), examples=examples)
