from langchain.chains import LLMChain
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from config import OPENAI_API_KEY, STANDARD_FIELDS
from rag.retriever import retrieve_similar_mappings

OPENAI_MODEL= "gpt-4.1-nano"

llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY, model_name=OPENAI_MODEL)

prompt_template = PromptTemplate(
    input_variables=["sample_cols", "examples"],
    template="""
        You are a smart data field mapper.
        Standard fields: {standard_fields}
        Client sample columns: {sample_cols}
        Use past examples:
        {examples}
        
        Map client columns to standard fields and return as JSON.
""".strip().replace("{standard_fields}", ", ".join(STANDARD_FIELDS))
)

chain = LLMChain(llm=llm, prompt=prompt_template)

def map_fields(sample_columns):
    examples = "\n".join([doc.page_content for doc in retrieve_similar_mappings(", ".join(sample_columns))])
    return chain.run(sample_cols=", ".join(sample_columns), examples=examples)
