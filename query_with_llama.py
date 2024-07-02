from llama_index.core import (
    VectorStoreIndex,
    SimpleDirectoryReader,
    StorageContext,
    load_index_from_storage,
)
from file_utils import get_downloads_folder
import os
from dotenv import load_dotenv
from llama_index.llms.anthropic import Anthropic
from llama_index.core import Settings


def query_data(query):
    load_dotenv()
    # openai.api_key = os.environ.get("OPENAI_API_KEY")
    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")

    downloads_folder = get_downloads_folder()
    data_dir = os.path.join(downloads_folder, "todays_jobs")

    os.environ["ANTHROPIC_API_KEY"] = anthropic_api_key
    Settings.llm = Anthropic(model="claude-3-haiku-20240307")
    Settings.tokenizer = Anthropic().tokenizer

    # check if storage already exists
    PERSIST_DIR = "./storage"
    if not os.path.exists(PERSIST_DIR):
        # load the documents and create the index
        documents = SimpleDirectoryReader(data_dir).load_data()
        index = VectorStoreIndex.from_documents(documents)
        # store it for later
        index.storage_context.persist(persist_dir=PERSIST_DIR)
    else:
        # load the existing index
        storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
        index = load_index_from_storage(storage_context)

    query_engine = index.as_query_engine(similarity_top_k=5)  # default is top 2
    response = query_engine.query(query)
    print(response)
