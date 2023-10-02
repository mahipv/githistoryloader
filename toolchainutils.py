from abc import abstractmethod
import datetime
import openai
from typing import List, Tuple
from llama_index.vector_stores import TimescaleVectorStore
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv, find_dotenv
from langchain.vectorstores import TimescaleVector
from pandas import DataFrame
from timescale_vector import client
from llama_index.schema import TextNode
from llama_index.embeddings import OpenAIEmbedding

MAX_STR_LENGTH = 2048
EMBEDDING_DIMENSIONS = 1536
class ToolChain:

    def __init__(self, table_name, toolchain) -> None:
        self._table_name = table_name
        self._tool_chain = toolchain
        self._time_delta = timedelta(days=7)
        openai.api_key  = os.environ['OPENAI_API_KEY']
        _ = load_dotenv(find_dotenv())

    def get_table_name(self)->str:
        return self._table_name

    def get_tool_chain(self)->str:
        return self._tool_chain
    
    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def process_frame(self, df):
        pass

    @abstractmethod
    def insert_rows(self, rows):
        pass

    @abstractmethod
    def create_index(self):
        pass

    # Helper function: get embeddings for a text
    def get_embeddings(self, text):
        response = openai.Embedding.create(
            model="text-embedding-ada-002",
            input = text.replace("\n"," ")
        )
        embedding = response['data'][0]['embedding']
        return embedding

    # helper function to take in a date string in the past and return a uuid v1
    def create_uuid(self, date_string: str):
        datetime_obj = datetime.fromisoformat(date_string)
        uuid = client.uuid_from_time(datetime_obj)
        return str(uuid)

class LangChain(ToolChain):
    def __init__(self, table_name) -> None:
        super().__init__(table_name, "langchain")
        self._ts_vector_store = TimescaleVector(
            service_url=os.environ["TIMESCALE_SERVICE_URL"],
            embedding=EMBEDDING_DIMENSIONS,
            collection_name=self._table_name,
            time_partition_interval=self._time_delta
        )

    def create_tables(self):
        self._ts_vector_store.sync_client.drop_table()
        self._ts_vector_store.sync_client.create_tables()

    def process_row(self, row) -> any:
        max_retries = 2  # Number of times to retry
        text = row['Author'] + " " + row['Date'] + " " + row['Commit Hash'] + " " + row['Subject'] + " " + row['Body']
        record = None
        for _ in range(max_retries):
            try:
                embedding = self.get_embeddings(text)    
                # If the code block succeeds, break out of the loop
                uuid = self.create_uuid(row['Date'])
                # Create metadata
                metadata = {
                    "author": row['Author'],
                    "date": row['Date'],
                    "commit": row['Commit Hash'],
                }
                record = (uuid, metadata, text, embedding)
                break
            except Exception as e:
                print(f"An exception occurred: {e} Retrying")
                if len(text) > MAX_STR_LENGTH:
                    text = text[:MAX_STR_LENGTH]
        else:
            # This block is executed if the maximum number of retries is reached
            print(f"Unable to add the record {text}")
        return record

    def process_frame(self, df):
        records = []
        for _, row in df.iterrows():
            record = self.process_row(row)
            if record:
                records.append(record)
        print(f"Inserting {len(records)} records")
        self._ts_vector_store.sync_client.upsert(records)    

    def create_index(self):
        self._ts_vector_store.create_index()

class LlamaIndex(ToolChain):
    def __init__(self, table_name) -> None:
        super().__init__(table_name, "llamaindex")
        self._ts_vector_store = TimescaleVectorStore.from_params(
            service_url=os.environ["TIMESCALE_SERVICE_URL"],
            table_name=self._table_name,
            time_partition_interval=self._time_delta,
        )

    def create_tables(self):
        self._ts_vector_store._sync_client.drop_table()
        self._ts_vector_store._sync_client.create_tables()

    # Create a Node object from a single row of data
    def create_node(self, row):
        record = row.to_dict()
        record_content = (
            str(record["Date"])
            + " "
            + record['Author']
            + " "
            + str(record["Subject"])
            + " "
            + str(record["Body"])
        )
        # Can change to TextNode as needed
        node = TextNode(
            id_=self.create_uuid(record["Date"]),
            text=record_content,
            metadata={
                "commit_hash": record["Commit Hash"],
                "author": record['Author'],
                "date": record["Date"],
            },
        )
        return node    
    
    def process_frame(self, df):
        nodes = [self.create_node(row) for _, row in df.iterrows()]
        embedding_model = OpenAIEmbedding()
        embedding_model.api_key = os.environ['OPENAI_API_KEY']
        for node in nodes:
            node_embedding = embedding_model.get_text_embedding(node.get_content(metadata_mode="all"))
        node.embedding = node_embedding
        _ = self._ts_vector_store.add(nodes)

    def create_index(self):
        self._ts_vector_store.create_index()
    
