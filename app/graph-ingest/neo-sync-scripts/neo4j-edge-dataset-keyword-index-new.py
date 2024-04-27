import os
from pathlib import Path
import logging
from weaviate.util import generate_uuid5
import json
from neo4j import GraphDatabase

# Get Neo4j driver
#driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "kendallg"))
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

cwd = os.getcwd()
pd = Path(cwd).parents[0]

# Setting up logs
log_dir = os.path.join(cwd, "logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join("logs", "neo4j_dataset_keyword_index_logs.log"))
file_handler.setLevel(logging.WARNING)
logger.addHandler(file_handler)
log_formatter = logging.Formatter("%(asctime)s|%(name)s|%(message)s")
file_handler.setFormatter(log_formatter)

# Path to data folder
data_path = os.path.join(pd, "data")

# Path to Dataset/Collection jsons
collection_jsons_path = Path(os.path.join(data_path, "PROD_20230409"))

def create_dataset_variable(batch, data_dict):
    keyword_id = data_dict["keyword_id"]
    dataset_id = data_dict["dataset_id"]
    batch.append({"dataset_id": dataset_id, "keyword_id": keyword_id})
    return batch

batch = []
for file in collection_jsons_path.iterdir():
    if file.suffix == ".json":
        with open(file) as json_file:
            data = json.load(json_file)
            dataset_id = generate_uuid5(data["ShortName"])
            for item in data["ScienceKeywords"]:
                for keyword in item:
                    level = keyword
                    name = item[keyword].lower()
                    if level != 'Category':
                        keyword_id = generate_uuid5(item[keyword])
                        data_dict = {"keyword_id": keyword_id, "dataset_id": dataset_id}
                        create_dataset_variable(batch, data_dict)

# Define function to create relationships
def create_relationships(tx, batch, rel_type):
    for data_dict in batch:
        tx.run(
            f"""
            MATCH (d:Dataset {{globalId: $dataset_id}})
            MATCH (k:Keyword {{globalId: $keyword_id}})
            MERGE (d)-[:{rel_type}]->(k)
            """,
            dataset_id=data_dict["dataset_id"],
            keyword_id=data_dict["keyword_id"]
        )

# Process data and add relationships to Neo4j
with driver.session() as session:
    session.write_transaction(create_relationships, batch, "HAS_KEYWORD")
    session.write_transaction(create_relationships, batch, "OF_DATASET")