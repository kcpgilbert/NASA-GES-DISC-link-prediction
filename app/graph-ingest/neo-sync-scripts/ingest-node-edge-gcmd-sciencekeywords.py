import csv
#import uuid
from weaviate.util import generate_uuid5
from pathlib import Path
import json
import os
from neo4j import GraphDatabase
from tqdm import tqdm
import logging

#Get paths
cwd = os.getcwd()
pd = Path(cwd).parents[0]

# Path to data folder
data_path = os.path.join(pd, "data")

# Path to Dataset/Collection jsons
collection_jsons_path = os.path.join(data_path, "PROD_20230409")

# Path to gcmd keywords
gcmd_path = os.path.join(data_path, "gcmd")
keywords_path = os.path.join(gcmd_path, "sciencekeywords.csv")


# Setting up logs
log_dir = os.path.join(cwd, "logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join("logs", "neo4j_node_dataset_keyword_index_logs.log"))
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)
log_formatter = logging.Formatter("%(asctime)s|%(name)s|%(message)s")
file_handler.setFormatter(log_formatter)

# Get Neo4j driver
#driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "kendallg"))
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

def set_uniqueness_constraint():
    query = "CREATE CONSTRAINT ON (sk:ScienceKeyword) ASSERT sk.globalId IS UNIQUE"
    with driver.session() as session:
        try:
            session.run(query)
            logger.info("Uniqueness constraint on ScienceKeyword.globalId set successfully.")
        except Exception as e:
            logger.error(f"Failed to create uniqueness constraint: {str(e)}")

def generate_uuid_from_string(input_string):
    return generate_uuid5(input_string)

def create_node(tx, name, globalId):
    query = "MERGE (n:ScienceKeyword {name: $name, globalId: $globalId})"
    tx.run(query, name=name, globalId=globalId)

def create_relationship(tx, globalId_from, globalId_to):
    query = "MATCH (a:ScienceKeyword {globalId: $globalId_from}), (b:ScienceKeyword {globalId: $globalId_to}) MERGE (a)-[:SUBCATEGORY_OF]->(b)"
    tx.run(query, globalId_from=globalId_from, globalId_to=globalId_to)

def process_csv(file_path, batch_size=100):
    with open(file_path, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        operations = []
        for row in tqdm(reader, desc="Processing CSV Rows", unit="rows"):
            prev_globalId = None
            for label in ["Topic", "Term", "Variable_Level_1", "Variable_Level_2", "Variable_Level_3", "Detailed_Variable"]:
                value = row.get(label, "").strip()
                if value:
                    current_globalId = generate_uuid_from_string(value)
                    operations.append(("node", value, current_globalId))
                    if prev_globalId:
                        operations.append(("rel", prev_globalId, current_globalId))
                    prev_globalId = current_globalId
            if len(operations) >= batch_size:
                execute_batch(operations)
                operations.clear()
        if operations:
            execute_batch(operations)

def execute_batch(operations):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for op in tqdm(operations, desc="Executing Batch", unit="ops", leave=False):
                if op[0] == "node":
                    create_node(tx, op[1], op[2])
                elif op[0] == "rel":
                    create_relationship(tx, op[1], op[2])
            tx.commit()

if __name__ == "__main__":
    set_uniqueness_constraint()
    file_path = keywords_path
    process_csv(file_path)
    logger.info("Nodes and relationships have been created in the Neo4j database.")