import os
from pathlib import Path, PurePath
import logging
from weaviate.util import generate_uuid5
import requests
from tqdm import tqdm
import json
from neo4j import GraphDatabase

# Get Neo4j driver
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

cwd = os.getcwd()
pd = Path(cwd).parents[0]

# Setting up logs
log_dir = os.path.join(cwd, "logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join("logs", "neo4j_dataset_platform_index_logs.log"))
file_handler.setLevel(logging.WARNING)
logger.addHandler(file_handler)
log_formatter = logging.Formatter("%(asctime)s|%(name)s|%(message)s")
file_handler.setFormatter(log_formatter)

# Path to data folder
data_path = os.path.join(pd, "data")

# Path to Dataset/Collection jsons
collection_jsons_path = os.path.join(data_path, "PROD_20230409")

collections_dict = {}
collection_jsons_list = [
    os.path.join(collection_jsons_path, file)
    for file in os.listdir(collection_jsons_path)
    if file.endswith(".json")
]

def create_dataset_platform(tx, data_dict):
    """
    Create relationship between dataset and platform
    """
    platform_id = data_dict["platform_id"]
    dataset_id = data_dict["dataset_id"]

    tx.run(
        """
        MATCH (d:Dataset {globalId: $dataset_id})
        MATCH (p:Investigator {globalId: $platform_id})
        MERGE (d)-[:HAS_KEYWORD]->(p)
        """,
        dataset_id=dataset_id,
        platform_id=platform_id
    )

with driver.session() as session:
    for file in collection_jsons_list:
        with open(file) as json_file:
            data = json.load(json_file)
            dataset_id = generate_uuid5(data["ShortName"])
            for platform in data["ContactPersons"]:
                if "Investigator" in platform["Roles"]:
                    platform_id = generate_uuid5(platform["LastName"])
                    data_dict = {"platform_id": platform_id, "dataset_id": dataset_id}
                    session.write_transaction(create_dataset_platform, data_dict)