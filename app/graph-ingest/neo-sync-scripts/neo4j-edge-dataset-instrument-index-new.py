import os
import logging
from pathlib import Path
from neo4j import GraphDatabase
from weaviate.util import generate_uuid5
import json

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

def create_dataset_platform(batch, data_dict, session):
    platform_id = data_dict["platform_id"]
    dataset_id = data_dict["dataset_id"]

    batch.append((dataset_id, {}, platform_id))

    return batch

batch = []
for file in os.listdir(collection_jsons_path):
    if file.endswith(".json"):
        file_path = os.path.join(collection_jsons_path, file)
        with open(file_path) as json_file:
            data = json.load(json_file)
            dataset_id = generate_uuid5(data["ShortName"])
            for platform in data["Platforms"]:
                for instrument in platform["Instruments"]:
                    platform_id = generate_uuid5(instrument["ShortName"])
                    data_dict = {"platform_id": platform_id, "dataset_id": dataset_id}
                    with driver.session() as session:
                        create_dataset_platform(batch=batch, data_dict=data_dict, session=session)

with driver.session() as session:
    session.run("""
        UNWIND $batch AS row
        MERGE (d:Dataset {globalId: row[0]})
        MERGE (p:Instrument {globalId: row[2]})
        MERGE (d)-[:HAS_KEYWORD]->(p)
        """, batch=batch)

# Merge relationships using the Neo4j Python driver
with driver.session() as session:
    for data_dict in batch:
        dataset_id = data_dict[0]
        platform_id = data_dict[2]
        session.run(
            """
            MATCH (d:Dataset {globalId: $dataset_id})
            MATCH (p:Platform {globalId: $platform_id})
            MERGE (d)-[:PLATFORM_OF_DATASET]->(p)
            """,
            dataset_id=dataset_id,
            platform_id=platform_id
        )