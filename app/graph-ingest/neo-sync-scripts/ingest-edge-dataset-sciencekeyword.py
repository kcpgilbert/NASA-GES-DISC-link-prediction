import os
import json
from pathlib import Path
#import uuid
from weaviate.util import generate_uuid5
from neo4j import GraphDatabase
from tqdm import tqdm
import logging

#Get paths
cwd = os.getcwd()
pd = Path(cwd).parents[0]

# Path to data folder
data_path = os.path.join(pd, "data")

# Path to Dataset/Collection jsons
collection_jsons_path = Path(os.path.join(data_path, "PROD_20230409"))

# Setting up logs
log_dir = os.path.join(cwd, "logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join("logs", "neo4j_edge_dataset_keyword_index_logs.log"))
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)
log_formatter = logging.Formatter("%(asctime)s|%(name)s|%(message)s")
file_handler.setFormatter(log_formatter)

# Get Neo4j driver
#driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "kendallg"))
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

def generate_uuid_from_doi(doi):
    return generate_uuid5(doi)

def generate_uuid_from_string(input_string):
    return generate_uuid5(input_string)

def create_relationship(tx, dataset_uuid, keyword_uuid):
    query = """
    MATCH (d:Dataset {globalId: $dataset_uuid}), (k:ScienceKeyword {globalId: $keyword_uuid})
    MERGE (d)-[:HAS_KEYWORD]->(k)
    RETURN d, k
    """
    result = tx.run(query, dataset_uuid=dataset_uuid, keyword_uuid=keyword_uuid)
    nodes = result.data()
    if not nodes:
        logger.info(f"No relationship created: Dataset UUID={dataset_uuid}, Keyword UUID={keyword_uuid}")
        return 0
    else:
        logger.info(f"Relationship created between: {nodes[0]['d']['shortName']} and {nodes[0]['k']['name']}")
        return 1

def find_json_files(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                yield os.path.join(root, file)

def process_json_files(directory):
    relationships = []
    json_files = list(find_json_files(directory))
    for json_file in tqdm(json_files):
        with open(json_file, "r") as file:
            data = json.load(file)
            doi = data.get("DOI", {}).get("DOI", "")
            if doi:
                dataset_uuid = generate_uuid_from_doi(doi)
                if "ScienceKeywords" in data:
                    for item in data["ScienceKeywords"]:
                        for level in ["Topic", "Term", "Variable_Level_1", "Variable_Level_2", "Variable_Level_3", "Detailed_Variable"]:
                            keyword = item.get(level)
                            if keyword:
                                keyword_uuid = generate_uuid_from_string(keyword)
                                relationships.append((dataset_uuid, keyword_uuid))

    created, failed = 0, 0
    with driver.session() as session:
        batch_size = 100
        for i in tqdm(range(0, len(relationships), batch_size)):
            batch = relationships[i:i+batch_size]
            results = [
                session.execute_write(create_relationship, ds_uuid, kw_uuid)
                for ds_uuid, kw_uuid in batch
            ]
            created += sum(results)
            failed += len(results) - sum(results)

    logger.info(f"Total relationships processed: {len(relationships)}")
    logger.info(f"Relationships created: {created}")
    logger.info(f"Relationships failed: {failed}")

if __name__ == "__main__":
    directory = collection_jsons_path
    process_json_files(directory)
    logger.info("Dataset and ScienceKeyword relationship processing completed.")