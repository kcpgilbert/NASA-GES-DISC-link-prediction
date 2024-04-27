import os
from pathlib import Path
import logging
import json

from neo4j import GraphDatabase, basic_auth
from weaviate.util import generate_uuid5

# Get Neo4j driver
#driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "kendallg"))
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

cwd = os.getcwd()
pd = Path(cwd).parents[0]

# Setting up logs
log_dir = os.path.join(cwd, "logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join("logs", "neo4j_dataset_index_logs.log"))
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

for file in collection_jsons_list:
    with open(file) as json_file:
        data = json.load(json_file)
        globalID = generate_uuid5(data["ShortName"])
        collections_dict[globalID] = data

def extract_daac(data):
    """Extract DAAC name from the 'DataCenters' field in the dataset's metadata."""
    if "DataCenters" in data:
        for center in data["DataCenters"]:
            if "Roles" in center and "ARCHIVER" in center["Roles"]:
                return center.get("ShortName", "N/A")
    return "N/A"

def add_dataset(tx, dataset_data, dataset_id):
    # Extract properties for the Dataset node
    short_name = dataset_data.get("ShortName", "N/A")
    long_name = dataset_data.get("EntryTitle", "N/A")
    doi = dataset_data.get("DOI", {}).get("DOI", "N/A")
    daac = extract_daac(dataset_data)  # Extract DAAC name using the new function
    abstract = dataset_data.get("Abstract", "N/A").replace("\n", "")

    # Cypher query to create a Dataset node
    query = (
        "CREATE (d:Dataset { "
        "globalId: $globalId, doi: $doi, shortName: $shortName, "
        "longName: $longName, daac: $daac, abstract: $abstract "
        "})"
    )

    # Run the query
    tx.run(
        query,
        globalId=dataset_id,
        doi=doi,
        shortName=short_name,
        longName=long_name,
        daac=daac,
        abstract=abstract,
    )

    return dataset_id


with driver.session() as session:
    keys = ["globalId", "doi", "shortName", "longName", "daac", "abstract"]
    for doc in collections_dict:
        dataset_dict = collections_dict[doc]
        logger.debug(
            "func: main. Iterating over collections. Collection is: {}.".format(dataset_dict["DOI"])
        )
        dataset_id = add_dataset(session, dataset_data=dataset_dict, dataset_id=doc)

# Counting the number of Dataset nodes
with driver.session() as session:
    result = session.run("MATCH (d:Dataset) RETURN COUNT(d) AS count")
    for record in result:
        count = record["count"]
    print("Number of Dataset nodes:", count)
