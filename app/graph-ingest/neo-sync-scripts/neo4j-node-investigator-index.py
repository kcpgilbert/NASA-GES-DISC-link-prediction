import os
from pathlib import Path, PurePath
import logging
from weaviate.util import generate_uuid5
import requests
from tqdm import tqdm
import json


from py2neo import Graph, Node, Relationship
from py2neo.bulk import create_nodes, merge_nodes, create_relationships

# Get the NEO4J_AUTH from the environment variables
neo4j_auth = os.getenv("NEO4J_AUTH")

# NEO4J_AUTH is in the format "username/password", so we split it

graph = Graph("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

cwd = os.getcwd()
pd = Path(cwd).parents[0]


# Setting up logs
log_dir = os.path.join(cwd, "logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(
    os.path.join("logs", "neo4j_platform_index_logs.log")
)
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

platforms_dict = {}
for file in collection_jsons_list:
    with open(file) as json_file:
        data = json.load(json_file)
        for platform in data["ContactPersons"]:
            if "Investigator" in platform["Roles"]:
                globalID = generate_uuid5(platform["LastName"])
                platforms_dict[globalID] = platform
                
                



def add_platform(batch: list, platform_data: dict, platform_id) -> str:
    keys = ["globalId", "firstName", "lastName"]
    data = []

    """
        Add Platform nodes to Neo4j
    """

    #     try:
    #         DOI = dataset_data["DOI"]["DOI"]
    #     except:
    #         DOI = "0000"

    #     try:
    #         DAAC = dataset_data["DAAC"]
    #     except:
    #         DAAC = "NA"

    # assert(dataset_data["ShortName"]==dataset_data['CollectionCitations'][0]['SeriesName'])

    #     platform_object = {
    #         "globalID": platform_id,
    #         "shortName": platform_data["ShortName"],
    #         "longName": platform_data["LongName"],
    #     }

    platform_globalId = platform_id
    try:
        platform_shortName = platform_data["FirstName"]
    except KeyError as error: 
        platform_shortName = None
        print(f"Platform {platform_shortName} does not have {error}!")
    try:
        platform_longName = platform_data["LastName"]
    except KeyError as error:
        platform_longName = None
        print(f"Platform {platform_shortName} does not have {error}!")

    current_batch = [
        platform_globalId,
        platform_shortName,
        platform_longName,

    ]
    batch.append(current_batch)

    return platform_id


keys = ["globalId", "firstName", "lastName"]
batch = []
for platf in platforms_dict:
    platform_dict = platforms_dict[platf]
    logger.debug(
        "func: main. Iterating over Platforms. Platform is: {}.".format(
            platform_dict["LastName"]
        )
    )
    platform_id = add_platform(
        batch=batch, platform_data=platform_dict, platform_id=platf
    )

merge_nodes(graph.auto(), batch, merge_key=("Investigator", "globalId"), keys=keys)
print(f"Total investigators indexed: {graph.nodes.match('Investigator').count()}")
