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
file_handler = logging.FileHandler(os.path.join("logs", "neo4j_platform_index_logs.log"))
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
        for platform in data["Platforms"]:
            for instrument in platform["Instruments"]:
                globalID = generate_uuid5(instrument["ShortName"])
                platforms_dict[globalID] = instrument


def add_platform(tx, platform_data: dict, platform_id) -> str:
    """
    Add Platform nodes to Neo4j
    """
    platform_globalId = platform_id
    platform_shortName = platform_data["ShortName"]
    platform_longName = platform_data.get("LongName")  # Use get to handle missing keys
    current_batch = [
        platform_globalId,
        platform_shortName,
        platform_longName,
    ]
    tx.run(
        """
        MERGE (p:Instrument {globalId: $globalId})
        ON CREATE SET p.shortName = $shortName, p.longName = $longName
        """,
        globalId=platform_globalId,
        shortName=platform_shortName,
        longName=platform_longName,
    )
    return platform_id


# Process data and add to Neo4j
with driver.session() as session:
    for platf_id, platform_data in platforms_dict.items():
        logger.debug("func: main. Iterating over Platforms. Platform is: {}.".format(platform_data["ShortName"]))
        add_platform(session, platform_data=platform_data, platform_id=platf_id)

# Count the total number of indexed platforms
with driver.session() as session:
    result = session.run("MATCH (p:Instrument) RETURN COUNT(p) AS count")
    count = result.single()["count"]
    print(f"Total Instruments indexed: {count}")