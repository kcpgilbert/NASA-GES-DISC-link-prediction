import os
from pathlib import Path
import json

from neo4j import GraphDatabase, basic_auth
from weaviate.util import generate_uuid5

# Get Neo4j driver
#driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "kendallg"))
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))

cwd = os.getcwd()
pd = Path(cwd).parents[0]

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


def add_keyword(tx, keyword_data: dict, keyword_id) -> str:
    keys = ["globalId", "name", "level"]
    name = keyword_data["name"].lower()
    level = keyword_data["level"]

    keyword_object = {
        "globalId": keyword_id,
        "name": name,
        "level": level,
    }

    tx.run(
        """
        MERGE (k:Keyword {globalId: $globalId})
        ON CREATE SET k.name = $name, k.level = $level
        """,
        **keyword_object
    )

    return keyword_id


aggregate_keywords = {}
for file in collection_jsons_list:
    with open(file) as json_file:
        data = json.load(json_file)
        for item in data["ScienceKeywords"]:
            for keyword in item:
                level = keyword
                name = item[keyword].lower()
                keyword_id = generate_uuid5(item[keyword])
                aggregate_keywords[keyword_id] = {
                    "globalId": keyword_id,
                    "name": name,
                    "level": level,
                }

keys = ["globalId", "name", "level"]
with driver.session() as session:
    for keyword in aggregate_keywords:
        keyword_id = add_keyword(
            session, keyword_data=aggregate_keywords[keyword], keyword_id=keyword
        )

# Counting the number of Keyword nodes
with driver.session() as session:
    result = session.run("MATCH (k:Keyword) RETURN COUNT(k) AS count")
    for record in result:
        count = record["count"]
    print("Number of Keyword nodes:", count)