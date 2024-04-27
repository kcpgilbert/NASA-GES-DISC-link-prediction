from neo4j import GraphDatabase
import os

uri = "bolt://neo4j_instance_test:7687"
user = "neo4j"
password = "kendallg"
driver = GraphDatabase.driver(uri, auth=(user, password))

def drop_pipeline(driver):
    with driver.session() as session:
        # Drop pipeline if it exists so we can rerun; comment out if needed
        drop_pipeline_query = """
        CALL gds.beta.pipeline.drop('pipe')
        """
        session.run(drop_pipeline_query)


def create_pipeline(driver):
    with driver.session() as session:
        # Create pipeline
        create_pipeline_query = """
        CALL gds.beta.pipeline.linkPrediction.create('pipe')
        """
        session.run(create_pipeline_query)


# Execute functions
drop_pipeline(driver)
create_pipeline(driver)