from neo4j import GraphDatabase
import os

# Get Neo4j driver
driver = GraphDatabase.driver("bolt://neo4j_instance_test:7687", auth=("neo4j", "kendallg"))


def run_projection(driver):
    # Define the relationship projection details
    relationship_proj = """
    {
        relType: {
          type: '*',
          orientation: 'UNDIRECTED',
          properties: {}
        }
    }
    """
    empty_brances = "{}"

    # Project the graph in-memory
    projection_command = f"""
    CALL gds.graph.project('in-memory-graph-1681323347597', "*", {relationship_proj}, {empty_brances});
    """
    with driver.session() as session:
        session.run(projection_command)


def run_fastrp(driver):
    # Configuration for FastRP
    config = """
    {
        relationshipWeightProperty: null,
        embeddingDimension: 10,
        normalizationStrength: 0.5,
        writeProperty: 'fastrp'
    }
    """
    # Execute FastRP
    generation_fastrp = f"""
    CALL gds.fastRP.write("in-memory-graph-1681323347597", {config});
    """
    with driver.session() as session:
        session.run(generation_fastrp)


def drop_graph(driver):
    # Drop the in-memory graph
    drop_graph_command = """
    CALL gds.graph.drop('in-memory-graph-1681323347597');
    """
    with driver.session() as session:
        session.run(drop_graph_command)


# Execute functions
run_projection(driver)
run_fastrp(driver)
drop_graph(driver)

# Close the driver connection when done
#driver.close()
