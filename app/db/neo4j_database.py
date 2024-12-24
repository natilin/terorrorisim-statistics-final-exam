from dotenv import load_dotenv
from neo4j import GraphDatabase
import os
load_dotenv(verbose=True)
driver = GraphDatabase.driver(
    os.environ.get("NEO4J_URI"),
    auth=( os.environ.get("NEO4J_USER"),os.environ.get("NEO4J_PASSWORD"))
)
