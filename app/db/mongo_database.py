import os
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv(verbose=True)

client = MongoClient(os.environ["MONGO_DB_URL"])
mongo_db = client["global_terrorism"]
events_collection = mongo_db["terror_events"]

