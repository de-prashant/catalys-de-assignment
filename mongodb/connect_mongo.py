import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection string
# mongo_uri = os.getenv("MONGODB_URI", "mongodb+srv://prashantpdwbi_db_user:2mGMJlyqfAOTacNc@freecluster1.oshjz36.mongodb.net/")
mongo_uri = os.getenv("MONGODB_URI")

def get_mongo_client():
    """Establish a MongoDB connection."""
    try:
        client = MongoClient(mongo_uri)
        # Test the connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None

if __name__ == "__main__":
    client = get_mongo_client()
    if client:
        # List databases
        print("Databases:", client.list_database_names())
        client.close()