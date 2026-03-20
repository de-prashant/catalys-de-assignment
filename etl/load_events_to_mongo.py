import json
import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Load environment variables
load_dotenv()

# MongoDB connection string
mongo_uri = os.getenv("MONGODB_URI")

def load_json_to_mongodb_incremental(json_file_path, database_name="catalys", collection_name="events"):
    """Load JSON data incrementally to MongoDB collection (skip existing events)."""

    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]

        # Test connection
        client.admin.command('ping')
        logging.info("Connected to MongoDB")

        # Ensure indexes exist
        collection.create_index("event_id", unique=True)
        collection.create_index("user_id")
        collection.create_index("event_type")
        collection.create_index("timestamp")

        # Read JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        if not isinstance(data, list):
            raise ValueError("JSON must be an array of documents")

        if not data:
            logging.warning("No data found in file")
            return True

        try:
            result = collection.insert_many(data, ordered=False)
            inserted_count = len(result.inserted_ids)
            skipped_count = 0

        except BulkWriteError as e:
            inserted_count = len(e.details.get("writeResults", []))
            skipped_count = len(e.details.get("writeErrors", []))

        logging.info(f"Incremental load complete: {inserted_count} inserted, {skipped_count} skipped")

        return True

    except FileNotFoundError:
        logging.error(f"File not found: {json_file_path}")
        return False
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        return False
    except Exception as e:
        logging.error(f"Error: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    json_file_path = "data/events/events.json"

    success = load_json_to_mongodb_incremental(json_file_path)

    if success:
        print("Incremental load completed successfully!")
    else:
        print("Incremental load failed.")
