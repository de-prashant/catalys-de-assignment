import os
import json
import logging
from pymongo import MongoClient
from dotenv import load_dotenv
import snowflake.connector

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Load env
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": "CATALYS",
    "schema": "RAW"
}

def load_raw_json(batch_size=1000):
    try:
        # MongoDB connection
        mongo_client = MongoClient(MONGO_URI)
        collection = mongo_client["catalys"]["events"]
        logging.info("Connected to MongoDB")

        # Snowflake connection
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        logging.info("Connected to Snowflake")

        # Fetch documents (no transformation at all)
        mongo_cursor = collection.find({}, {"_id": 0})

        batch = []
        total = 0

        for doc in mongo_cursor:
            # ONLY convert dict → string (no transformation)
            batch.append((json.dumps(doc),))

            if len(batch) >= batch_size:
                insert_batch(cursor, batch)
                total += len(batch)
                batch.clear()

        # Remaining batch
        if batch:
            insert_batch(cursor, batch)
            total += len(batch)

        conn.commit()
        logging.info(f"Inserted {total} raw JSON records into Snowflake")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
            mongo_client.close()
        except:
            pass


def insert_batch(cursor, batch):
    values_placeholder = ",".join(["(%s)"] * len(batch))
    flat_values = [item[0] for item in batch]

    insert_sql = f"""
        INSERT INTO CATALYS.RAW.EVENTS (raw)
        SELECT PARSE_JSON(v)
        FROM VALUES {values_placeholder} AS t(v)
    """

    cursor.execute(insert_sql, flat_values)



if __name__ == "__main__":
    load_raw_json()
