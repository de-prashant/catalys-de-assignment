import os
import yaml
import argparse
import snowflake.connector
import hashlib
import logging
from dotenv import load_dotenv


# -----------------------------
# CLI Usage
# Load all datasets
# python etl/load_to_snowflake.py --all
# Load only transactions
# python etl/load_to_snowflake.py --table transactions
# -----------------------------

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()


# -----------------------------
# ARGUMENT PARSER
# -----------------------------
def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Snowflake ETL Loader")

    parser.add_argument("--table", help="Table name to load")
    parser.add_argument("--all", action="store_true", help="Load all tables")

    return parser.parse_args()


# -----------------------------
# LOAD YAML CONFIG
# -----------------------------
def load_config():
    """Load YAML configuration for ingestion."""
    with open("config/ingestion_config.yml", "r") as f:
        return yaml.safe_load(f)


# -----------------------------
# SNOWFLAKE CONNECTION
# -----------------------------
def get_connection():
    """Establish a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


# -----------------------------
# EXECUTE QUERY
# -----------------------------
def execute_query(conn, query, params=None):
    """Execute a query with optional parameters."""
    with conn.cursor() as cur:
        logging.info(f"Executing SQL: {query.strip()}")
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)


# -----------------------------
# CREATE STAGE
# -----------------------------
def create_stage(conn, stage_name, file_format):
    """Create a Snowflake stage if it does not exist."""
    query = f"""
    CREATE STAGE IF NOT EXISTS {stage_name}
    FILE_FORMAT = {file_format};
    """
    execute_query(conn, query)


# -----------------------------
# CALCULATE CHECKSUM OF A FILE
# -----------------------------
def calculate_checksum(file_path):
    """Calculate SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

# -----------------------------
# CHECK IF FILE ALREADY LOADED
# -----------------------------
def file_already_loaded(conn, file_name, file_checksum, table_name, tracking_table):
    """Check if a file with the same checksum has already been loaded for a table."""
    query = f"""
    SELECT COUNT(*) FROM {tracking_table}
    WHERE file_name = %s AND file_checksum = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (file_name, file_checksum, table_name))
        result = cur.fetchone()
        return result[0] > 0

# -----------------------------
# INSERT FILE TRACKING
# -----------------------------
def insert_file_tracking(conn, file_name, file_checksum, table_name, status, tracking_table):
    """Insert a record into the file load tracking table."""
    query = f"""
    INSERT INTO {tracking_table} (file_name, file_checksum, table_name, status)
    VALUES (%s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(query, (file_name, file_checksum, table_name, status))

# -----------------------------
# UPLOAD FILES TO STAGE
# -----------------------------
def upload_files(conn, stage_name, folder_path, table_name, tracking_table):
    """Upload files to Snowflake stage if not already loaded (by checksum)."""
    loaded_files = []
    for file in os.listdir(folder_path):
        file_path = os.path.abspath(os.path.join(folder_path, file))
        if not os.path.isfile(file_path):
            continue
        checksum = calculate_checksum(file_path)
        if file_already_loaded(conn, file, checksum, table_name, tracking_table):
            logging.info(f"Skipping {file} (already loaded with same checksum)")
            continue
        logging.info(f"Uploading: {file_path}")
        query = f"""
        PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE;
        """
        execute_query(conn, query)
        loaded_files.append((file, checksum))
    return loaded_files


# -----------------------------
# COPY INTO
# -----------------------------
def copy_into(conn, table_name, stage_name, pattern, on_error, file_format):
    """Copy data from stage into Snowflake table."""
    query = f"""
    COPY INTO {table_name}
    FROM @{stage_name}
    FILE_FORMAT = (FORMAT_NAME = {file_format})
    PATTERN = '{pattern}'
    ON_ERROR = '{on_error}';
    """
    execute_query(conn, query)


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run_pipeline():
    """Main ETL pipeline runner."""
    args = parse_args()
    if not args.all and not args.table:
        raise ValueError("Please provide either --all or --table")
    config = load_config()
    db = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    file_format = config["snowflake"]["file_format"]
    tracking_table = os.getenv("SNOWFLAKE_TRACKING_TABLE", "etl.file_load_tracking")
    conn = get_connection()
    try:
        for table in config["tables"]:
            table_name = table["name"]
            if args.table and table_name != args.table:
                continue
            folder_path = table["path"]
            pattern = table.get("pattern", ".*")
            on_error = table.get("on_error", "CONTINUE")
            full_table = f"{db}.{schema}.{table_name.upper()}"
            stage_name = f"{table_name}_stage"
            logging.info(f"--- Processing {table_name} ---")
            create_stage(conn, stage_name, file_format)
            loaded_files = upload_files(conn, stage_name, folder_path, table_name, tracking_table)
            if loaded_files:
                copy_into(
                    conn,
                    full_table,
                    stage_name,
                    pattern,
                    on_error,
                    file_format
                )
                for file, checksum in loaded_files:
                    insert_file_tracking(conn, file, checksum, table_name, "LOADED", tracking_table)
            else:
                logging.info("No new files to load for this table.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
    finally:
        conn.close()


# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    run_pipeline()
