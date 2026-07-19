"""
This script provides the code for ETL basic learning. There are two part involves in this learning.
Part A: extract netflix csv data > save raw data as parquet > transform the raw data and save the transformed data as parquet > load transformed data to SQLite DB.
Part B: extract users data from API > save raw data as parquet > transform the raw data and save the transformed data as parquet > load transformed data to SQLite DB.
"""

# Import required libraries
import pandas as pd
import requests
import logging
import time
import random
import os
from sqlalchemy import create_engine, text, inspect

CSV_PATH = "/home/vinaygautam/etl_learning/data/Netflix TV Shows and Movies.csv"
DB_URL = "sqlite:////home/vinaygautam/etl_learning/etl_learning.db"
LOG_PATH = "/home/vinaygautam/etl_learning/logs/etl_pipeline.log"
API_URL = "https://jsonplaceholder.typicode.com/users"
RAW_DIR = "/home/vinaygautam/etl_learning/data/raw/"
TRANSFORMED_DIR = "/home/vinaygautam/etl_learning/data/transformed/"

logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
                        level=logging.INFO,                       # minimum severity to capture
                        format="%(asctime)s | %(levelname)s | %(message)s",   # timestamp | severity | message
                        handlers=[logging.FileHandler(LOG_PATH),   # write to file
                                    logging.StreamHandler()                     # also show on console
                                    ]
                        )


def extract_from_csv(path):
    try:
        df = pd.read_csv(path)
        logger.info(f"Extracted {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except FileNotFoundError:
        logger.error(f"Error: File not found at {path}")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"Error: File is Empty")
        raise
    except pd.errors.ParserError as e:
        logger.error(f"Error: Could not parse CSV - {e}")
        raise


def extract_from_api(url, max_retries=3, base_delay=1, max_delay=32):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logger.info(f"API extraction successful: Status {response.status_code}")
            data = response.json()
            return pd.json_normalize(data)
        
        # 1. Handle explicit Network Layer timeouts & drops
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Attempt {attempt + 1} failed due to network transient error: {e}")
        
        # 2. Handle Application Layer HTTP status errors
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            logger.warning(f"Attempt {attempt + 1} failed with HTTP Status: {status_code}")

            # Immediately raise and exit if it's a permanent failure (e.g., 404, 403)
            retryable_codes = {429, 500, 502, 503, 504}
            if status_code not in retryable_codes:
                logger.error(f"Critical non-retryable status {status_code}. Aborting execution loop.")
                raise
        # 3. Structural fallback catch-all
        except requests.exceptions.RequestException as e:
            logger.error(f"Fatal anomalous network layer library error: {e}")
            raise

        # --- RETRY & TIMING CONTROL ---
        if attempt == max_retries - 1:
            logger.error("Max retries reached. Failing permanently.")
            raise requests.exceptions.RequestException("Max retries exceeded with transient failures.")
        
        # 1. Compute standard exponential backoff: Base * 2 ^ attempt
        delay_window = min(max_delay, base_delay * (2 ** attempt))
        # 2. Apply Full Jitter: Pick a random float between 0 and the delay
        sleep_time = random.uniform(0, delay_window)
        logger.warning(f"Backing off {sleep_time:.2f}s (Max Window: {delay_window}s) before next attempt.")
        time.sleep(sleep_time)

def save_parquet(df,path,name,stage):
    full_path = os.path.join(path,f"{name}_{stage}.parquet")
    df.to_parquet(full_path)
    df_check = pd.read_parquet(full_path)
    logger.info(f"{name} — original: {df.shape}, read back: {df_check.shape}")

def log_null_profile(df, name):
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]   # only columns that actually have nulls
    if len(nulls) > 0:
        logger.info(f"{name} null profile: {nulls.to_dict()}")
    else:
        logger.info(f"{name}: no nulls detected")

def transform_netflix(df):
    df = df.copy()
    before_shape = df.shape
    # Handle nulls
    df['description'] = df['description'].fillna("Description not available")
    df['age_certification'] = df['age_certification'].fillna("Certification not available")
    # imdb_votes left as NaN (numeric measurement — no fabrication)
    # Drop redundant index column
    df = df.drop('index', axis=1)
    logger.info(f"Netflix transformed: {before_shape} -> {df.shape}")
    return df

def transform_users(df):
    df = df.copy()
    before_shape = df.shape
    # droping unwanted columns
    df = df.drop(['username', 'address.geo.lat', 'address.geo.lng',
                  'company.catchPhrase', 'company.bs'],axis=1)
    # renaming the complex column names for readability
    df = df.rename(columns={'address.street': 'street',
                            'address.city': 'city',
                            'address.suite': 'suite',
                            'address.zipcode': 'zipcode',
                            'company.name': 'company_name'
                            })
    logger.info(f"Users transformed: {before_shape} -> {df.shape}")
    return df

    
def load_full_atomic(df, engine, target_table):
    df = df.copy()
    staging_table = f"{target_table}_staging"
    backup_table = f"{target_table}_old"

    # 1. Get the baseline row count from your python memory space
    expected_row_count = len(df)
    logger.info(f"Starting load. Expected row count from transformed data: {expected_row_count}")

    # 2. Stream data to the hidden staging table
    logger.info(f"Streaming rows into staging table: '{staging_table}'...")
    df.to_sql(name=staging_table, con=engine, if_exists="replace", index=False)

    # 3. Audit Check: Verify what actually landed in the database
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {staging_table};"))
        actual_row_count = result.scalar()

    logger.info(f"Audit Check -> Expected: {expected_row_count} | Actual in DB: {actual_row_count}")

    if expected_row_count != actual_row_count:
        error_msg = f"Data Integrity check FAILED! Missing rows. Expected {expected_row_count}, got {actual_row_count}."
        logger.error(error_msg)
        raise ValueError(error_msg)

    inspector = inspect(engine)
    table_exists = target_table in inspector.get_table_names()

    with engine.begin() as transaction_conn:
        transaction_conn.execute(text(f"DROP TABLE IF EXISTS {backup_table};"))
        if table_exists:
            transaction_conn.execute(text(f"ALTER TABLE {target_table} RENAME TO {backup_table};"))
        transaction_conn.execute(text(f"ALTER TABLE {staging_table} RENAME TO {target_table}"))

        logger.info("Production table successfully swapped and updated!")


def main():
    setup_logging()
    logger.info("Pipeline started")

    # Connect to (and create) SQLite database
    engine = create_engine(DB_URL)

    # Extract from CSV
    df_nf = extract_from_csv(path=CSV_PATH)
    # Saving the extracted files in parquet form
    save_parquet(df_nf,path=RAW_DIR,name="netflix",stage="raw")

    # Extract from URL
    df_users = extract_from_api(url=API_URL)
    # Saving the extracted files in parquet form
    save_parquet(df_users,path=RAW_DIR,name="users",stage="raw")

    # log the null values profile in netflix
    log_null_profile(df_nf,name="netflix")
    # Tranform the netflix data
    df_nf_trfm = transform_netflix(df_nf)
    # Saving the transformed files in parquet form
    save_parquet(df_nf_trfm,path=TRANSFORMED_DIR,name="netflix",stage="transformed")

    # log the null values profile in users
    log_null_profile(df_users,name="users")
    # Tranform the users data
    df_users_trfm = transform_users(df_users)
    # Saving the transformed files in parquet form
    save_parquet(df_users_trfm,path=TRANSFORMED_DIR,name="users",stage="transformed")
    
    # Load the both transformed netflix and users data to database.
    load_full_atomic(df_nf_trfm, engine, target_table='netflix')
    load_full_atomic(df_users_trfm, engine, target_table='users')


if __name__ == "__main__":
    main()