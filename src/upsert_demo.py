"""
Standalone demonstration of the incremental upsert pattern.
Assumes the 'users' table already exists (populated by etl_pipeline.py).
Simulates an incoming batch: one updated existing record + one new record,
then merges it into the users table using insert/update split logic.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from etl_pipeline import DB_URL, setup_logging
import logging

logger = logging.getLogger(__name__)


def load_upsert(incoming_batch, engine, target_table, key):
    # YOUR LOGIC:
    # 1. Read existing keys from target_table
    # 2. Split incoming_batch into to_update / to_insert via .isin() on `key`
    # 3. Append to_insert via to_sql(append)
    # 4. Update to_update rows via parameterized UPDATE
    # 5. Log rows updated, inserted, final total

    existing_records = pd.read_sql(f"SELECT * FROM {target_table}", engine)

    # Split incoming batch: existing (update) vs new (insert)
    is_existing = incoming_batch[key].isin(existing_records[key])
    to_update = incoming_batch[is_existing]
    to_insert = incoming_batch[~is_existing]

    # Insert new records
    logger.info(f"To insert: {len(to_insert)} row(s)")
    to_insert.to_sql(target_table, engine, if_exists="append", index=False)
    logger.info(f"Inserted {len(to_insert)} row(s)")

    # Update existing records (parameterized — no string interpolation)
    logger.info(f"To update: {len(to_update)} row(s)")
    update_stmt = text(f"UPDATE {target_table} SET email = :email, phone = :phone WHERE {key} = :key_val")
    with engine.connect() as conn:
        for _, row in to_update.iterrows():
            conn.execute(update_stmt, {"email": row["email"], "phone": row["phone"], "key_val": row[key]})
        conn.commit()
        logger.info(f"Updated {len(to_update)} row(s)")


def main():
    setup_logging()
    logger.info("Upsert demo started")
    engine = create_engine(DB_URL)
    
    # Read back a data
    data = pd.read_sql("SELECT * FROM users", engine)

    # t1 = existing record (id=1) with modified email + phone; t2 = new record (id=11)
    t1 = [1, 'Leanne Graham', 'Leanne.Graham@april.biz', '1-776-775-5845 x46585', 'hildegard.org', 'Kulas Light', 'Apt. 556', 'Gwenborough', '92998-43874', 'Romaguera-Crona']
    t2 = [11, 'John Smith', 'John.Smith@almond.biz', '1-771-734-7865 x68755', 'kontas.org', 'kansas Dark', 'Apt. 7512', 'Miami', '96546-5564', 'Delloite']

    # Create incoming_batch dataset from above udpated and new record
    incoming_batch = pd.DataFrame([t1, t2], columns=data.columns)

    load_upsert(incoming_batch, engine, target_table="users", key="id")


if __name__ == "__main__":
    main()