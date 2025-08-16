import duckdb as db
from prefect.blocks.system import Secret
import os
from logger import logging as log

TABLE_NAME = "users"

if __name__ == "__main__":
    log.info("Loading data...")

    data = "data/cleaned/users.csv"
    if not os.path.exists(data):
        print(f"Data not found: {data}")
        exit(1)

    db_file = Secret.load("database").get()
    with db.connect(db_file) as con:
        con.sql(f"CREATE OR REPLACE TABLE '{TABLE_NAME}' AS SELECT * FROM '{data}';")
        count = con.sql(f"SELECT COUNT(*) FROM '{TABLE_NAME}'").fetchone()[0]
        log.info(f"{count} rows loaded")

    log.info(f"Data loaded to {db_file}")
