from prefect import flow, task
from prefect.blocks.system import Secret
from pydantic import SecretStr
import subprocess
import duckdb
import os

DB_PATH = "database/warehouse.duckdb"

@task
def setup_database():
    if not os.path.exists(DB_PATH):
        # create file if non-existing
        duckdb.connect(DB_PATH).close()

    secret = SecretStr(DB_PATH)
    Secret(value=secret).save("database", overwrite=True)

@task(retries=2, retry_delay_seconds=5)
def generate_data():
    subprocess.run(["python", "scripts/generate_data.py"], check=True)

@task(retries=2, retry_delay_seconds=5)
def clean_data():
    subprocess.run(["python", "scripts/clean_data.py"], check=True)

@task(retries=2, retry_delay_seconds=5)
def load_data():
    subprocess.run(["python", "scripts/load_data.py"], check=True)

@task(retries=2, retry_delay_seconds=5)
def transform_data():
    subprocess.run([
        "dbt", "run",
        "--project-dir", "transformation",
        "--profiles-dir", "transformation",
        "--profile", "transformation"
    ], check=True)

@task(retries=2, retry_delay_seconds=5)
def start_dashboard():
    subprocess.run(["streamlit", "run", "scripts/dashboard.py"], check=True)

@flow
def pipeline():
    setup_database()
    generate_data()
    clean_data()
    load_data()
    transform_data()
    start_dashboard()

if __name__ == "__main__":
    pipeline()
