# Simple Data Pipeline

Showcase for data pipelines with DuckDB, dbt, Streamlit and Prefect.

The pipeline is scheduled to run every minute via Prefect.

Fake users will be created, cleaned and loaded into a DuckDB file.
Additionally, we transform loaded data via dbt.
A dashboard shows the final user data.

- Dashboard: http://localhost:8501/
- Prefect Server: http://localhost:4200/

## Prerequisites

```sh
pip install -r requirements.txt
```

## Commands

Run pipeline:
```sh
./run-pipeline.sh
```

### Sub-Commands

Create warehouse:
```sh
database\duckdb.exe database\warehouse.duckdb  
```

Dashboard: 
```sh
streamlit run scripts\dashboard.py
```
