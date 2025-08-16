# Simple Data Pipeline

Showcase for data pipelines with duckDB, dbt, streamlit and prefect.

## Prerequisites

```sh
pip install -r requirements.txt
```

## Commands

Run pipeline:
```sh
python pipeline.py
```

Create warehouse:
```sh
database\duckdb.exe database\warehouse.duckdb  
```

Dashboard: 
```sh
streamlit run scripts\dashboard.py
```
