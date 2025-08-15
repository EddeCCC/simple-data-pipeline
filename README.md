# Simple Data Pipeline

Showcase for data pipelines

## Prerequisites

```sh
pip install -r requirements.txt
```

## Commands

Run pipeline:
```sh
./pipeline.sh
```

Create warehouse:
```sh
database\duckdb.exe database\warehouse.duckdb  
```

Dashboard: 
```sh
streamlit run dashboard.py
```

Data transformation: 
```sh
dbt run --profiles-dir .  --profile transformation
```
