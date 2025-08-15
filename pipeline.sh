#!/bin/bash
set -e  # stop on first error

python scripts/generate_data.py

python scripts/clean_data.py

python scripts/load_data.py

streamlit run scripts/dashboard.py

echo "Pipeline finished"
