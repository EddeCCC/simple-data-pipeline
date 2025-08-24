#!/bin/bash

# Start orchestration server
prefect server start &
sleep 5

# Start dashboard
streamlit run scripts/dashboard.py &

# Start pipeline
python pipeline.py