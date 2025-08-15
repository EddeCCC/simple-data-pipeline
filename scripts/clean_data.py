import os
import pandas as pd
from logger import logging as log

def normalize_is_active(value):
    """Converts the value to `Boolean` or `None`"""
    if str(value).strip().lower() in ["yes", "true", "1"]:
        return True
    elif str(value).strip().lower() in ["no", "false", "0"]:
        return False
    return None

def clean_users(input_data):
    """Cleans raw users data by normalizing or removing invalid rows"""
    df = pd.read_csv(input_data)

    # Normalize is_active fields
    df["is_active"] = df["is_active"].apply(normalize_is_active)
    df = df.dropna(subset=["is_active"])

    # Remove invalid ages
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = df[df["age"].between(18, 100)]

    return df.drop_duplicates()

if __name__ == "__main__":
    log.info("Cleaning data...")

    data = "data/raw/users.csv"
    if not os.path.exists(data):
        print(f"Data not found: {data}")
        exit(1)

    cleaned_users = clean_users(data)

    output = "data/cleaned/users.csv"
    cleaned_users.to_csv(output, index=False)

    log.info(f"Cleaned data saved to {output}")
