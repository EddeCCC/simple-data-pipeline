import streamlit as st
from streamlit_autorefresh import st_autorefresh
import duckdb as db
from duckdb.duckdb import DuckDBPyConnection
from duckdb.experimental.spark import DataFrame
from prefect.blocks.system import Secret
from enum import Enum

DB_PATH = Secret.load("database").get()
TABLE_NAME = "users"

class UserStatus(Enum):
    """All available user status"""
    ALL = "all"
    ACTIVE = "activ"
    INACTIVE = "inactiv"

class SortOption(Enum):
    """All available sorting options"""
    AGE = "age"
    SIGNUP_DATE_DESC = "signup date (newest)"
    SIGNUP_DATE_ASC = "signup date (oldest)"


def build_users_query(status: UserStatus, sort: SortOption) -> str:
    """Build an SQL query based on filters and sort order"""
    query = f"SELECT * FROM {TABLE_NAME}"

    if status == UserStatus.ACTIVE:
        query += " WHERE is_active = TRUE"
    elif status == UserStatus.INACTIVE:
        query += " WHERE is_active = FALSE"

    if sort == SortOption.AGE:
        query += " ORDER BY age DESC"
    elif sort == SortOption.SIGNUP_DATE_DESC:
        query += " ORDER BY signup_date DESC"
    elif sort == SortOption.SIGNUP_DATE_ASC:
        query += " ORDER BY signup_date ASC"

    return query

def build_loyal_users_query() -> str:
    return "SELECT * FROM users_loyal"

# ------------------------
# Load data into dashboard
# ------------------------

# Use still need to refresh the website manually to view new data
st_autorefresh(interval=60_000, key="data_refresh")

st.title("Simple Dashboard")
st.sidebar.header("Filter")

with db.connect(DB_PATH) as con:
    status_filter = st.sidebar.selectbox("User status", list(UserStatus), format_func=lambda status: status.value)
    sort_option = st.sidebar.selectbox("Sort by", list(SortOption), format_func=lambda option: option.value)
    query = build_users_query(status_filter, sort_option)
    users = con.execute(query).df()

    query = build_loyal_users_query()
    users_loyal = con.execute(query).df()

users["signup_date"] = users["signup_date"].dt.strftime("%Y-%m-%d")               # fix date format
users = users.rename(columns={"signup_date": "signup", "is_active": "active"})    # fix column names

st.subheader("All Users")
columns = ["name", "email", "signup", "active", "age"]
st.dataframe(users[columns], use_container_width=True)

st.subheader("Statistics")
col1, col2, col3 = st.columns(3)

col1.metric("Total", len(users))
col2.metric("Avg age", round(users["age"].mean(), 1))
col3.metric("Active users", users["active"].sum())

st.subheader("Most Loyal Users")
st.dataframe(users_loyal, use_container_width=True)
