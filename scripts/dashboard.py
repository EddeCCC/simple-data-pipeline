import streamlit as st
import duckdb as db
from enum import Enum

DB_PATH = "../database/warehouse.duckdb"
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


def build_query(status: UserStatus, sort: SortOption) -> str:
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


# ------------------------
# Load data into dashboard
# ------------------------
con = db.connect("database/warehouse.duckdb")

st.title("Simple Dashboard")
st.sidebar.header("Filter")

status_filter = st.sidebar.selectbox("User status", list(UserStatus), format_func=lambda status: status.value)
sort_option = st.sidebar.selectbox("Sort by", list(SortOption), format_func=lambda option: option.value)

columns = ["name", "email", "signup", "active", "age"]
query = build_query(status_filter, sort_option)
df = con.execute(query).df()
df["signup_date"] = df["signup_date"].dt.strftime("%Y-%m-%d")               # fix date format
df = df.rename(columns={"signup_date": "signup", "is_active": "active"})    # fix column names

st.subheader("All Users")
st.dataframe(df[columns], use_container_width=True)

# TODO Is this the right place to calculate aggregations?
st.subheader("Statistics")
col1, col2, col3 = st.columns(3)
col1.metric("Total", len(df))
col2.metric("Avg age", round(df["age"].mean(), 1))
col3.metric("Active users", df["active"].sum())
