# data_querying.py

from sqlalchemy import text
import pandas as pd
import re

def execute_query(query, engine):
    """
    Executes the given SQL query on the SQLite database and returns the result as a DataFrame.
    """
    try:
        print(f"Executing query: {query}")
        with engine.connect() as connection:
            result = pd.read_sql_query(text(query), connection)

            # Clean up column names - remove table name prefix or tuple formatting
            result.columns = [col.split('.')[-1] if isinstance(col, str) else col[0] for col in result.columns]

            # Format the Date column if it exists
            if 'Date' in result.columns:
                result['Date'] = pd.to_datetime(result['Date']).dt.strftime('%Y-%m-%d')

        print("Query executed successfully.")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return f"Error executing query: {e}"

def extract_ticker_from_query(query):
    match = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
    if match:
        return match.group(1).upper()  # Convert to uppercase
    return None