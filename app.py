# app.py

import streamlit as st
from sqlalchemy import text
import pandas as pd

# Import the modules
from database_utils import create_sqlite_engine
from data_extraction import extract_and_store_data
from data_querying import execute_query, extract_ticker_from_query
from data_visualization import visualize_data_with_pygwalker

from pygwalker.api.streamlit import init_streamlit_comm

def main():
    # Configure Streamlit page
    st.set_page_config(
        page_title="Stock Data Management App",
        layout="wide"
    )

    # Initialize pygwalker communication
    init_streamlit_comm()

    st.title("Stock Data Management App")
    st.sidebar.header("Options")

    # Sidebar options
    option = st.sidebar.selectbox("Choose an action", ["Extract Data", "Query Data"])

    # Create a persistent SQLite database engine
    if "db_engine" not in st.session_state:
        st.session_state.db_engine = create_sqlite_engine()

    engine = st.session_state.db_engine

    # Session state to store tickers
    if "tickers_list" not in st.session_state:
        st.session_state.tickers_list = ["AAPL", "MSFT", "GOOGL"]  # Default tickers are uppercase

    # Session state to store query results
    if "query_result" not in st.session_state:
        st.session_state.query_result = None

    # Session state to store the current selected stock
    if "current_stock" not in st.session_state:
        st.session_state.current_stock = st.session_state.tickers_list[0]

    if option == "Extract Data":
        st.subheader("Extract Stock Data")

        # Input for tickers
        tickers = st.text_input("Enter stock tickers (comma-separated):", ",".join(st.session_state.tickers_list))
        tickers_list = [ticker.strip().upper() for ticker in tickers.split(",")]  # Convert to uppercase

        if st.button("Extract and Store Data"):
            with st.spinner("Extracting data..."):
                try:
                    extract_and_store_data(tickers_list, engine)
                    st.session_state.tickers_list = tickers_list  # Already in uppercase
                    st.success("Data extracted and stored successfully!")
                except Exception as e:
                    st.error(f"Error: {e}")

    elif option == "Query Data":
        st.subheader("Query Stock Data")

        # Default query setup
        default_ticker = st.session_state.tickers_list[0] if st.session_state.tickers_list else "AAPL"
        query = st.text_area("Enter your SQL query:", f"SELECT * FROM {default_ticker} LIMIT 10")

        # Extract ticker and validate
        extracted_ticker = extract_ticker_from_query(query)
        if extracted_ticker:
            st.session_state.current_stock = extracted_ticker  # Already uppercase

        st.markdown(f"### Current Selected Stock: `{st.session_state.current_stock}`")

        # Check if the table exists
        selected_table = st.session_state.current_stock.upper()
        with engine.connect() as connection:
            table_exists = connection.execute(
                text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{selected_table}'")
            ).fetchone()

        if not table_exists:
            st.error(f"Table `{st.session_state.current_stock}` does not exist. Please extract data first.")
            return

        # Run query button
        if st.button("Run Query"):
            with st.spinner("Executing query..."):
                result = execute_query(query, engine)
                st.session_state.query_result = result

        # Display results and visualization
        if st.session_state.query_result is not None:
            if isinstance(st.session_state.query_result, pd.DataFrame):
                st.write("### Query Results")
                st.write(st.session_state.query_result)

                # PyGWalker Visualization Section
                st.subheader("Interactive Visualization with PyGWalker")

                # Clear the cache for PyGWalker renderer when new query is executed
                st.cache_resource.clear()

                # Visualize the data
                visualize_data_with_pygwalker(st.session_state.query_result)

            else:
                st.error(st.session_state.query_result)

if __name__ == "__main__":
    main()