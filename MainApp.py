import streamlit as st
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import re
from pygwalker.api.streamlit import init_streamlit_comm, StreamlitRenderer

# Function to create a persistent SQLite database engine
def create_sqlite_engine():
    """
    Creates a persistent SQLite database engine.
    """
    engine = create_engine("sqlite:///stocks.db", echo=False)
    return engine

# Function to extract and store stock data
def extract_and_store_data(tickers, engine):
    """
    Downloads stock data for the given tickers and stores it in an SQLite database.
    """
    for ticker in tickers:
        try:
            print(f"Downloading data for {ticker}...")
            df = yf.download(ticker, period="max")
            df.reset_index(inplace=True)  # Reset index to make 'Date' a column
            print(f"Data for {ticker} downloaded successfully. Storing in database...")

            # Store data in SQLite database
            df.to_sql(ticker, con=engine, if_exists="replace", index=False)
            print(f"Data for {ticker} stored successfully.")
        except Exception as e:
            print(f"Error while processing {ticker}: {e}")

# Function to execute SQL queries
def execute_query(query, engine):
    """
    Executes the given SQL query on the SQLite database and returns the result as a DataFrame.
    """
    try:
        print(f"Executing query: {query}")
        with engine.connect() as connection:
            result = pd.read_sql_query(text(query), connection)

            # Clean up column names
            new_columns = []
            for col in result.columns:
                if isinstance(col, tuple):
                    # Take only the first part of the tuple and remove any stock ticker
                    col_name = col[0].split(',')[0].strip("()'")
                    new_columns.append(col_name)
                else:
                    # Remove any stock ticker suffix if present
                    col_name = col.split(',')[0].strip("()'")
                    new_columns.append(col_name)

            # Assign the cleaned column names
            result.columns = new_columns

        print("Query executed successfully.")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return f"Error executing query: {e}"

# Function to extract the stock ticker from the SQL query
def extract_ticker_from_query(query):
    """
    Extracts the stock ticker from the SQL query.
    """
    match = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

# Streamlit app
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
        st.session_state.tickers_list = ["AAPL", "MSFT", "GOOGL"]

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
        tickers_list = [ticker.strip() for ticker in tickers.split(",")]

        if st.button("Extract and Store Data"):
            with st.spinner("Extracting data..."):
                try:
                    extract_and_store_data(tickers_list, engine)
                    st.session_state.tickers_list = tickers_list
                    st.success("Data extracted and stored successfully!")
                except Exception as e:
                    st.error(f"Error: {e}")

    elif option == "Query Data":
        st.subheader("Query Stock Data")

        # Default query setup
        default_ticker = st.session_state.tickers_list[0] if st.session_state.tickers_list else "AAPL"
        query = st.text_area(
    "Enter your SQL query:", 
    value=f"SELECT * FROM {default_ticker} LIMIT 10",
    height=300,  # Taller text area
    key="sql_query_input"  # Optional: add a key for component state management
)

        # Extract ticker and validate
        extracted_ticker = extract_ticker_from_query(query)
        if extracted_ticker:
            st.session_state.current_stock = extracted_ticker

        st.markdown(f"### Current Selected Stock: `{st.session_state.current_stock}`")

        # Check if the table exists
        with engine.connect() as connection:
            table_exists = connection.execute(
                text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{st.session_state.current_stock}'")
            ).fetchone()

        if not table_exists:
            st.error(f"Table `{st.session_state.current_stock}` does not exist. Please extract data first.")
            return

        # Run query button
        if st.button("Run Query"):
            with st.spinner("Executing query..."):
                try:
                    result = execute_query(query, engine)
                    st.session_state.query_result = result
                except Exception as e:
                    st.error(f"Error: {e}")

        # Display results and visualization
        if st.session_state.query_result is not None:
            if isinstance(st.session_state.query_result, pd.DataFrame):
                st.write("### Query Results")
                st.write(st.session_state.query_result)

                # PyGWalker Visualization Section
                st.subheader("Interactive Visualization with PyGWalker")

                # Clear the cache for PyGWalker renderer when new query is executed
                st.cache_resource.clear()

                # Initialize the StreamlitRenderer with the latest query results
                @st.cache_resource
                def get_pyg_renderer() -> "StreamlitRenderer":
                    return StreamlitRenderer(
                        st.session_state.query_result,
                        spec="./gw_config.json",
                        debug=False
                    )

                # Create and render the visualization
                if not st.session_state.query_result.empty:
                    renderer = get_pyg_renderer()
                    with st.container():
                        renderer.render_explore()
            else:
                st.error(st.session_state.query_result)

if __name__ == "__main__":
    main()