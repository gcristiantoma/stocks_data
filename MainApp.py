import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
import re  # For extracting the stock ticker from the SQL query

# Function to create a persistent SQLite database engine
def create_sqlite_engine():
    """
    Creates a persistent SQLite database engine.
    """
    engine = create_engine("sqlite:///stocks.db", echo=False)  # Persistent database
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
        st.session_state.tickers_list = ["AAPL", "MSFT", "GOOGL"]  # Default tickers

    # Session state to store query results
    if "query_result" not in st.session_state:
        st.session_state.query_result = None

    # Session state to store the current selected stock
    if "current_stock" not in st.session_state:
        st.session_state.current_stock = st.session_state.tickers_list[0]  # Default to the first ticker

    if option == "Extract Data":
        st.subheader("Extract Stock Data")

        # Input for tickers
        tickers = st.text_input("Enter stock tickers (comma-separated):", ",".join(st.session_state.tickers_list))
        tickers_list = [ticker.strip() for ticker in tickers.split(",")]

        if st.button("Extract and Store Data"):
            with st.spinner("Extracting data..."):
                try:
                    extract_and_store_data(tickers_list, engine)
                    st.session_state.tickers_list = tickers_list  # Update session state
                    st.success("Data extracted and stored successfully!")
                except Exception as e:
                    st.error(f"Error: {e}")

    elif option == "Query Data":
        st.subheader("Query Stock Data")

        # Use the first ticker from the session state as the default table in the query
        default_ticker = st.session_state.tickers_list[0] if st.session_state.tickers_list else "AAPL"
        query = st.text_area("Enter your SQL query:", f"SELECT * FROM {default_ticker} LIMIT 100")

        # Extract the stock ticker from the query
        extracted_ticker = extract_ticker_from_query(query)
        if extracted_ticker:
            st.session_state.current_stock = extracted_ticker

        # Display the current selected stock
        st.markdown(f"### Current Selected Stock: `{st.session_state.current_stock}`")

        # Check if the table exists in the database
        with engine.connect() as connection:
            table_exists = connection.execute(
                text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{st.session_state.current_stock}'")
            ).fetchone()

        if not table_exists:
            st.error(f"Table `{st.session_state.current_stock}` does not exist. Please extract data first.")
            print(f"Table `{st.session_state.current_stock}` does not exist.")
            return

        # Run the query and store the result in session state
        if st.button("Run Query"):
            with st.spinner("Executing query..."):
                try:
                    result = execute_query(query, engine)
                    st.session_state.query_result = result  # Store result in session state
                except Exception as e:
                    st.error(f"Error: {e}")

        # Display the query result from session state
        if st.session_state.query_result is not None:
            if isinstance(st.session_state.query_result, pd.DataFrame):
                st.write(st.session_state.query_result)

                # Plotting functionality
                st.subheader("Trendline and Chart Plot")
                column_to_plot = st.selectbox("Select a column to plot:", st.session_state.query_result.columns)

                if column_to_plot:
                    # Ensure the column is numeric
                    try:
                        data = st.session_state.query_result[column_to_plot].astype(float)
                        dates = pd.to_datetime(st.session_state.query_result['Date'])

                        # Plot the data with a trendline
                        fig, ax = plt.subplots(figsize=(10, 6))
                        ax.plot(dates, data, label="Data", color="blue")

                        # Add a trendline
                        x = np.arange(len(data))
                        z = np.polyfit(x, data, 1)  # Linear regression
                        p = np.poly1d(z)
                        ax.plot(dates, p(x), label="Trendline", color="red", linestyle="--")

                        ax.set_title(f"{column_to_plot} Trendline")
                        ax.set_xlabel("Date")
                        ax.set_ylabel(column_to_plot)
                        ax.legend()

                        st.pyplot(fig)

                    except Exception as e:
                        st.error(f"Error plotting data: {e}")
            else:
                st.error(st.session_state.query_result)

if __name__ == "__main__":
    main()