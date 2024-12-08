# data_extraction.py

import yfinance as yf
import pandas as pd

def extract_and_store_data(tickers, engine):
    """
    Downloads stock data for the given tickers and stores it in an SQLite database.
    """
    for ticker in tickers:
        try:
            print(f"Downloading data for {ticker}...")
            df = yf.download(ticker, period="max")
            df.reset_index(inplace=True)  # Reset index to make 'Date' a column

            # Format the Date column to YYYY-MM-DD
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

            # Rename columns to ensure consistent names
            df.columns = [col.replace(" ", "_") for col in df.columns]

            print(f"Data for {ticker} downloaded successfully. Storing in database...")

            # Store data in SQLite database
            df.to_sql(ticker.upper(), con=engine, if_exists="replace", index=False)
            print(f"Data for {ticker} stored successfully.")
        except Exception as e:
            print(f"Error while processing {ticker}: {e}")