from src.ingestion_stock import fetch_stock_data
from src.db.db_operations import create_db_engine, init_db, insert_raw_data, insert_raw_data_with_cdc
from src.config.constants import TICKERS
import pandas as pd
import time

def main():

    all_tickers = []

    for ticker in TICKERS:
        print(f"Fetching data for {ticker}...")
        records=fetch_stock_data(ticker)
        all_tickers.extend(records)  # Collect data for all tickers
        time.sleep(12)  # Sleep for 12 seconds to avoid hitting the API rate limit

    if all_tickers:
        
        df = pd.DataFrame(all_tickers)

        # Convert 'Date' to datetime format
        df['Date'] = pd.to_datetime(df['Date'])

        # Set 'Date' as the index
        df.set_index('Date', inplace=True)

        # Display all columns in the DataFrame
        pd.set_option('display.max_columns', None)
        
        # Print the resulting DataFrame
        print(df)

    else:
        print("No data to display.")
    
    # Initialize the database and create tables
    try:
        db_engine = init_db()
        insert_raw_data_with_cdc(db_engine, df, table_name='raw_data')
    except Exception as e:
        print("Database connection failed. Data not inserted.")


if __name__ == "__main__":
    main()