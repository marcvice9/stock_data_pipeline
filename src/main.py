from src.ingestion_stock import fetch_stock_data
from src.db.db_operations import init_db, insert_raw_data_with_cdc, aggregate_stock_data
from src.config.constants import TICKERS
from src.validation.stock_data_validation import validate_stock_data
from datetime import datetime
import pandas as pd
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def fetch_and_process_data(debug=False):
    """
    Fetches stock data, validates it, and processes it into the database.
    """

    # Print the timestamp when the task starts
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*40}")
    print(f"Task started at: {current_time}")
    print(f"{'='*40}\n")

    csv_file = 'stock_data.csv'

    if debug:
        print(f"Debugging mode enabled. Loading data from '{csv_file}'...")
        try:
            # Load data from the CSV file
            df = pd.read_csv(csv_file)

            # Ensure 'Date' is in datetime format
            df['Date'] = pd.to_datetime(df['Date'])
            df['curr_timestamp'] = pd.to_datetime(df['curr_timestamp'])

            print(f"Data loaded successfully from '{csv_file}':")
            print(df.head())  # Print a sample of the data for verification
        except FileNotFoundError:
            print(f"Error: '{csv_file}' not found. Please ensure the file exists.")
            return
        except Exception as e:
            print(f"Error loading data from '{csv_file}': {e}")
            return

    else:
        all_tickers = []

        for ticker in TICKERS:
            print(f"Fetching data for {ticker}...")
            records=fetch_stock_data(ticker)
            all_tickers.extend(records)  # Collect data for all tickers
            time.sleep(12)  # Sleep for 12 seconds to avoid hitting the API rate limit

        if all_tickers:
            
            df = pd.DataFrame(all_tickers)

            # Convert 'Date' to datetime format &  Add current_timestamp column
            df['Date'] = pd.to_datetime(df['Date'])
            df['curr_timestamp'] = pd.Timestamp.now().to_datetime64()
            df['curr_timestamp'] = df['curr_timestamp'].astype('datetime64[ns]')  # Explicitly cast to datetime64[ns]

            # Display all columns in the DataFrame
            pd.set_option('display.max_columns', None)
            print(df.head())

        else:
            print("No data to display.")
    
    # Validate the data
    is_valid, errors = validate_stock_data(df)

    if not is_valid:
        print("Data validation failed with the following errors:")
        for error in errors:
            print(f"- {error}")
        return  # Stop further processing if validation fails
    
    # Initialize the database and create tables
    try:
        db_engine = init_db()
        insert_raw_data_with_cdc(db_engine, df)
        aggregate_stock_data(db_engine)
        
    except Exception as e:
        print("Database connection failed. Data not inserted.")


if __name__ == "__main__":
    fetch_and_process_data(debug=True)