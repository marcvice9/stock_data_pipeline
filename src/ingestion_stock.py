import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Get the API key from the environment variable
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
if not API_KEY:
    raise ValueError("API key not found. Please make sure to set the ALPHA_VANTAGE_API_KEY in your .env file.")

# Define your tickers
tickers = ['AAPL', 'GOOGL', 'MSFT']

# Base URL for the Alpha Vantage API (daily time series data)
base_url = 'https://www.alphavantage.co/query'

# List to collect data for all tickers
all_tickers = []

# Function to fetch stock data for a specific ticker
def fetch_stock_data(ticker):

    # Define the parameters for the API request
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': ticker,
        'apikey': API_KEY,
        'outputsize': 'compact' # 'compact' returns 100 data points (last 100 days)
    }

    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()

        # Parse the data to get the daily time series
        time_series = data.get('Time Series (Daily)', {})
        if time_series:
            
            records = []
            for date, stats in time_series.items():
                record = {
                    "Date": date,
                    "Ticker": str(ticker),
                    "Open": float(stats['1. open']),
                    "High": float(stats['2. high']),
                    "Low": float(stats['3. low']),
                    "Close": float(stats['4. close']),
                    "Volume": int(stats['5. volume'])
                }
                records.append(record)
            all_tickers.extend(records)  # Collect data for all tickers
            print(f"Data for {ticker} fetched successfully.")

        else:
            print("No time series data found.")

    else:
        print(f"Error fetching data: {response.status_code}")

for ticker in tickers:
    print(f"Fetching data for {ticker}...")
    fetch_stock_data(ticker)
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