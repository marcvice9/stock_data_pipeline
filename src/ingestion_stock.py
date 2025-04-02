import requests
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Get the API key from the environment variable
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = os.getenv("ALPHA_VANTAGE_BASE_URL")

if not API_KEY:
    raise ValueError("API key not found. Please make sure to set the ALPHA_VANTAGE_API_KEY in your .env file.")
if not BASE_URL:
    raise ValueError("BASE_URL not found. Please make sure to set the ALPHA_VANTAGE_BASE_URL in your .env file.")

# Function to fetch stock data for a specific ticker
def fetch_stock_data(ticker):
    """
    Fetches daily stock data for a specific ticker from the Alpha Vantage API.

    Args:
        ticker (str): The stock ticker symbol.

    Returns:
        list: A list of dictionaries containing stock data for each day.
    """

    # Define the parameters for the API request
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': ticker,
        'apikey': API_KEY,
        'outputsize': 'compact' # 'compact' returns 100 data points (last 100 days)
    }

    response = requests.get(BASE_URL, params=params)
    
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
            
            print(f"Data for {ticker} fetched successfully.")
            return records

        else:
            print("No time series data found.")
            return []

    else:
        print(f"Error fetching data: {response.status_code}")
        return []
