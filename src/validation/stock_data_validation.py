import pandas as pd

def validate_stock_data(df):
    """
    Validates the stock data DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to validate.

    Returns:
        bool: True if the data is valid, False otherwise.
        list: A list of validation error messages.
    """
    errors = []

    # Define the required columns and their expected data types
    required_columns = {
        'Date': 'datetime64[ns]',
        'Ticker': 'object',
        'Open': 'float64',
        'High': 'float64',
        'Low': 'float64',
        'Close': 'float64',
        'Volume': 'int64',
        'curr_timestamp': 'datetime64[ns]'
    }

    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")

    # Check for missing values
    if df.isnull().values.any():
        errors.append("Data contains missing values.")

    # Check if numeric columns are greater than or equal to 0
    numeric_columns =  [col for col, dtype in required_columns.items() if dtype in ['float64', 'int64']]
    for col in numeric_columns:
        if col in df.columns and (df[col] < 0).any():
            errors.append(f"Column '{col}' contains negative values.")

    # Check data types of each column
    for col, expected_dtype in required_columns.items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if actual_dtype != expected_dtype:
                errors.append(f"Column '{col}' has incorrect data type. Expected: {expected_dtype}, Found: {actual_dtype}")

    # Return validation result
    return len(errors) == 0, errors