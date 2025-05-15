import yfinance as yf
import pandas as pd
import os
import datetime
import time
import glob

# --- Configuration ---
CONFIG_FILE = "tickers.txt"
BASE_DOWNLOAD_DIR = "data" # As per your last log
MAX_FILE_SIZE_MB = 50  # Max size for each CSV file in MB
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
DOWNLOAD_INTERVAL = "1m"
DOWNLOAD_PERIOD_DAYS = 7 # For initial download and updates

# --- Helper Functions ---

def parse_config(config_file_path):
    """Parses the configuration file."""
    if not os.path.exists(config_file_path):
        print(f"Error: Configuration file '{config_file_path}' not found.")
        return None
    
    config = {}
    with open(config_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            try:
                stock_type, tickers_str = line.split(':', 1)
                tickers = [t.strip() for t in tickers_str.split(',')]
                config[stock_type.strip()] = tickers
            except ValueError:
                print(f"Warning: Skipping malformed line in config: {line}")
    return config

def get_latest_timestamp_from_dir(ticker_dir):
    """
    Finds the latest timestamp (as a timezone-aware pandas Timestamp in UTC)
    from all CSV files in a ticker's directory. Returns None if no valid data.
    """
    csv_files = sorted(glob.glob(os.path.join(ticker_dir, "*.csv")))
    if not csv_files:
        return None

    latest_overall_timestamp_utc = None
    
    last_file = csv_files[-1]
    try:
        # Read the index column as is first, then parse
        df_last_file = pd.read_csv(last_file, index_col=0) 
        
        if df_last_file.empty:
            return None

        known_csv_datetime_format = '%Y-%m-%d %H:%M:%S%z'
        parsed_index = None
        try:
            parsed_index = pd.to_datetime(df_last_file.index, format=known_csv_datetime_format, utc=True)
        except (ValueError, TypeError):
            parsed_index = pd.to_datetime(df_last_file.index, errors='coerce', utc=True)

        valid_timestamps = parsed_index.dropna()

        if valid_timestamps.empty:
            return None
        
        file_latest_ts_utc = valid_timestamps.sort_values()[-1]

        if not isinstance(file_latest_ts_utc, pd.Timestamp):
            print(f"Internal Error: Last timestamp from {last_file} is not pd.Timestamp. Type: {type(file_latest_ts_utc)}")
            return None
        
        if file_latest_ts_utc.tzinfo is None or file_latest_ts_utc.tzinfo.utcoffset(file_latest_ts_utc) != datetime.timedelta(0):
            file_latest_ts_utc = file_latest_ts_utc.tz_localize('UTC', ambiguous='infer') if file_latest_ts_utc.tzinfo is None else file_latest_ts_utc.tz_convert('UTC')

        if latest_overall_timestamp_utc is None or file_latest_ts_utc > latest_overall_timestamp_utc:
            latest_overall_timestamp_utc = file_latest_ts_utc
            
    except pd.errors.EmptyDataError:
        pass 
    except FileNotFoundError:
        print(f"Warning: File {last_file} disappeared unexpectedly.")
        return None
    except Exception as e:
        print(f"Warning: Could not read or parse last timestamp from {last_file}: {e}")
        return None

    return latest_overall_timestamp_utc


def get_next_available_filename(ticker_dir, base_filename="data"):
    """Finds the next available filename like data_0.csv, data_1.csv, etc."""
    i = 0
    while True:
        filename = os.path.join(ticker_dir, f"{base_filename}_{i}.csv")
        if not os.path.exists(filename):
            return filename
        i += 1

def download_and_store_data(stock_type, ticker):
    """Downloads data for a single ticker and stores/updates it."""
    print(f"\nProcessing [{stock_type}] -> {ticker}...")
    ticker_dir = os.path.join(BASE_DOWNLOAD_DIR, stock_type, ticker)
    os.makedirs(ticker_dir, exist_ok=True)

    latest_timestamp_from_csv = get_latest_timestamp_from_dir(ticker_dir)
    
    end_date_yf = pd.Timestamp.now(tz='UTC')

    if latest_timestamp_from_csv:
        start_date_yf = latest_timestamp_from_csv + pd.Timedelta(minutes=1)
        print(f"  Found existing data. Last UTC timestamp: {latest_timestamp_from_csv}. Fetching from UTC: {start_date_yf}")
        
        if start_date_yf >= end_date_yf:
            print(f"  Data is already up to date for {ticker}. Last record at {latest_timestamp_from_csv}.")
            return
    else:
        start_date_yf = end_date_yf - pd.Timedelta(days=DOWNLOAD_PERIOD_DAYS)
        print(f"  No existing data. Fetching for last {DOWNLOAD_PERIOD_DAYS} days. From UTC: {start_date_yf}")

    try:
        print(f"  Fetching data for {ticker} from {start_date_yf.strftime('%Y-%m-%d %H:%M:%S %Z')} to {end_date_yf.strftime('%Y-%m-%d %H:%M:%S %Z')} interval {DOWNLOAD_INTERVAL}")
        
        data = yf.download(
            ticker, 
            start=start_date_yf, 
            end=end_date_yf, 
            interval=DOWNLOAD_INTERVAL, 
            progress=False, 
            auto_adjust=True
        )
        
    except Exception as e:
        print(f"  Error downloading data for {ticker}: {e}")
        return

    if data.empty:
        print(f"  No new data found for {ticker} in the requested period.")
        return

    # Ensure downloaded data index is UTC
    if data.index.tz is None:
        data.index = data.index.tz_localize('UTC', ambiguous='infer')
    else:
        data.index = data.index.tz_convert('UTC')
    
    data = data.sort_index()

    # Filter out any data that might be before or exactly at our latest_timestamp_from_csv
    if latest_timestamp_from_csv:
        data = data[data.index > latest_timestamp_from_csv]

    if data.empty:
        print(f"  No new data after filtering for {ticker}.")
        return

    print(f"  Downloaded {len(data)} new rows for {ticker}.")

    # --- Storing data ---
    csv_files = sorted(glob.glob(os.path.join(ticker_dir, "*.csv")))
    target_csv_file = None

    if csv_files:
        target_csv_file = csv_files[-1]
        try:
            if os.path.getsize(target_csv_file) >= MAX_FILE_SIZE_BYTES:
                print(f"  File {target_csv_file} is full. Creating a new one.")
                target_csv_file = get_next_available_filename(ticker_dir)
        except FileNotFoundError: # Should not happen if glob found it but good practice
             target_csv_file = get_next_available_filename(ticker_dir)
    else: # No CSV files exist yet
        target_csv_file = get_next_available_filename(ticker_dir)

    rows_to_write = data.copy()
    
    while not rows_to_write.empty:
        current_file_size = 0 # Initialize for current target_csv_file
        if os.path.exists(target_csv_file):
            # --- !!! THIS IS THE FIX !!! ---
            current_file_size = os.path.getsize(target_csv_file)
            # --- !!! END OF FIX !!! ---

        # Determine if header needs to be written for the current target_csv_file
        write_header = not os.path.exists(target_csv_file) or current_file_size == 0
        
        estimated_bytes_per_row = 150 # A rough guess for OHLCV + timestamp
        estimated_new_data_size = len(rows_to_write) * estimated_bytes_per_row

        # Check if current target_csv_file needs to be rolled over
        if current_file_size > 0 and (current_file_size + estimated_new_data_size > MAX_FILE_SIZE_BYTES) :
            print(f"  File {target_csv_file} ({current_file_size / (1024*1024):.2f}MB) + new data estimate ({estimated_new_data_size / (1024*1024):.2f}MB) would exceed limit. Creating new file.")
            target_csv_file = get_next_available_filename(ticker_dir)
            write_header = True  # New file always needs a header
            current_file_size = 0 # Reset for the new file

        # Check if the data to write itself is too large for one file segment
        # (even if target_csv_file is currently empty or new)
        if len(rows_to_write.index) > 1 and estimated_new_data_size > MAX_FILE_SIZE_BYTES and current_file_size == 0:
            num_rows_for_chunk = max(1, int(MAX_FILE_SIZE_BYTES / estimated_bytes_per_row * 0.90)) 

            current_chunk = rows_to_write.iloc[:num_rows_for_chunk]
            remaining_rows = rows_to_write.iloc[num_rows_for_chunk:]
            
            print(f"  Large new data batch. Writing {len(current_chunk)} rows to {target_csv_file} (Header: {write_header}).")
            current_chunk.to_csv(target_csv_file, mode='a', header=write_header, index=True, date_format='%Y-%m-%d %H:%M:%S%z')
            
            rows_to_write = remaining_rows # Update for next iteration
            
            if not rows_to_write.empty: # If there are more rows, they go to a new file
                target_csv_file = get_next_available_filename(ticker_dir)
                # For the next iteration of this while loop, current_file_size will be 0 for this new target,
                # and write_header will correctly be True.
        else:
            # Write all (remaining) rows to the current target_csv_file
            print(f"  Writing {len(rows_to_write)} rows to {target_csv_file} (Header: {write_header}).")
            rows_to_write.to_csv(target_csv_file, mode='a', header=write_header, index=True, date_format='%Y-%m-%d %H:%M:%S%z')
            rows_to_write = pd.DataFrame() # Mark all data as written

    print(f"  Successfully updated {ticker}.")


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Yahoo Finance Data Downloader/Updater...")
    
    config_data = parse_config(CONFIG_FILE)
    if not config_data:
        print("Exiting due to configuration error.")
        exit()

    if not os.path.exists(BASE_DOWNLOAD_DIR):
        os.makedirs(BASE_DOWNLOAD_DIR)
        print(f"Created base directory: {BASE_DOWNLOAD_DIR}")

    for stock_type, tickers in config_data.items():
        print(f"\n--- Processing Stock Type: {stock_type} ---")
        type_dir = os.path.join(BASE_DOWNLOAD_DIR, stock_type)
        os.makedirs(type_dir, exist_ok=True)
        
        for ticker in tickers:
            download_and_store_data(stock_type, ticker)
            time.sleep(2)

    print("\nAll tasks completed.")
