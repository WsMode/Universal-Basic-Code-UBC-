import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
import pyarrow.dataset as ds

# ---------------------------
# Configurable Parameters
# ---------------------------
DROP_TARGET = 30.0      # Drop target value (percent)
RISE_TARGET = 30.0      # Rise target value (percent)
EMA_INIT_INDEX = 371    # Compute the initial EMA using rows 1 to 371.
TRADE_START_INDEX = 371 # Begin calculated columns at row 372 (0-indexed 371).
MULTIPLIER = 2 / EMA_INIT_INDEX
PROFIT_TARGET = 0.30    # 10% profit target.
INVESTMENT_CAPITAL = 100
Strategy = 1            # 1: Buy signals. Process asset only if a valid "Last EMA% from Start"
                        #    is present and its value is below -10.
                        # 0: Sell signals. Process asset only if a valid "Last EMA% from start Re"
                        #    is present and its value is above 10.
                        # Blank: Process all assets.
TARGET_ASSET = ""       # If empty, process the entire parquet file.
                        # Otherwise, only process the asset defined by TARGET_ASSET.

# Global variable to hold the Delete Assets file path.
DELETE_ASSETS_FILE = None

# ---------------------------
# Utility Functions: Delete Assets File Handling
# ---------------------------
def add_asset_to_delete_file(symbol, name, delete_file_path):
    # If delete_file_path is None, return immediately.
    if not delete_file_path:
        print("Delete file path is None. Skipping addition of asset to delete file.")
        return
    # Read existing entries if any
    entries = set()
    if os.path.exists(delete_file_path):
        try:
            existing_df = pd.read_csv(delete_file_path)
            for sym in existing_df.get("Symbol", []):
                entries.add(sym.strip())
        except Exception as e:
            print(f"Error reading {delete_file_path}: {e}")
    # If symbol not already in there, append it
    if symbol not in entries:
        write_header = not os.path.exists(delete_file_path)
        with open(delete_file_path, "a") as f:
            if write_header:
                f.write("Symbol,Name\n")
            f.write(f"{symbol},{name}\n")
        print(f"Added asset {symbol} - {name} to Delete Assets file.")

# ---------------------------
# Utility Functions: Download and File IO
# ---------------------------
def download_asset_data(asset_list_path, all_data_csv_file, all_data_parquet_file, delete_file_path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Read Asset List
    try:
        df_asset_list = pd.read_csv(asset_list_path)
        print(f"Successfully read Asset List with {len(df_asset_list)} rows.")
    except Exception as e:
        print(f"Error reading Asset List file: {e}")
        exit(1)
    
    # Remove any assets scheduled for deletion from the Asset List
    if os.path.exists(delete_file_path):
        try:
            delete_df = pd.read_csv(delete_file_path)
            if not delete_df.empty:
                delete_symbols = set(delete_df["Symbol"].astype(str).str.strip())
                before = len(df_asset_list)
                df_asset_list = df_asset_list[~df_asset_list["Symbol"].isin(delete_symbols)]
                after = len(df_asset_list)
                if before != after:
                    print(f"Removed {before - after} rows from Asset List per Delete Assets file.")
                # Write the updated Asset List back to the file
                df_asset_list.to_csv(asset_list_path, index=False)
        except Exception as e:
            print(f"Error updating Asset List file: {e}")

    unique_assets = df_asset_list.drop_duplicates(subset=['Symbol', 'Name'])
    symbol_to_name = dict(zip(unique_assets['Symbol'], unique_assets['Name']))
    symbols = list(symbol_to_name.keys())

    today_date = datetime.now().date()
    end_fetch_date = today_date + timedelta(days=1)
    
    # Determine start date using parquet file if it exists
    if os.path.exists(all_data_parquet_file):
        mod_time = os.path.getmtime(all_data_parquet_file)
        last_mod_date = datetime.fromtimestamp(mod_time).date()
        start_fetch_date = last_mod_date + timedelta(days=1)
        print(f"Assets_Data.parquet last modified on {last_mod_date}.")
    else:
        start_fetch_date = None
        print(f"'Assets_Data.parquet' does not exist. Fetching maximum data.")
    
    # Download asset data using yfinance
    if start_fetch_date:
        if start_fetch_date > today_date:
            print("Assets_Data.parquet is already up-to-date. No new data to fetch.")
            new_data = pd.DataFrame(columns=['Symbol', 'Name', 'Date', 'Open', 'High', 'Low', 'Close'])
        else:
            start_str = start_fetch_date.strftime("%Y-%m-%d")
            end_str = end_fetch_date.strftime("%Y-%m-%d")
            print(f"Fetching data from {start_str} to {end_str} for symbols: {', '.join(symbols)}")
            data = yf.download(symbols, start=start_str, end=end_str, interval="1d",
                               group_by="column", auto_adjust=False)
            new_data = process_downloaded_data(data, symbols, symbol_to_name)
    else:
        print(f"Fetching maximum daily data for symbols: {', '.join(symbols)}")
        data = yf.download(symbols, period="max", interval="1d",
                           group_by="column", auto_adjust=False)
        new_data = process_downloaded_data(data, symbols, symbol_to_name)
    
    # Read old data if exists.
    if os.path.exists(all_data_parquet_file):
        try:
            if TARGET_ASSET:
                print(f"Loading data only for target asset {TARGET_ASSET} from Parquet in chunks")
                dataset = ds.dataset(all_data_parquet_file, format="parquet")
                table = dataset.to_table(filter=(ds.field("Symbol") == TARGET_ASSET))
                all_data = table.to_pandas()
            else:
                all_data = pd.read_parquet(all_data_parquet_file)
            if 'Date' in all_data.columns:
                all_data['Date'] = pd.to_datetime(all_data['Date']).dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error reading existing Parquet file: {e}")
            all_data = pd.DataFrame(columns=['Symbol', 'Name', 'Date', 'Open', 'High', 'Low', 'Close'])
    else:
        all_data = pd.DataFrame(columns=['Symbol', 'Name', 'Date', 'Open', 'High', 'Low', 'Close'])
    
    # Preprocessing: Exclude any assets scheduled for deletion ONLY if the Delete Assets file is non-empty.
    if os.path.exists(delete_file_path):
        try:
            delete_df = pd.read_csv(delete_file_path)
            if not delete_df.empty:
                delete_symbols = set(delete_df["Symbol"].astype(str).str.strip())
                before = len(all_data)
                all_data = all_data[~all_data["Symbol"].isin(delete_symbols)]
                after = len(all_data)
                if before != after:
                    print(f"Excluding {before - after} assets from existing data per Delete Assets file.")
                # After deletion, clear the file to only have headers.
                with open(delete_file_path, "w") as f:
                    f.write("Symbol,Name\n")
            else:
                print("Delete Assets file is empty. Skipping deletion process.")
        except Exception as e:
            print(f"Error processing {delete_file_path}: {e}")
    
    if not new_data.empty:
        combined_data = pd.concat([all_data, new_data], ignore_index=True)
        combined_data.drop_duplicates(subset=['Symbol', 'Date'], keep='last', inplace=True)
        combined_data = combined_data.sort_values(by=['Symbol', 'Date'])
        try:
            combined_data.to_csv(all_data_csv_file, index=False, date_format='%Y-%m-%d')
            print(f"Data saved successfully to CSV at {all_data_csv_file}.")
        except Exception as e:
            print(f"Error saving CSV file: {e}")
        try:
            combined_data.to_parquet(all_data_parquet_file, index=False)
            print(f"Data saved successfully to Parquet at {all_data_parquet_file}.")
        except Exception as e:
            print(f"Error saving Parquet file: {e}")
    else:
        print("No new data was fetched; CSV and Parquet files remain unchanged.")
        combined_data = all_data
    return combined_data

def process_downloaded_data(data, symbols, symbol_to_name):
    data_frames = []
    symbols_no_data = []
    price_cols = ['Open', 'High', 'Low', 'Close']
    if 'data' in locals():
        for symbol in symbols:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if symbol not in data.columns.levels[1]:
                        print(f"No data found for {symbol}.")
                        symbols_no_data.append(symbol)
                        continue
                    df_symbol = data.xs(symbol, axis=1, level=1)
                else:
                    df_symbol = data.copy()
                df_symbol = df_symbol.reset_index()
                if not set(price_cols).issubset(df_symbol.columns):
                    print(f"Missing required price columns for {symbol}.")
                    symbols_no_data.append(symbol)
                    continue
                df_symbol = df_symbol.dropna(subset=price_cols)
                if df_symbol.empty:
                    print(f"All rows for {symbol} were dropped due to missing price data.")
                    symbols_no_data.append(symbol)
                    continue
                df_symbol['Date'] = pd.to_datetime(df_symbol['Date']).dt.strftime('%Y-%m-%d')
                df_symbol[price_cols] = df_symbol[price_cols].round(2)
                df_symbol['Symbol'] = symbol
                df_symbol['Name'] = symbol_to_name.get(symbol, "")
                df_symbol = df_symbol[['Symbol', 'Name', 'Date'] + price_cols]
                data_frames.append(df_symbol)
                print(f"Fetched {len(df_symbol)} valid rows for {symbol}.")
            except Exception as e:
                print(f"Error processing data for {symbol}: {e}")
                symbols_no_data.append(symbol)
        if data_frames:
            return pd.concat(data_frames, ignore_index=True)
    return pd.DataFrame(columns=['Symbol', 'Name', 'Date'] + price_cols)

# ---------------------------
# Calculation Process Functions
# ---------------------------
def calculate_initial_ema(close_series):
    return round(close_series.iloc[:EMA_INIT_INDEX].mean(), 2)

def calculate_ema(current_close, previous_ema):
    return round(current_close * MULTIPLIER + previous_ema * (1 - MULTIPLIER), 2)

def add_tracking_columns(df):
    df = df.copy().reset_index(drop=True)
    df["EMA"] = None
    df["Was above EMA"] = None
    df["Action"] = None
    df["EMA% from Start"] = None
    if len(df) < TRADE_START_INDEX:
        print("Not enough data to compute tracking values.")
        return df
    initial_ema = calculate_initial_ema(df["Close"])
    baseline_value = None
    for i in range(TRADE_START_INDEX, len(df)):
        if i == TRADE_START_INDEX:
            previous_ema = initial_ema
        else:
            previous_ema = float(df.at[i-1, "EMA"]) if df.at[i-1, "EMA"] is not None else initial_ema
        current_close = df.at[i, "Close"]
        new_ema = calculate_ema(current_close, previous_ema)
        df.at[i, "EMA"] = new_ema
        was_above = (current_close > new_ema)
        df.at[i, "Was above EMA"] = was_above
        if i == TRADE_START_INDEX:
            action = "Start Tracking" if not was_above else "Stop Tracking"
        else:
            previous_action = df.at[i-1, "Action"]
            if not was_above and previous_action in ["Start Tracking", "Keep Tracking"]:
                action = "Keep Tracking"
            elif was_above:
                action = "Stop Tracking"
            else:
                action = "Start Tracking"
        df.at[i, "Action"] = action
        if not was_above:
            if action == "Start Tracking":
                baseline_value = new_ema
            ema_pct = round(100 * (df.at[i, "Low"] - baseline_value) / baseline_value, 2) if baseline_value is not None else None
        else:
            ema_pct = None
            baseline_value = None
        df.at[i, "EMA% from Start"] = ema_pct
    return df

def add_reverse_tracking_columns(df):
    # This function is only used for Strategy 0 (sell signals).
    df = df.copy().reset_index(drop=True)
    df["EMA Re"] = None
    df["Was above EMA Re"] = None
    df["Action Re"] = None
    df["EMA% from Start Re"] = None
    if len(df) < TRADE_START_INDEX:
        print("Not enough data to compute reverse tracking values.")
        return df
    initial_ema_re = round(df["Close"].iloc[:TRADE_START_INDEX].mean(), 2)
    baseline_value_re = None
    reverse_tracking = False
    for i in range(TRADE_START_INDEX, len(df)):
        if i == TRADE_START_INDEX:
            previous_ema_re = initial_ema_re
        else:
            previous_ema_re = float(df.at[i-1, "EMA Re"]) if df.at[i-1, "EMA Re"] is not None else initial_ema_re
        current_close = df.at[i, "Close"]
        new_ema_re = round(current_close * MULTIPLIER + previous_ema_re * (1 - MULTIPLIER), 2)
        df.at[i, "EMA Re"] = new_ema_re
        was_above_re = (current_close >= new_ema_re)
        df.at[i, "Was above EMA Re"] = was_above_re
        if not reverse_tracking and was_above_re:
            reverse_tracking = True
            baseline_value_re = new_ema_re
            action_re = "Start Reverse Tracking"
        elif reverse_tracking:
            if was_above_re:
                action_re = "Keep Reverse Tracking"
            else:
                action_re = "Stop Reverse Tracking"
                reverse_tracking = False
                baseline_value_re = None
        else:
            action_re = "No Reverse Tracking"
        df.at[i, "Action Re"] = action_re
        if reverse_tracking and baseline_value_re is not None:
            ema_pct_re = round(100 * (current_close - baseline_value_re) / baseline_value_re, 2)
        else:
            ema_pct_re = None
        df.at[i, "EMA% from Start Re"] = ema_pct_re
    try:
        last_ema_pct_re = df["EMA% from Start Re"].dropna().iloc[-1]
    except IndexError:
        last_ema_pct_re = None
    insert_index = df.columns.get_loc("EMA% from Start Re") + 1
    df.insert(insert_index, "Last EMA% from start Re", last_ema_pct_re)
    print(f"Inserted column 'Last EMA% from start Re' with value: {last_ema_pct_re}")
    return df

def add_last_ema_pct_from_start(df):
    # Use the EMA% from Start value at the last row regardless of whether it's null or not.
    last_value = df["EMA% from Start"].iloc[-1]
    insert_index = df.columns.get_loc("EMA% from Start") + 1
    df.insert(insert_index, "Last EMA% from Start", last_value)
    print(f"Inserted column 'Last EMA% from Start' with value: {last_value}")
    return df

def add_max_drop_columns(df):
    """
    Adds three new columns: "Max drop 30", "Max drop 90", and "Max drop 365".
    For Strategy 1 (buy), the baseline is taken from "Last EMA% from Start".
    For Strategy 0 (sell), the baseline is taken from "Last EMA% from start Re".
    If Strategy is blank, the function will use whichever baseline column is available.
    """
    df = df.copy().reset_index(drop=True)
    if Strategy == 1:
        baseline_col = "Last EMA% from Start"
    elif Strategy == 0:
        baseline_col = "Last EMA% from start Re"
    else:
        if "Last EMA% from start Re" in df.columns:
            baseline_col = "Last EMA% from start Re"
        elif "Last EMA% from Start" in df.columns:
            baseline_col = "Last EMA% from Start"
        else:
            df["Max drop 30"] = [None] * len(df)
            df["Max drop 90"] = [None] * len(df)
            df["Max drop 365"] = [None] * len(df)
            print("No baseline column found for max drop calculations.")
            return df

    pct_col = "EMA% from Start"
    max_drop_30 = [None] * len(df)
    max_drop_90 = [None] * len(df)
    max_drop_365 = [None] * len(df)

    for i in range(len(df)):
        current_pct = df.at[i, pct_col]
        baseline_val = df.at[i, baseline_col] if baseline_col in df.columns else None
        if current_pct is not None and baseline_val is not None and (current_pct <= baseline_val):
            # For 30 rows drop calculation
            end_index_30 = min(i + 30, len(df))
            window_vals_30 = [val for val in df.loc[i+1:end_index_30, pct_col] if val is not None]
            max_drop_30[i] = round(current_pct - min(window_vals_30), 2) if window_vals_30 else None

            # For 90 rows drop calculation
            end_index_90 = min(i + 90, len(df))
            window_vals_90 = [val for val in df.loc[i+1:end_index_90, pct_col] if val is not None]
            max_drop_90[i] = round(current_pct - min(window_vals_90), 2) if window_vals_90 else None

            # For 365 rows drop calculation
            end_index_365 = min(i + 365, len(df))
            window_vals_365 = [val for val in df.loc[i+1:end_index_365, pct_col] if val is not None]
            max_drop_365[i] = round(current_pct - min(window_vals_365), 2) if window_vals_365 else None
        else:
            max_drop_30[i] = None
            max_drop_90[i] = None
            max_drop_365[i] = None

    df["Max drop 30"] = max_drop_30
    df["Max drop 90"] = max_drop_90
    df["Max drop 365"] = max_drop_365
    return df

def add_max_rise_columns(df):
    """
    Calculates the maximum rise in EMA% from Start over the next 30, 90, and 365 rows.
    For Strategy 1 (buy), the baseline is taken from "Last EMA% from Start".
    For Strategy 0 (sell), the baseline is taken from "Last EMA% from start Re".
    If Strategy is blank, the function will use whichever baseline column is available.
    """
    df = df.copy().reset_index(drop=True)
    if Strategy == 1:
        baseline_col = "Last EMA% from Start"
    elif Strategy == 0:
        baseline_col = "Last EMA% from start Re"
    else:
        if "Last EMA% from start Re" in df.columns:
            baseline_col = "Last EMA% from start Re"
        elif "Last EMA% from Start" in df.columns:
            baseline_col = "Last EMA% from Start"
        else:
            df["Max Rise 30"] = [None] * len(df)
            df["Max Rise 90"] = [None] * len(df)
            df["Max Rise 365"] = [None] * len(df)
            print("No baseline column found for max rise calculations.")
            return df

    pct_col = "EMA% from Start"
    max_rise_30 = [None] * len(df)
    max_rise_90 = [None] * len(df)
    max_rise_365 = [None] * len(df)
    
    for i in range(len(df)):
        current_val = df.at[i, pct_col]
        baseline_val = df.at[i, baseline_col] if baseline_col in df.columns else None
        if current_val is not None and baseline_val is not None and (current_val <= baseline_val):
            window_30 = df.loc[i+1 : i+30, pct_col].dropna()
            if not window_30.empty:
                max_future_30 = max(window_30)
                rise_30 = round(max_future_30 - current_val, 2)
                max_rise_30[i] = rise_30 if rise_30 > 0 else 0
            else:
                max_rise_30[i] = None

            window_90 = df.loc[i+1 : i+90, pct_col].dropna()
            if not window_90.empty:
                max_future_90 = max(window_90)
                rise_90 = round(max_future_90 - current_val, 2)
                max_rise_90[i] = rise_90 if rise_90 > 0 else 0
            else:
                max_rise_90[i] = None

            window_365 = df.loc[i+1 : i+365, pct_col].dropna()
            if not window_365.empty:
                max_future_365 = max(window_365)
                rise_365 = round(max_future_365 - current_val, 2)
                max_rise_365[i] = rise_365 if rise_365 > 0 else 0
            else:
                max_rise_365[i] = None
        else:
            max_rise_30[i] = None
            max_rise_90[i] = None
            max_rise_365[i] = None

    df["Max Rise 30"] = max_rise_30
    df["Max Rise 90"] = max_rise_90
    df["Max Rise 365"] = max_rise_365
    return df

def process_data(df):
    # Convert price columns to numeric and sort data
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_values("Date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = add_tracking_columns(df)
    # Early check: For Strategy 1, if the last EMA% from Start is >= -20, skip further processing.
    if Strategy == 1:
        non_null = df["EMA% from Start"].dropna()
        if non_null.empty or non_null.iloc[-1] >= -20:
            print(f"Skipping asset early due to Last EMA% from Start = {non_null.iloc[-1] if not non_null.empty else 'None'}")
            return pd.DataFrame()
        df = add_last_ema_pct_from_start(df)
    else:
        df = add_last_ema_pct_from_start(df)
    if Strategy == 0 or Strategy == "":
        df = add_reverse_tracking_columns(df)
    df = add_max_drop_columns(df)
    df = add_max_rise_columns(df)
    return df

def process_asset_group(asset_df, delete_file_path):
    asset_df = asset_df.sort_values("Date").reset_index(drop=True)
    processed_data = process_data(asset_df)
    
    if processed_data.empty:
        return processed_data

    # Retrieve the last row of the processed data
    last_row = processed_data.iloc[-1]
    last_ema_start = last_row.get("Last EMA% from Start")
    last_ema_start_re = last_row.get("Last EMA% from start Re")
    symbol = last_row.get("Symbol", "Unknown")
    asset_name = processed_data.iloc[0].get("Name", symbol)
    
    # Check deletion criteria:
    # If Last EMA% from Start <= -95 OR Last EMA% from start Re >= 95, add to Delete Assets file and exclude asset.
    if (last_ema_start is not None and last_ema_start <= -95) or (last_ema_start_re is not None and last_ema_start_re >= 95):
        print(f"Asset {symbol} meets deletion criteria. Excluding it from processing.")
        add_asset_to_delete_file(symbol, asset_name, delete_file_path)
        return pd.DataFrame()
    
    # Apply other strategy filtering (redundant now as early check is done in process_data)
    if Strategy == 1:
        if last_ema_start is None or last_ema_start >= -10:
            print(f"Skipping asset {symbol} due to Last EMA% from Start = {last_ema_start}")
            return pd.DataFrame()
    elif Strategy == 0:
        if last_ema_start_re is None or last_ema_start_re <= 20:
            print(f"Skipping asset {symbol} due to Last EMA% from start Re = {last_ema_start_re}")
            return pd.DataFrame()
    else:
        if last_ema_start is not None and last_ema_start >= -10:
            print(f"Skipping asset {symbol} due to Last EMA% from Start = {last_ema_start}")
            return pd.DataFrame()
        if last_ema_start_re is not None and last_ema_start_re <= 10:
            print(f"Skipping asset {symbol} due to Last EMA% from start Re = {last_ema_start_re}")
            return pd.DataFrame()
    
    print(f"Asset {symbol} successfully added to the DataFrame.")
    return processed_data

def process_asset_group_wrapper(group):
    # Wrapper function to allow ProcessPoolExecutor to pickle the function.
    # It uses the global DELETE_ASSETS_FILE.
    return process_asset_group(group, DELETE_ASSETS_FILE)

def create_report_sheet(processed_df):
    # Updated report sheet with Baseline Column (Column C) and conditional % Above/% Below calculation
    if processed_df.empty or "Symbol" not in processed_df.columns:
        print("No processed data available for report.")
        return pd.DataFrame(columns=[
            "Name", 
            "Symbol", 
            "Baseline",  # New Baseline column
            "% Above", 
            "% Below",
            "Drop 30", 
            "Drop 90", 
            "Drop 365", 
            "Rise 30", 
            "Rise 90", 
            "Rise 365"
        ])
    
    report_rows = []
    grouped = processed_df.groupby("Symbol")
    for symbol, group in grouped:
        group = group.sort_values("Date").reset_index(drop=True)
        # Determine asset name from first row
        asset_name = group.iloc[0]["Name"] if "Name" in group.columns else symbol

        # Determine Baseline based on the strategy
        if Strategy == 1:
            # Strategy 1 (Buy signals): Use "Last EMA% from Start"
            baseline_value = group["Last EMA% from Start"].dropna().iloc[-1] if "Last EMA% from Start" in group.columns else None
        elif Strategy == 0:
            # Strategy 0 (Sell signals): Use "Last EMA% from start Re"
            baseline_value = group["Last EMA% from start Re"].dropna().iloc[-1] if "Last EMA% from start Re" in group.columns else None
        else:
            # No specific strategy: Use whichever is available
            baseline_value = group["Last EMA% from Start"].dropna().iloc[-1] if "Last EMA% from Start" in group.columns else None
            if baseline_value is None and "Last EMA% from start Re" in group.columns:
                baseline_value = group["Last EMA% from start Re"].dropna().iloc[-1]

        # If no valid baseline, skip this asset
        if baseline_value is None:
            print(f"Skipping asset {symbol} due to missing baseline value.")
            continue

        # Round the baseline value to 2 decimal places
        baseline_value = round(baseline_value, 2)

        # Conditionally calculate % Above and % Below based on the strategy
        if Strategy == 1:
            # Strategy 1: Only calculate % Below
            pct_above = None
            pct_below = f'=COUNTIF(INDIRECT("\'{symbol}\'!K:K"),"<="&{baseline_value})'
        elif Strategy == 0:
            # Strategy 0: Only calculate % Above
            pct_above = f'=COUNTIF(INDIRECT("\'{symbol}\'!K:K"),">="&{baseline_value})'
            pct_below = None
        else:
            # No specific strategy: Calculate both
            pct_above = f'=COUNTIF(INDIRECT("\'{symbol}\'!K:K"),">="&{baseline_value})'
            pct_below = f'=COUNTIF(INDIRECT("\'{symbol}\'!K:K"),"<="&{baseline_value})'
        
        # Updated formulas: denominator becomes (INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))
        past_drop_30  = f'=ROUND(COUNTIF(INDIRECT("\'{symbol}\'!M:M"),">="&{DROP_TARGET})/(INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))*100, 2)'
        past_drop_90  = f'=ROUND(COUNTIF(INDIRECT("\'{symbol}\'!N:N"),">="&{DROP_TARGET})/(INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))*100, 2)'
        past_drop_365 = f'=ROUND(COUNTIF(INDIRECT("\'{symbol}\'!O:O"),">="&{DROP_TARGET})/(INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))*100, 2)'
        past_rise_30  = f'=ROUND(COUNTIF(INDIRECT("\'{symbol}\'!P:P"),">="&{RISE_TARGET})/(INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))*100, 2)'
        past_rise_90  = f'=ROUND(COUNTIF(INDIRECT("\'{symbol}\'!Q:Q"),">="&{RISE_TARGET})/(INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))*100, 2)'
        past_rise_365 = f'=ROUND(COUNTIF(INDIRECT("\'{symbol}\'!R:R"),">="&{RISE_TARGET})/(INDIRECT("D"&ROW())+INDIRECT("E"&ROW()))*100, 2)'
        
        # Create hyperlinks:
        # "Name" links to the asset's sheet (assumed to be named as the symbol)
        name_link = f'=HYPERLINK("#\'{symbol}\'!A1","{asset_name}")'
        # "Symbol" links to Yahoo Finance chart page for the symbol
        symbol_link = f'=HYPERLINK("https://finance.yahoo.com/chart/{symbol}","{symbol}")'
        
        report_rows.append({
            "Name": name_link,
            "Symbol": symbol_link,
            "Baseline": baseline_value,  # Add Baseline column
            "% Above": pct_above,
            "% Below": pct_below,
            "Drop 30": past_drop_30,
            "Drop 90": past_drop_90,
            "Drop 365": past_drop_365,
            "Rise 30": past_rise_30,
            "Rise 90": past_rise_90,
            "Rise 365": past_rise_365
        })
    report_df = pd.DataFrame(report_rows, columns=[
        "Name", 
        "Symbol", 
        "Baseline",  # New Baseline column
        "% Above", 
        "% Below",
        "Drop 30", 
        "Drop 90", 
        "Drop 365", 
        "Rise 30", 
        "Rise 90", 
        "Rise 365"
    ])
    return report_df

def main():
    global DELETE_ASSETS_FILE
    script_dir = os.path.dirname(os.path.abspath(__file__))
    asset_list_path = os.path.join(script_dir, "Asset List.txt")
    all_data_csv_file = os.path.join(script_dir, "Assets_Data.csv")
    all_data_parquet_file = os.path.join(script_dir, "Assets_Data.parquet")
    processed_excel_file = os.path.join(script_dir, "Assets_Processed.xlsx")
    DELETE_ASSETS_FILE = os.path.join(script_dir, "Delete Assets.txt")
    
    # Preprocessing: If Delete Assets file exists, load it and remove matching rows from existing CSV, Parquet.
    # Note: This deletion from Asset List was handled in download_asset_data.
    if os.path.exists(DELETE_ASSETS_FILE):
        try:
            delete_df = pd.read_csv(DELETE_ASSETS_FILE)
            delete_symbols = set(delete_df["Symbol"].astype(str).str.strip())
            if os.path.exists(all_data_csv_file):
                csv_df = pd.read_csv(all_data_csv_file)
                before = len(csv_df)
                csv_df = csv_df[~csv_df["Symbol"].isin(delete_symbols)]
                csv_df.to_csv(all_data_csv_file, index=False)
                after = len(csv_df)
                if before != after:
                    print(f"Removed {before - after} rows from CSV per Delete Assets file.")
            if os.path.exists(all_data_parquet_file):
                parquet_df = pd.read_parquet(all_data_parquet_file)
                before = len(parquet_df)
                parquet_df = parquet_df[~parquet_df["Symbol"].isin(delete_symbols)]
                parquet_df.to_parquet(all_data_parquet_file, index=False)
                after = len(parquet_df)
                if before != after:
                    print(f"Removed {before - after} rows from Parquet per Delete Assets file.")
        except Exception as e:
            print(f"Error during preprocessing deletion: {e}")
    
    combined_data = download_asset_data(asset_list_path, all_data_csv_file, all_data_parquet_file, DELETE_ASSETS_FILE)
    if combined_data.empty:
        print("No data available to process.")
        return

    # If TARGET_ASSET is provided, ensure we have data for it.
    if TARGET_ASSET:
        if TARGET_ASSET not in combined_data['Symbol'].unique():
            print(f"Asset {TARGET_ASSET} does not exist.")
            return
        else:
            print(f"Processing only asset {TARGET_ASSET}.")
            combined_data = combined_data[combined_data['Symbol'] == TARGET_ASSET]

    groups = [group for _, group in combined_data.groupby("Symbol")]
    with ProcessPoolExecutor(max_workers=4) as executor:
        processed_groups = list(executor.map(process_asset_group_wrapper, groups))
    non_empty_groups = [group for group in processed_groups if not group.dropna(how="all").empty]
    if not non_empty_groups:
        print("No processed asset groups to create report.")
        return
    processed_df = pd.concat(non_empty_groups, ignore_index=True)
    
    # If targeting a single asset, display relevant columns
    if TARGET_ASSET:
        target_df = processed_df[processed_df['Symbol'] == TARGET_ASSET]
        if not target_df.empty:
            print("\nTimestamp, EMA, Action, and EMA% from Start:")
            print(target_df[["Date", "EMA", "Action", "EMA% from Start"]].to_string(index=False))
    
    report_df = create_report_sheet(processed_df)
    
    with pd.ExcelWriter(processed_excel_file, engine="xlsxwriter") as writer:
        report_df.to_excel(writer, sheet_name="Report", index=False)
        # Freeze the top row in the Report sheet.
        worksheet = writer.sheets["Report"]
        worksheet.freeze_panes(1, 0)
        for symbol, group in processed_df.groupby("Symbol"):
            group.to_excel(writer, sheet_name=symbol, index=False)
    print(f"\nProcessed Excel file with Report created at: {processed_excel_file}")
