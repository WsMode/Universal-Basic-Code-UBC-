Below is a detailed description of the script, its components, and how it operates:

---

## Overview

This Python script is an asset tracking and analysis tool designed to fetch historical asset data from Yahoo Finance, compute technical indicators (like the Exponential Moving Average or EMA), and analyze price movements. By applying customizable strategies and thresholds, the tool generates a comprehensive Excel report that includes summary insights and asset-specific details. The script is part of the WsMode Universal Basic Code (UBC) project and is intended to help users evaluate potential low-risk, high-success opportunities for assets such as ETFs, cryptocurrencies, real estate, commodities, and products.

---

## Detailed Breakdown

### 1. Configurable Parameters

At the beginning of the script, several global variables are defined to control its behavior:

- **Trade Thresholds and Targets**:  
  - `DROP_TARGET` and `RISE_TARGET` define the percentage thresholds for considering a drop or rise in price.  
  - `PROFIT_TARGET` and `INVESTMENT_CAPITAL` outline target profit and the capital to be invested.
  
- **EMA and Trade Indices**:  
  - `EMA_INIT_INDEX` is used to compute an initial EMA value over a specified number of rows (371 in this case).  
  - `TRADE_START_INDEX` indicates the point in the data from which further computed columns and tracking begin.  
  - The `MULTIPLIER` for the EMA is computed from `EMA_INIT_INDEX`.

- **Strategy Setting**:  
  - The `Strategy` variable distinguishes between buy signals (Strategy = 1), sell signals (Strategy = 0), or processing all assets when left blank.

- **Asset Targeting**:  
  - `TARGET_ASSET` allows processing of a single asset if specified or all assets if left blank.

- **File Path for Deletion List**:  
  - A global variable `DELETE_ASSETS_FILE` stores the file path used for marking assets that meet deletion criteria.

---

### 2. Utility Functions for File Management

#### Asset Deletion
- **`add_asset_to_delete_file(symbol, name, delete_file_path)`**:  
  This function handles adding an asset to a deletion list:
  - It reads an existing "delete file" (if present) and prevents duplicate entries.
  - If the asset isn't already marked, it appends the asset symbol and its name.
  - This mechanism helps in excluding assets that have met specific criteria during the analysis (for example, those that have declined too far or reversed too strongly).

#### File Input/Output and Data Downloading
- **`download_asset_data(asset_list_path, all_data_csv_file, all_data_parquet_file, delete_file_path)`**:  
  - Reads an asset list from a CSV (typically named "Asset List.txt").
  - Optionally filters out any assets already marked in the deletion file.
  - Determines the date range for the data fetch:
    - If an existing Parquet file is present, it fetches only new data since the last update.
    - Otherwise, it downloads the maximum available daily data.
  - It uses **yfinance** to download daily historical data and then processes this data via the helper function `process_downloaded_data`.

- **`process_downloaded_data(data, symbols, symbol_to_name)`**:  
  - Converts the raw data fetched from yfinance into a structured DataFrame.
  - Ensures that each asset has the required price columns, drops rows with missing price data, and reformats the date.
  - Each symbol’s data is augmented with its corresponding name and then standardized to contain columns such as: Symbol, Name, Date, Open, High, Low, and Close.

After data processing, the updated DataFrame is saved as both CSV and Parquet files. This not only preserves an archive of asset data but also supports incremental updates.

---

### 3. Data Analysis and Indicator Calculation

The heart of the script is in computing EMAs and tracking subsequent price movements:

#### EMA Calculation and Tracking
- **`calculate_initial_ema(close_series)`**:  
  Computes the mean of the first `EMA_INIT_INDEX` closing prices. This value serves as the starting EMA for further calculations.

- **`calculate_ema(current_close, previous_ema)`**:  
  Calculates the new EMA based on the current closing price and the previous EMA using an exponential smoothing formula.

- **`add_tracking_columns(df)`**:  
  - Processes an asset’s time series data (passed as a DataFrame) and computes the following:
    - **EMA**: The exponential moving average for each applicable row.
    - **Was above EMA**: Boolean indicator showing if the price was above the newly computed EMA.
    - **Action**: A column that flags whether to “Start Tracking,” “Keep Tracking,” or “Stop Tracking” based on the price relative to the EMA.
    - **EMA% from Start**: Percentage deviation from a baseline EMA value (established when tracking begins).  
  - The baseline is reset whenever tracking starts, ensuring that the computation measures relative movement from key turnaround points.

#### Reverse Tracking (for Sell Signals)
- **`add_reverse_tracking_columns(df)`**:
  - Similar in structure to the standard tracking, this function calculates a reverse EMA, intended for use with sell signals (Strategy 0).
  - It computes additional columns:
    - **EMA Re, Was above EMA Re, Action Re, EMA% from Start Re**.
  - A unique column, **Last EMA% from start Re**, is inserted to capture the most recent percentage deviation based on reverse tracking.

#### Finalizing EMA Percentage for Reporting
- **`add_last_ema_pct_from_start(df)`**:
  - This function takes the last computed EMA percentage (from the “EMA% from Start” column) and appends it to the DataFrame in a new column, “Last EMA% from Start.”

---

### 4. Performance Metrics: Max Drop and Max Rise Calculations

To offer deeper insights into the asset's historical performance, the script computes:
  
- **`add_max_drop_columns(df)`**:
  - Calculates the maximum percentage drop from the current EMA percentage over three different time windows: 30, 90, and 365 rows.
  - The baseline for these calculations depends on the chosen strategy (using either the standard or reverse EMA percentage).

- **`add_max_rise_columns(df)`**:
  - Similar to the drop calculations, this function computes the maximum percentage rise from the current EMA percentage for the same time windows.
  - These values are useful for assessing potential recovery strength or profit opportunities.

Both functions iterate over the DataFrame rows and look ahead to determine the worst (or best) percentage change within the respective window.

---

### 5. Data Processing Workflow

#### Processing Individual Assets
- **`process_data(df)`**:
  - Cleans the data and ensures price columns are numeric.
  - Applies the tracking functions to compute EMA-based columns.
  - Checks early thresholds to quickly exclude data that does not meet the strategy criteria.
  - Calls functions to add last EMA percentage, reverse tracking columns (if needed), and max drop/rise columns.

- **`process_asset_group(asset_df, delete_file_path)`**:
  - This function processes data for a single asset group:
    - It orders the data by date.
    - It applies `process_data()` and then checks deletion criteria (e.g., if the last EMA percentage is too low or too high).
    - If the asset meets deletion criteria, it is added to the delete file and excluded from further analysis.
  - A wrapper (`process_asset_group_wrapper`) is provided to support parallel processing.

#### Parallel Processing of Multiple Assets
- Using Python’s **`ProcessPoolExecutor`**, asset groups are processed in parallel. This greatly speeds up the analysis when dealing with many assets.

---

### 6. Generating the Excel Report

- **`create_report_sheet(processed_df)`**:
  - Aggregates the processed data into a summary report.
  - Groups data by asset symbols and constructs rows containing:
    - Asset name and symbol (with hyperlinks to additional details and external sources such as Yahoo Finance).
    - A computed baseline value (last EMA percentage) for the asset.
    - Dynamically generated Excel formulas that compute the percentage of days above or below the baseline, and percentage changes (drop and rise over specified windows).

- **Excel Report Generation** (within `main()`):
  - The report DataFrame and individual asset sheets are written to an Excel workbook using **XlsxWriter**.
  - For usability, sheets are named according to asset symbols, and the main "Report" sheet includes frozen panes to keep headers visible.

---

### 7. Orchestrating the Workflow with `main()`

The `main()` function acts as the script’s entry point and coordinates the entire process:

1. **Setup**:
   - Determines file paths for the asset list, the deletion file, CSV data, Parquet data, and the output Excel report.
   - Reads and preprocesses the deletion file, removing assets from the existing dataset as necessary.

2. **Data Acquisition**:
   - Calls `download_asset_data()` to fetch new data and combine it with historical data.
   - Applies filtering based on `TARGET_ASSET` if specified.

3. **Asset Processing**:
   - Groups the combined data by asset symbol.
   - Processes each group in parallel via `ProcessPoolExecutor` using the wrapper function.

4. **Report Creation and Output**:
   - Concatenates processed data into a single DataFrame.
   - Generates a detailed report sheet along with individual asset sheets.
   - Saves the entire output into an Excel workbook (`Assets_Processed.xlsx`).

5. **User Feedback**:
   - Throughout the process, the script prints status messages to track progress and highlight any issues (such as missing data, assets skipped due to criteria, and successful file outputs).

---

## Conclusion

This script is a comprehensive asset tracking tool that automates the collection, processing, and analysis of historical financial data. By integrating technical analysis (through EMAs and trend tracking) with practical file management and parallel processing, it provides users with actionable reports to help drive informed trading or investment decisions. The modular design—dividing tasks into data fetching, processing, metric computation, and reporting—ensures that the code is both flexible and scalable for different asset classes and analysis strategies.

--- 

This detailed description should help you understand the inner workings of the script and how each component contributes to its overall functionality.
