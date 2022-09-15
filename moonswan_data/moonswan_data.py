# =============================================================================
# IMPORTS
# =============================================================================
import pandas as pd
import datetime as dt
import sys, os, time
import glob

# sys.path.append(os.path.abspath("./utils"))
# from utils.pprint_helper import pprint

# =============================================================================
# CONSTANTS
# =============================================================================
LIMIT = {"1m": 1440, "5m": 288, "1h": 24}
MS_PER_DAY = 24 * 60 * 60 * 1000
INPUT_PATH = "/Users/julian/Documents/exchange_arbitrage/moonswan_data/original_data"
OUTPUT_PATH = "/Users/julian/Documents/exchange_arbitrage/moonswan_data/processed_data"

# =============================================================================
# FILES
# =============================================================================
DOWNTIME_FILES = glob.glob(os.path.join(INPUT_PATH, "*.csv"))

# =============================================================================
# MAIN
# =============================================================================
def main():
    prices_obj = get_data_from_csv()
    merged_obj = structure_data_by_column(prices_obj)
    merged_obj = compute_stats_on_data(merged_obj)
    for column, df in merged_obj.items():
        df.round(2).to_csv(f"{OUTPUT_PATH}/minute_{column}.csv")
    daily_obj = process_all_to_daily_data(merged_obj)
    for column, df in daily_obj.items():
        df.round(2).to_csv(f"{OUTPUT_PATH}/daily_{column}.csv")


# =============================================================================
# Get data from csvs
# =============================================================================
def get_data_from_csv():
    data_obj = {}
    for file in DOWNTIME_FILES:
        exchange = file.split("/")[-1].split(".")[0].split("-")[-1]
        df = pd.read_csv(file)
        data_obj[exchange] = df
    return data_obj


# =============================================================================
# Create dataframes for bid, ask, mid
# =============================================================================
def structure_data_by_column(prices_obj):
    merged_obj = {"bid": None, "ask": None, "mid": None}
    for column, merged in merged_obj.items():
        merged = structure_data_by_exchange(prices_obj, column, merged)
        merged_obj[column] = merged
    return merged_obj


# =============================================================================
# Create dataframes for bid, ask, mid
# =============================================================================
def structure_data_by_exchange(prices_obj, column, merged):
    for exchange, prices in prices_obj.items():
        prices = prices.rename(columns={column: exchange})

        if merged is None:
            merged = prices[["timestamp", exchange]]
        else:
            merged = pd.merge(
                merged, prices[["timestamp", exchange]], on="timestamp", how="inner"
            )
    merged = merged.set_index("timestamp")
    return merged


# =============================================================================
# Iterate over obj and compute stats like mean, median, min, max
# =============================================================================
def compute_stats_on_data(merged_obj):
    for column, df in merged_obj.items():
        columns = list(df.columns)
        df["max"] = df[columns].max(axis=1)
        df["min"] = df[columns].min(axis=1)
        df["mean"] = df[columns].mean(axis=1)
        df["median"] = df[columns].median(axis=1)
        merged_obj[column] = df
    return merged_obj


# =============================================================================
# Iterate over obj and summarize to daily data
# =============================================================================
def process_all_to_daily_data(merged_obj):
    daily_obj = {}
    for col, df in merged_obj.items():
        df = process_df_to_start_n_end_midnight(df)
        df.index = pd.to_datetime(df.index)
        df = df.resample("D").mean()
        daily_obj[col] = df
    return daily_obj


# =============================================================================
# Summarize to daily data
# =============================================================================
def process_df_to_start_n_end_midnight(df):
    start, end = None, None
    for idx, row in df.iterrows():
        time_str = idx.split(" ")[-1]
        if time_str != "00:00:00":
            continue
        if start is None:
            start = idx
        if end is None or end < idx:
            end = idx
    df = df.loc[start:end][:-1]
    return df


if __name__ == "__main__":
    main()


# df = df.rename(
#     columns={
#         "bid": f"{exchange}-bid",
#         "ask": f"{exchange}-ask",
#         "mid": f"{exchange}-mid",
#     }
# )
