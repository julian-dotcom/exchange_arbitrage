# =============================================================================
# IMPORTS
# =============================================================================
import enum
import pandas as pd
import datetime as dt
import sys, os, time
import glob
from pprint import pprint

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
FILES = glob.glob(os.path.join(INPUT_PATH, "*.csv"))

# =============================================================================
# MAIN
# =============================================================================
def main():
    original_data = get_data_from_csv()
    unique_pairs = create_unique_exchange_pairs(original_data)

    merged_obj = structure_data_by_column(original_data)
    diff_obj = compute_differences(unique_pairs, merged_obj)
    for column, df in diff_obj.items():
        # df.round(3).to_csv(f"{OUTPUT_PATH}/minute_data_{column}.csv")
        stats = df[unique_pairs].describe()
        # stats.round(3).to_csv(f"{OUTPUT_PATH}/minute_stats_{column}.csv")

    daily_stats = compute_daily_stats(unique_pairs, diff_obj)
    for col, val in daily_stats.items():
        for pair, df in val.items():
            df.round(3).to_csv(f"{OUTPUT_PATH}/daily_{col}_{pair}.csv")


# =============================================================================
# Get data from csvs
# =============================================================================
def get_data_from_csv():
    original_data = {}
    for file in FILES:
        exchange = file.split("/")[-1].split(".")[0].split("-")[-1]
        df = pd.read_csv(file)
        original_data[exchange] = df
    return original_data


# =============================================================================
# Create all unique exchange pairs
# =============================================================================
def create_unique_exchange_pairs(original_data):
    exchanges = list(original_data.keys())
    i = 0
    pairs = []
    for i, ex in enumerate(exchanges):
        j = i + 1
        if j == len(exchanges):
            break
        for ex2 in exchanges[j:]:
            pairs.append(f"{ex}-{ex2}")
    return pairs


# =============================================================================
# Create dataframes for bid, ask, mid
# =============================================================================
def structure_data_by_column(original_data):
    merged_obj = {}
    columns = ["bid", "ask", "mid"]
    for column in columns:
        merged_obj[column] = structure_data_by_exchange(original_data, column)
    return merged_obj


# =============================================================================
# Create dataframes for bid, ask, mid
# =============================================================================
def structure_data_by_exchange(original_data, column):
    merged = None
    for exchange, df in original_data.items():
        prices = df[["timestamp", column]]
        prices = prices.rename(columns={column: exchange})
        if merged is None:
            merged = prices
        else:
            merged = pd.merge(merged, prices, on="timestamp", how="inner")
    merged = merged.set_index("timestamp")
    return merged


# =============================================================================
# Compute the differences for all prices
# =============================================================================
def compute_differences(unique_pairs, merged_obj):
    diff_obj = {}
    for column, df in merged_obj.items():
        diff_obj[column] = compute_differences_between_exchanges(unique_pairs, df)
    return diff_obj


# =============================================================================
# Compute the differences between exchanges
# =============================================================================
def compute_differences_between_exchanges(unique_pairs, df):
    for pair in unique_pairs:
        ex1, ex2 = pair.split("-")
        df[pair] = abs(df[ex1] - df[ex2])
    return df


# =============================================================================
# Compute daily stats on dfs
# =============================================================================
def compute_daily_stats(unique_pairs, diff_obj):
    daily_stats = {}
    for column, df in diff_obj.items():
        daily_stats[column] = compute_daily_stats_for_df(unique_pairs, df)
    return daily_stats


# =============================================================================
# Compute daily stats on df
# =============================================================================
def compute_daily_stats_for_df(unique_pairs, df):
    day_stats = {}
    df.index = pd.to_datetime(df.index)
    for pair in unique_pairs:
        p_data = df[pair]
        day_stats[pair] = compute_daily_stats_for_pair(p_data)
    return day_stats


# =============================================================================
# Compute daily stats on row
# =============================================================================
def compute_daily_stats_for_pair(p_data):
    day_stats = {}
    tracker = None
    _max = p_data.resample("D").max()
    _min = p_data.resample("D").min()
    mean = p_data.resample("D").mean()
    stats = _max.to_frame(name="max")
    stats["min"] = _min
    stats["mean"] = mean
    return stats


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
