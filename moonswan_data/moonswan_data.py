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
PATH = "/Users/julian/Documents/exchange_arbitrage/moonswan_data/data"

# =============================================================================
# FILES
# =============================================================================
DOWNTIME_FILES = glob.glob(os.path.join(PATH, "*.csv"))

# =============================================================================
# MAIN
# =============================================================================
def main():
    prices_obj = get_data_from_csv()
    merged_obj = structure_data_by_column(prices_obj)
    merged_obj = compute_stats_on_data(merged_obj)


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
    combined_obj = {"bid": None, "ask": None, "mid": None}
    for column, combined in combined_obj.items():
        combined = structure_data_by_exchange(prices_obj, column, combined)
        combined_obj[column] = combined


# =============================================================================
# Create dataframes for bid, ask, mid
# =============================================================================
def structure_data_by_exchange(prices_obj, column, combined):
    for exchange, prices in prices_obj.items():
        prices = prices.rename(columns={column: exchange})

        if combined is None:
            combined = prices[["timestamp", exchange]]
        else:
            combined = pd.merge(
                combined, prices[["timestamp", exchange]], on="timestamp", how="inner"
            )
    return combined


# =============================================================================
# Iterate over obj and compute stats like mean, median, min, max
# =============================================================================
def compute_stats_on_data(merged_obj):
    for df in merged_obj.values():
        df["max"] = df.max(axis=1)
        df["min"] = df.min(axis=1)
        df["mean"] = df.mean(axis=1)
        df["median"] = df.median(axis=1)
        print(df)


if __name__ == "__main__":
    main()


# df = df.rename(
#     columns={
#         "bid": f"{exchange}-bid",
#         "ask": f"{exchange}-ask",
#         "mid": f"{exchange}-mid",
#     }
# )
