# =============================================================================
# IMPORTS
# =============================================================================
import pandas as pd
import ccxt
import datetime as dt
import sys, os, time

sys.path.append(os.path.abspath("./utils"))
from utils.pprint_helper import pprint

# =============================================================================
# CONSTANTS
# =============================================================================
LIMIT = {"1m": 1440, "5m": 288, "1h": 24}
MS_PER_DAY = 24 * 60 * 60 * 1000
PATH = "/Users/julian/Documents/exchange_arbitrage/data.csv"

# =============================================================================
# MAIN
# =============================================================================
def main(exchanges, symbol, timeframe, day):
    since = determine_ms_timestamp(day)
    limit = determine_limit(timeframe)
    until = determine_until(since)
    combined = None
    for exchange_str in exchanges:
        exchange = instantiate_exchange(exchange_str)
        ohlcv = fetch_from_exchange(exchange, symbol, timeframe, since, until, limit)
        df = process_ohlcv_to_df(exchange_str, ohlcv)
        combined = combine_dataframes(combined, exchange_str, df)

    combined = add_stats_to_combined_df(combined)
    combined = format_df_for_csv(combined)
    combined.to_csv(PATH)


# =============================================================================
# FETCH DATA FROM SPECIFIC EXCHANGE
# =============================================================================
def fetch_from_exchange(exchange, symbol, timeframe, since, until, limit):
    ohlcv = []
    while since < until and limit > 0:
        print(f"Sleeping for {exchange.rateLimit}ms")
        time.sleep(exchange.rateLimit / 1000)
        res = exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, since=since, limit=limit
        )
        ohlcv += res
        since = res[-1][0] + int(MS_PER_DAY / LIMIT[timeframe])  # update since + 1 unit
        limit -= len(res)
    return ohlcv


# =============================================================================
# Determine `since` timestamp in milliseconds from datetime string
# =============================================================================
def determine_ms_timestamp(day):
    determine_validity_of_day_input(day)
    obj = dt.datetime.strptime(day, "%Y-%m-%d")
    return int(obj.replace(tzinfo=dt.timezone.utc).timestamp()) * 1000


# =============================================================================
# Determine validity of day string input
# =============================================================================
def determine_validity_of_day_input(day):
    if len(day) != 10:
        raise ValueError("Invalid date input. Should be `YYYY-MM-DD`")
    for idx, char in enumerate(day):
        if idx == 4 or idx == 7:
            if char != "-":
                raise ValueError("Invalid date input. Should be `YYYY-MM-DD`")
        else:
            try:
                int(char)
            except:
                raise ValueError("Invalid date input. Should be `YYYY-MM-DD`")


# =============================================================================
# Determine how many candles we fetch for day
# =============================================================================
def determine_limit(timeframe):
    if timeframe not in LIMIT:
        raise ValueError(
            f"Need to specific a valid candle size. {timeframe} is NOT valid yet."
        )
    return LIMIT[timeframe]


# =============================================================================
# Determine the last timestamp of desired data
# =============================================================================
def determine_until(since):
    return since + (24 * 60 * 80 * 1000)


# =============================================================================
# Instantiate exchange obj from str variable
# =============================================================================
def instantiate_exchange(exchange):
    if exchange not in ccxt.exchanges:
        raise ValueError(f"Exchange `{exchange}` not found in CCXT. Check spelling.")

    exchange = getattr(ccxt, exchange)
    exchange.enableRateLimit = True  # enable

    if not exchange.has["fetchOHLCV"]:
        raise ValueError("Exchange does not have fetch_ohlcv functionality with CCXT.")
    return exchange()


# =============================================================================
# Convert close prices to df
# =============================================================================
def process_ohlcv_to_df(exchange_str, ohlcv):
    closes = [[c[0], c[4]] for c in ohlcv]
    df = pd.DataFrame(closes)
    df.columns = ["unix", exchange_str]
    df = df.set_index("unix")
    return df


# =============================================================================
# Create a combined df
# =============================================================================
def combine_dataframes(combined, exchange_str, df):
    if combined is None:
        return df
    combined[exchange_str] = df[exchange_str]
    return combined


# =============================================================================
# Add stats like max, min, median, mean to df
# =============================================================================
def add_stats_to_combined_df(combined):
    columns = list(combined.columns)
    combined["max"] = combined[columns].max(axis=1)
    combined["min"] = combined[columns].min(axis=1)
    combined["mean"] = combined[columns].mean(axis=1)
    combined["median"] = combined[columns].median(axis=1)
    return combined


# =============================================================================
# Add stats like max, min, median, mean to df
# =============================================================================
def format_df_for_csv(combined):
    datetimes = pd.to_datetime(combined.index, unit="ms")
    combined.insert(0, "timestamps", datetimes)
    combined = combined.reset_index()
    return combined


if __name__ == "__main__":
    print("Check if close index is correct.")
    main(["binance", "kucoin", "ftx"], "ETH/USDT", "1m", "2022-09-14")
    print("Check if close index is correct.")


#         datetimes = pd.to_datetime(df["unix"], unit="ms")
