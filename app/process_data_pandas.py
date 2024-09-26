from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import s3fs

import time

load_dotenv()

fs = s3fs.S3FileSystem()

BUCKET_NAME = os.getenv("BUCKET_NAME", "<bucket_name>")
STOCK_FILENAME = os.getenv("stock_prices_filename", "stock_prices_10M.csv")
VOLUME_FILENAME = os.getenv("trading_volume_filename", "trading_volume_10M.csv")
FINANCIAL_FILENAME_ENRICHED = os.getenv("financial_filename_enriched", "stock_trading_volume_10M.parquet")

print(BUCKET_NAME)
print(STOCK_FILENAME)
print(VOLUME_FILENAME)
print(FINANCIAL_FILENAME_ENRICHED)

start_date = time.time()

s3_file_path = f's3://{BUCKET_NAME}/financial_data/{STOCK_FILENAME}'
df_prices = pd.read_csv(s3_file_path)

end_date = time.time()

print(f"Time to load stock prices: {end_date - start_date}")


start_date = time.time()

s3_file_path = f's3://{BUCKET_NAME}/financial_data/{VOLUME_FILENAME}'
df_volume = pd.read_csv(s3_file_path)

end_date = time.time()

print(f"Time to load trading volume: {end_date - start_date}")


s3_file_path_financial_enriched = f's3://{BUCKET_NAME}/financial_data_output/{FINANCIAL_FILENAME_ENRICHED}.parquet'


try:

    flag_start = time.time()

    start_date = pd.to_datetime("2023-07-01").tz_localize('UTC')
    end_date = pd.to_datetime("2023-08-01").tz_localize('UTC')

    # Convert columns to appropriate data types with UTC timezone
    df_volume['date'] = pd.to_datetime(df_volume['date'], utc=True)
    df_volume['symbol'] = df_volume['symbol'].astype('category')
    df_prices['date'] = pd.to_datetime(df_prices['date'], utc=True)
    df_prices['symbol'] = df_prices['symbol'].astype('category')

    # Filter data by date range
    df_volume_filtered = df_volume[(df_volume['date'] >= start_date) & (df_volume['date'] <= end_date)]
    df_prices_filtered = df_prices[(df_prices['date'] >= start_date) & (df_prices['date'] <= end_date)]

    # Perform the inner join on date and symbol
    df_joined = pd.merge(df_prices_filtered, df_volume_filtered, on=["date", "symbol"], how="inner")

    df_joined['trading_value'] = df_joined['close_price'] * df_joined['volume']

    # Group by symbol and calculate the sum of trading values
    df_grouped = df_joined.groupby('symbol').agg({'trading_value': 'sum'}).reset_index()

    with fs.open(s3_file_path_financial_enriched, mode='wb') as f:
        df_joined.to_parquet(f, compression='snappy')

    print("Data saved")

    flag_end = time.time()

    print(f"Time to process and save data: {flag_end - flag_start}")

except Exception as e:
    print(e)
    print('Error in the process')
