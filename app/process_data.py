from  datetime import datetime
import polars as pl

# read enviroment variables
from dotenv import load_dotenv
load_dotenv()

import os

import s3fs

import time

fs = s3fs.S3FileSystem()

BUCKET_NAME = os.getenv("BUCKET_NAME", "<bucket_name>")
STOCK_FILENAME = os.getenv("stock_prices_filename", "stock_prices_10M.csv")
VOLUMEN_FILENAME = os.getenv("trading_volume_filename", "trading_volume_10M.csv")

FINANCIAL_FILENAME_ENRICHED = os.getenv("financial_filename_enriched", "stock_trading_volume_10M.parquet")

print(BUCKET_NAME)

print(STOCK_FILENAME)

print(VOLUMEN_FILENAME)

print(FINANCIAL_FILENAME_ENRICHED)

start_date = time.time()

s3_file_path = f's3://{BUCKET_NAME}/financial_data/{STOCK_FILENAME}'

df_prices = pl.scan_csv(s3_file_path, )

end_date = time.time()

print(f"Time to load stock prices: {end_date - start_date}")

start_date = time.time()

s3_file_path = f's3://{BUCKET_NAME}/financial_data/{VOLUMEN_FILENAME}'

df_volume = pl.scan_csv(s3_file_path)

end_date = time.time()

print(f"Time to load trading volume: {end_date - start_date}")

s3_file_path_financial_enriched = f's3://{BUCKET_NAME}/financial_data_output/{FINANCIAL_FILENAME_ENRICHED}.parquet'

print("Data processing")
print(s3_file_path_financial_enriched)

try:
    flag_start = time.time()

    start_date = "2023-07-01"
    end_date = "2023-08-01"

    df_volume.cast({"symbol": pl.Categorical, "date": pl.Datetime})
    df_prices.cast({"symbol": pl.Categorical, "date": pl.Datetime})


    df_volume = df_volume \
                .filter(pl.col("date").is_between(pl.lit(start_date), pl.lit(end_date)))
    

    df_prices = df_prices \
                .filter(pl.col("date").is_between(pl.lit(start_date), pl.lit(end_date)))

    q1 = (
        df_prices
        .join(df_volume, on=["date", "symbol"], how="inner")
    )


    q2 = (
            q1
            .with_columns(
                (pl.col("close_price") * pl.col("volume")).alias("trading_value")
            )
            .group_by("symbol")
            .agg(pl.sum("trading_value"))
        )

    print("Data processed lazy")

    with fs.open(s3_file_path_financial_enriched, mode='wb') as f:
        q1.collect().write_parquet(f, compression='snappy')

    flag_end = time.time()

    print(f"Time to process and save data: {flag_end - flag_start}")

except Exception as e:
    print(e)
    print('Error in the process')
    