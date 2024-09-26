import pandas as pd
import numpy as np

np.random.seed(42)

# Define the number of rows
N = 20_000_000

date_range = pd.date_range(
    start=pd.to_datetime("1/1/2023").tz_localize("Europe/Berlin"), 
    end=pd.to_datetime("1/9/2024").tz_localize("Europe/Berlin"), 
    freq='T'  # Frequency set to minutes
)

# stock prices
stock_data = {
    "date": np.random.choice(date_range, N),

    "symbol": np.random.choice(["AAPL", "GOOGL", "AMZN", "MSFT", "BTC", "ETH", "ADA", "ICP", "SOL"], N),
    "close_price": np.random.uniform(100, 3000, N)
}
stock_df = pd.DataFrame(stock_data)

# trading volume
volume_data = {
    "date": stock_data["date"],
    "symbol": stock_data["symbol"],
    "volume": np.random.randint(10000, 500000, N)
}
volume_df = pd.DataFrame(volume_data)


stock_df.to_csv("stock_prices_10M.csv", index=False, encoding='utf-8', lineterminator='\n')
volume_df.to_csv("trading_volume_10M.csv", index=False, encoding='utf-8', lineterminator='\n')

print(stock_df.head())
print(volume_df.head())
