from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import plotters

client = CryptoHistoricalDataClient()

request_params = CryptoBarsRequest(
                        symbol_or_symbols=["BTC/USD"],
                        timeframe=TimeFrame.Day,
                        start=datetime(2000, 1, 1),
                        end=datetime(2025, 1, 1)
                    )

bars = client.get_crypto_bars(request_params)
df = bars.df.reset_index()

df["ma50"] = df["close"].rolling(window=50).mean()
df["ma200"] = df["close"].rolling(window=200).mean()

plotters.plot_hlc(df)
plotters.plot_moving_averages(df)
