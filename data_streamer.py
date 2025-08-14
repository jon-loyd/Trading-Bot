from alpaca.data.live import CryptoDataStream
import csv
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

csv_file = "price_log.csv"

try:
  with open(csv_file, "x", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["timestamp", 
                    "symbol", 
                    "open", 
                    "high", 
                    "low", 
                    "close", 
                    "volume", 
                    "trade count", 
                    "vwap"])
except FileExistsError:
  pass

stream = CryptoDataStream(API_KEY, API_SECRET)

async def data_handler(data):
  with open(csv_file, "a", newline="") as file:
    writer = csv.writer(file)
    writer.writerow([data.timestamp, 
                    data.symbol, 
                    data.open, 
                    data.high, 
                    data.low, 
                    data.close, 
                    data.volume, 
                    data.trade_count, 
                    data.vwap])

stream.subscribe_bars(data_handler, "BTC/USD")
stream.run()