from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass
from alpaca.trading.requests import GetAssetsRequest
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Dict

from utils import get_alpaca_credentials


# Dictionary  supported timeframes
TIMEFRAMES: Dict[str, TimeFrame] = {
    "1m": TimeFrame(1, TimeFrameUnit.Minute),
    "2m": TimeFrame(2, TimeFrameUnit.Minute),
    "5m": TimeFrame(5, TimeFrameUnit.Minute),
    "15m": TimeFrame(15, TimeFrameUnit.Minute),
    "30m": TimeFrame(30, TimeFrameUnit.Minute),
    "1h": TimeFrame(1, TimeFrameUnit.Hour),
    "2h": TimeFrame(2, TimeFrameUnit.Hour),
    "4h": TimeFrame(4, TimeFrameUnit.Hour),
    "12h": TimeFrame(12, TimeFrameUnit.Hour),
    "1d": TimeFrame.Day,
    "1w": TimeFrame.Week,
    "1M": TimeFrame.Month
}


class DataManager:

    """
    Manages downloading and loading data for cryptocurrencies from alpaces.
    """
    
    def __init__(self, path: str = "../data") -> None:
        self.historical_client = CryptoHistoricalDataClient()
        self.path = Path(__file__).parent.joinpath(path).resolve()
        self.available_symbols = None

    def download(self, symbol: str, timeframe: str, start_date: str,end_date: str) -> None:
        if not self.available_symbols:
            self._set_available_symbols()

        if symbol not in self.available_symbols:
            raise ValueError(f"The symbol {symbol} either does not exist on Alpaca or the format is wrong.")
        
        if timeframe not in TIMEFRAMES:
            raise ValueError(f"The timeframe {timeframe} is not supported.")
        
        df = self._get_data(symbol, TIMEFRAMES[timeframe], start_date, end_date)
        file_path = self._get_csv_file_path(symbol, timeframe)
        df.to_csv(file_path, header=True, index=True)

    @staticmethod
    def _create_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def _get_csv_file_path(self, symbol: str, timeframe: str) -> Path:
        timeframe_path = self.path.joinpath(timeframe)
        self._create_directory(timeframe_path)
        file_name = f"{symbol.replace("/", "-").replace(":", "-")}.csv"
        return timeframe_path.joinpath(file_name)

    def _set_available_symbols(self) -> None:
        api_key, api_secret = get_alpaca_credentials()
        trading_client = TradingClient(api_key, api_secret, paper=True)
        search_params = GetAssetsRequest(asset_class=AssetClass.CRYPTO)
        assets = trading_client.get_all_assets(search_params)
        self.available_symbols = [asset.symbol for asset in assets if asset.tradable]

    def _get_data(self, symbol: str, timeframe: TimeFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        request_params = CryptoBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=timeframe,
            start=start_date,
            end=end_date
        )
        
        bars = self.historical_client.get_crypto_bars(request_params)
        df = bars.df.reset_index()

        return df



data_manager = DataManager()

data_manager.download("BTC/USD", "1M", datetime(2017, 1, 1, 0, 0, 0), datetime.now())