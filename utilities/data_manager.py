"""
Data management utilites for loading and storing historical trading data.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass
from alpaca.trading.requests import GetAssetsRequest
import pandas as pd

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
        self.assets = None
        self.available_symbols = None
        self._set_assets()

    def download(self, symbol: str, timeframe: str, start_date: datetime, end_date: datetime) -> None:
        """
        Downloads data for a given symbol and timeframe, then saves it to a CSV file.
        """

        if symbol not in self.available_symbols:
            raise ValueError(f"The symbol {symbol} either does not exist on Alpaca or the format is wrong.")

        if timeframe not in TIMEFRAMES:
            raise ValueError(f"The timeframe {timeframe} is not supported.")

        df = self._get_data(symbol, TIMEFRAMES[timeframe], start_date, end_date)
        df.set_index('timestamp', inplace=True)
        file_path = self._get_csv_file_path(symbol, timeframe)
        df.to_csv(file_path, header=True, index=True)

    def fetch_symbol_asset_info(self, symbol: str):
        if symbol not in self.available_symbols:
            raise ValueError(f"The symbol {symbol} either does not exist on Alpaca or the format is wrong.")
        return self.assets[symbol]

    def load(self, symbol: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Loads data from a CSV file for a given symbol and timeframe.
        """
        
        file_path = self._get_csv_file_path(symbol, timeframe)

        if symbol not in self.available_symbols:
            raise ValueError(f"The symbol {symbol} either does not exist on Alpaca or the format is wrong.")

        if not file_path.exists():
            raise FileNotFoundError(f"The data file for {symbol} in timeframe {timeframe} does not exist. Please use the download method first.")

        df = pd.read_csv(file_path, header=0, parse_dates=["timestamp"], index_col="timestamp")
        if df.empty:
            raise ValueError(f"The data file for {symbol} in timeframe {timeframe} is empty.")

        available_start, available_end = df.index.min(), df.index.max()
        if not start_date:
            start_date = available_start
        if not end_date:
            end_date = available_end

        start_date = self._ensure_utc(start_date)
        end_date = self._ensure_utc(end_date)

        if (start_date < available_start or end_date > available_end):
            raise ValueError(
                f"The requested date range [{start_date} - {end_date}] "
                f"is outside the available data [{available_start} - {available_end}]. "
                "Please adjust your dates or update the data file by using the download method."
            )

        subset = df.loc[start_date:end_date]
        if subset.empty:
            raise ValueError(f"No data available between {start_date} and {end_date}.")

        return subset

    @staticmethod
    def _create_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _get_csv_file_path(self, symbol: str, timeframe: str) -> Path:
        timeframe_path = self.path.joinpath(timeframe)
        self._create_directory(timeframe_path)
        file_name = f"{symbol.replace("/", "-").replace(":", "-")}.csv"
        return timeframe_path.joinpath(file_name)

    def _get_data(self, symbol: str, timeframe: TimeFrame, start_date: datetime, end_date: datetime
                  ) -> pd.DataFrame:
        request_params = CryptoBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=timeframe,
            start=start_date,
            end=end_date
        )

        bars = self.historical_client.get_crypto_bars(request_params)
        df = bars.df.reset_index()

        return df

    def _set_assets(self) -> None:
        api_key, api_secret = get_alpaca_credentials()
        trading_client = TradingClient(api_key, api_secret, paper=True)
        search_params = GetAssetsRequest(asset_class=AssetClass.CRYPTO)
        assets = trading_client.get_all_assets(search_params)
        tradeable_assets = [asset for asset in assets if asset.tradable]
        self.assets = {asset.symbol: asset for asset in tradeable_assets}
        self.available_symbols = [asset.symbol for asset in tradeable_assets]