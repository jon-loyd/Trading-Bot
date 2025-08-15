from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Dict


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
        self.client = CryptoHistoricalDataClient()
        self.path = Path(__file__).parent.joinpath(path).resolve()

    def download(self, symbol: str, timeframe: str, start_date: str,end_date: str) -> None:
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

    def _get_data(self, symbol: str, timeframe: TimeFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        request_params = CryptoBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=timeframe,
            start=start_date,
            end=end_date
        )
        
        bars = self.client.get_crypto_bars(request_params)
        df = bars.df.reset_index()

        return df



data_manager = DataManager()

data_manager.download("BTC/USD", "1M", datetime(2017, 1, 1, 0, 0, 0), datetime.now())