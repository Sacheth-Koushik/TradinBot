import logging
from typing import List, Dict, Tuple, Union, Any, Optional, Callable

import numpy as np
import polars as pl
import asyncio
import websockets
import json
import queue
import logging
from datetime import datetime, timezone, timedelta

import alpaca
from alpaca.data.live.stock import *
from alpaca.data.historical.stock import *
from alpaca.data.live.option import *
from alpaca.data.historical.option import *
from alpaca.data.requests import *
from alpaca.data.timeframe import *
from alpaca.trading.client import *
from alpaca.trading.stream import *
from alpaca.trading.requests import *
from alpaca.trading.enums import *
from alpaca.common.exceptions import APIError

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class STOCKFRAME:

    def __init__(self, api_key: str, secret_key: str, subscribed: bool = False):
        self.api_key = api_key
        self.secret_key = secret_key
        self.subscribed = subscribed

        self.stock_data_client: StockHistoricalDataClient = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.options_data_client: OptionHistoricalDataClient = OptionHistoricalDataClient(self.api_key, self.secret_key)

        # Dict[symbol: pl.DataFrame]
        self.data_map: Dict[str: pl.DataFrame] = {}
        # Dict[symbol: Indicators...]
        self.indicator_map: Dict[str: List] = {}

    ################################################## api formatting ###########################################
    @staticmethod
    def _format_timeframe(timeframe) -> TimeFrame:
        if timeframe == 'day':
            return TimeFrame.Day
        elif timeframe in ['minute', 'min'] or not timeframe:
            return TimeFrame.Minute
        elif timeframe in ['hour', 'hr']:
            return TimeFrame.Hour
        elif timeframe == 'month':
            return TimeFrame.Month
        elif timeframe == 'week':
            return TimeFrame.Week
        else:
            raise ValueError('Time frame must be one of day, minute, hour, week or month.')

    def _set_start(self, start) -> datetime:
        if self.subscribed:
            return start

        else:
            time_diff = datetime.now(tz=timezone.utc) - start
            time_diff = time_diff / 60

            if time_diff < 15:
                return datetime.now(tz=timezone.utc) - timedelta(minutes=15)
            else:
                return start


    def _set_end(self, end) -> datetime:
        if self.subscribed:
            if not end:
                return datetime.now(tz=timezone.utc)
            else:
                return end

        else:
            if not end:
                return datetime.now(tz=timezone.utc) - timedelta(minutes=15)

            else:
                time_diff = end - datetime.now(tz=timezone.utc)
                time_diff = time_diff.total_seconds() / 60

                if time_diff < 15:
                    return datetime.now(tz=timezone.utc) - timedelta(minutes=15)
                else:
                    return end

    ##############################################################################################################

    ############################################### historical data #############################################
    # indicators must all be calculated which are in with this

    def _format_barSet_data(self, barSet: BarSet) -> None:
        """
        parses and formats the received data from api into symbol's respective data frames

        :param barSet: BarSet object from alpaca library
        """

        # Dict[ symbol: List of Bars]
        data = barSet.data

        for symbol, bars in data.items():
            df: pl.DataFrame = self.data_map[symbol]
            dict_list = []

            # open, high, low, close, volume, trade_count, vwap, exchange
            for bar in bars:
                dict_list.append({
                    'timestamp': bar.timestamp,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume,
                    'trade_count': bar.trade_count,
                    'vwap': bar.vwap
                })

            df = pl.concat([df, pl.DataFrame(dict_list)], how = 'vertical_relaxed').unique(subset = ['timestamp']).sort('timestamp')
            self.data_map[symbol] = df

    def fetch_historical_data(self, symbol_or_symbols: Union[List[str], str], start: datetime,
                              end: datetime = None, timeframe: str = "min", limit: int = None):
        """

        :param symbol_or_symbols:
        :param start:
        :param end:
        :param timeframe: defaults to minute
        :param limit: max number of bars
        """

        # if string make it into list
        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        timeframe = self._format_timeframe(timeframe)
        start = self._set_start(start)
        end = self._set_end(end)

        log.info("Attempting to fetch data")

        req = StockBarsRequest(
            symbol_or_symbols= symbol_or_symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit
        )

        # figure out the type of errors api can throw and accept those errors
        try:
            barSet = self.stock_data_client.get_stock_bars(req)
            log.info("Fetch successful.")
            self._format_barSet_data(barSet = barSet)
        except Exception as e:
            log.error(f"Error encountered while data fetch and process: {e}")
            pass





