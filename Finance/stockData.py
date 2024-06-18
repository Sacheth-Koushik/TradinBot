import logging
from typing import List, Dict, Tuple, Union, Any, Optional, Callable

import numpy as np
import polars as pl
import asyncio
import websockets
import json
import queue
import logging

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


class STOCKFRAME:

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

        self.stock_data_client = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.options_data_client = OptionHistoricalDataClient(self.api_key, self.secret_key)

        self.data_map: Dict[str: pl.DataFrame] = {}
        self.indicator_map: Dict[str: List] = {}

    ############################################### historical data #############################################
    # indicators must all be calculated which are in with this

    def format_barSet_data(self, barSet: BarSet):

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

