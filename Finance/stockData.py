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

    def __init__(self, api_key: str, secret_key: str, trade_client: TradingClient, subscribed: bool = False):
        self.api_key = api_key
        self.secret_key = secret_key
        self.trade_client = trade_client
        self.subscribed = subscribed

        self.stock_data_client: StockHistoricalDataClient = StockHistoricalDataClient(api_key=self.api_key, secret_key=self.secret_key)
        log.info("Client successful")
        self.options_data_client: OptionHistoricalDataClient = OptionHistoricalDataClient(self.api_key, self.secret_key)

        # Dict[symbol: pl.DataFrame]
        self.data_map: Dict[str: pl.DataFrame] = {}
        self.lvl1_data_map: Dict[str: pl.DataFrame] = {}
        self.trade_data_map: Dict[str: pl.DataFrame] = {}

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

    @property
    def is_open(self) -> bool:
        return self.trade_client.get_clock().is_open

    def _set_start(self, start) -> datetime:
        if self.subscribed or not self.is_open:
            return start

        else:
            time_diff = datetime.now(tz=timezone.utc) - start
            time_diff = time_diff.total_seconds() / 60

            if time_diff < 15:
                return datetime.now(tz=timezone.utc) - timedelta(minutes=15)
            else:
                return start


    def _set_end(self, end) -> datetime:
        if self.subscribed or not self.is_open:
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

        ########################################## data formatting ##############################################
    @staticmethod
    def _format_bar(bar: Bar):
        return {
                    'timestamp': bar.timestamp,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume,
                    'trade_count': bar.trade_count,
                    'vwap': bar.vwap
                }

    def _format_barSet_data(self, barSet: BarSet) -> None:
        """
        parses and formats the received data from api into symbol's respective data frames

        :param barSet: BarSet object from alpaca library
        """

        # Dict[ symbol: List of Bars]
        data = barSet.data
        schema = {
            'timestamp': pl.Datetime('us'),
            'open': pl.Float64,
            'high': pl.Float64,
            'low': pl.Float64,
            'close': pl.Float64,
            'volume': pl.Float64,
            'trade_count': pl.Float64,
            'vwap': pl.Float64
        }

        for symbol, bars in data.items():
            df: pl.DataFrame = self.data_map.get(symbol, pl.DataFrame(schema=schema))
            dict_list = []

            # open, high, low, close, volume, trade_count, vwap, exchange
            for bar in bars:
                dict_list.append(self._format_bar(bar=bar))

            df = pl.concat([df, pl.DataFrame(dict_list)], how = 'vertical_relaxed').unique(subset = ['timestamp']).sort('timestamp')
            self.data_map[symbol] = df

    @staticmethod
    def _format_quote(quote: Quote) -> Dict:
        return {
                    'timestamp': quote.timestamp,
                    'ask_price': quote.ask_price,
                    'ask_size': quote.ask_size,
                    'bid_price': quote.bid_price,
                    'bid_size': quote.bid_size,
                    'ask_exchange': quote.ask_exchange,
                    'bid_exchange': quote.bid_exchange,
                    'conditions': quote.conditions,
                    'tape': quote.tape
                }

    def _format_quoteSet_data(self, quoteSet: QuoteSet):
        quoteSet = quoteSet.data
        schema = {
            'timestamp': pl.Datetime('us'),
            'ask_price': pl.Float64,
            'ask_size': pl.Float64,
            'bid_price': pl.Float64,
            'bid_size': pl.Float64,
            'ask_exchange': pl.Utf8,
            'bid_exchange': pl.Utf8,
            'conditions': pl.List(pl.Utf8),
            'tape': pl.Utf8
        }

        for symbol, quotes in quoteSet.items():
            df: pl.DataFrame = self.lvl1_data_map.get(symbol, pl.DataFrame(schema=schema))
            dict_list = []

            for quote in quotes:
                dict_list.append(self._format_quote(quote))

            df = pl.concat([df, pl.DataFrame(dict_list)], how = 'vertical_relaxed').unique(subset = ['timestamp']).sort('timestamp')
            self.lvl1_data_map[symbol] = df

    @staticmethod
    def _format_trade(trade: Trade) -> Dict:
        return {
                    'timestamp': trade.timestamp,
                    'price': trade.price,
                    'size': trade.size,
                    'id': trade.id,
                    'exchange': trade.exchange,
                    'conditions': trade.conditions,
                    'tape': trade.tape
                }
    def _formate_tradeSet_data(self, tradeSet: TradeSet):

        tradeSet = tradeSet.data
        schema = {
            'timestamp': pl.Datetime('us'),
            'price': pl.Float64,
            'size': pl.Float64,
            'id': pl.Int64,
            'exchange': pl.Utf8,
            'conditions': pl.List(pl.Utf8),
            'tape': pl.Utf8
        }

        for symbol, trades in tradeSet.items():
            df: pl.DataFrame = self.trade_data_map.get(symbol, pl.DataFrame(schema=schema))
            dict_list = []

            for trade in trades:
                dict_list.append(self._format_trade(trade))

            df = pl.concat([df, pl.DataFrame(dict_list)], how='vertical_relaxed').unique(subset=['timestamp']).sort('timestamp')
            self.trade_data_map[symbol] = df

    ####################################### end of data formatting ###############################################

    ######################################## data fetch #########################################################
    # normal market data
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
            log.info("Fetch data successful.")
            self._format_barSet_data(barSet = barSet)
        except Exception as e:
            log.error(f"Error encountered while data fetch and process: {e}")
            pass

    # level  1 market data
    def fetch_stock_quotes(self, symbol_or_symbols: Union[str, List[str]], start: datetime, end: datetime = None,
                           limit: int = None):

        # if string make it into list
        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        start = self._set_start(start)
        end = self._set_end(end)

        log.info("Attempting to fetch lvl1 data")

        req = StockQuotesRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit
        )

        # figure out the type of errors api can throw and accept those errors
        try:
            quoteSet = self.stock_data_client.get_stock_quotes(req)
            log.info("Fetch lvl1 data successful.")
            self._format_quoteSet_data(quoteSet = quoteSet)

        except Exception as e:
            log.error(f"Error encountered while lvl1 data fetch and process: {e}")
            pass

    def fetch_trades(self, symbol_or_symbols: Union[str, List[str]], start: datetime, end: datetime = None,
                           limit: int = None):

        # if string make it into list
        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        start = self._set_start(start)
        end = self._set_end(end)

        log.info("Attempting to fetch trade data")

        req = StockTradesRequest(
            symbol_or_symbols=symbol_or_symbols,
            start=start,
            end=end,
            limit=limit
        )

        # figure out the type of errors api can throw and accept those errors
        try:
            tradeSet = self.stock_data_client.get_stock_trades(req)
            log.info("Fetch trade data successful.")

        except Exception as e:
            log.error(f"Error encountered while trade data fetch and process: {e}")
            pass

    ####################################### latest data ############################################################
    def get_latest_bar(self, symbol_or_symbols: Union[str, List[str]], timeframe: str = None, limit: int = None):

        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        timeframe = self._format_timeframe(timeframe)

        req = StockLatestBarRequest(
            symbol_or_symbols=symbol_or_symbols,
            timeframe=TimeFrame.Minute,
            feed=DataFeed.IEX
        )

        # figure out the type of errors api can throw and accept those errors
        try:
            data = self.stock_data_client.get_stock_latest_bar(req)
            log.info("Fetch data successful.")

            for symbol, bar in data.items():
                bar = self._format_bar(bar=bar)
                # figure out what to do with these processed bars

        except Exception as e:
            log.error(f"Error encountered while data fetch and process: {e}")
            pass

    def get_latest_quote(self, symbol_or_symbols: Union[str, List[str]], limit: int = None):
        # if string make it into list
        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        log.info("Attempting to fetch lvl1 data")

        req = StockLatestQuoteRequest(
            symbol_or_symbols=symbol_or_symbols,
            limit=limit
        )

        # figure out the type of errors api can throw and accept those errors
        try:
            data = self.stock_data_client.get_stock_latest_quote(req)
            log.info("Fetch lvl1 data successful.")

            for symbol, quote in data.items():
                quote = self._format_quote(quote)
                # figure out what to do with latest quotes

        except Exception as e:
            log.error(f"Error encountered while lvl1 data fetch and process: {e}")
            pass

    def get_latest_trade(self, symbol_or_symbols: Union[str, List[str]], limit: int = None):
        # if string make it into list
        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        log.info("attempting to fetch latest trade data")

        req = StockLatestTradeRequest(
            symbol_or_symbols=symbol_or_symbols
        )

        try:
            data = self.stock_data_client.get_stock_latest_trade(req)

            for symbol, trade in data.items():
                trade = self._format_trade(trade)
                # figure out what to do with this trade

        except Exception as e:
            log.error(f"Error encountered while latest trade data fetch and process: {e}")
            pass

    def get_snapshot(self, symbol_or_symbols: Union[str, List[str]]):
        # if string make it into list
        if isinstance(symbol_or_symbols, str):
            symbol_or_symbols = [symbol_or_symbols]

        req = StockSnapshotRequest(symbol_or_symbols = symbol_or_symbols)
        data = self.stock_data_client.get_stock_snapshot(req)

        # data is an Dict[symbol: SnapShot]
        for symbol, snap in data.items():

            # Snap: [ daily bar: bar, minute bar: bar, prev daily bar: bar, latest_quote: quote, latest trade: trade ]
            daily_bar = self._format_bar(snap.daily_bar)
            minute_bar = self._format_bar(snap.minute_bar)
            prev_daily_bar = self._format_bar(snap.previous_daily_bar)
            latest_quote = self._format_quote(snap.latest_quote)
            latest_trade = self._format_trade(snap.latest_trade)

            # figure out what to do with these latest data....

    ############################### end of latest data #######################################

    ################################ data stream ###############################################

import os
from dotenv import load_dotenv
import sys

if __name__ == '__main__':
    sys.stdout = open("output.txt", "w")
    load_dotenv()
    api_key = os.getenv("api_key")
    secret_key = os.getenv("secret_key")
    data_client = StockHistoricalDataClient(api_key, secret_key)






