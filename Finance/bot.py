import polars as pl

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

from datetime import datetime, time, timezone, timedelta
import pytz
from typing import List, Dict, Union, Optional

import time as true_time
import pathlib
import json

from Finance.portfolio import PORTFOLIO
from Finance.orders import ORDERS


class BOT:

    def __init__(self, api_key: str, secret_key: str,
                paper_trading: bool = True, fractional_trading: bool = False) -> None:

        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper_trading
        self.fractional_trading = fractional_trading

        # makes a trading client and sets fractional trading preference
        self.trade_client = self._make_client

        # make portfolio by passing in trade_client
        self.portfolio = PORTFOLIO(trade_client = self.trade_client)

        # make orders object
        self.orders = ORDERS(api_key = self.api_key, secret_key = self.secret_key, portfolio = self.portfolio,
                             trade_client = self.trade_client)

    ##################################### Client and calender ##############################################
    @property
    def _make_client(self) -> TradingClient:

        """
        Makes a trade client and sets the account configuration

        :return: trading client
        """

        # make client
        trade_client = TradingClient(
            api_key = self.api_key,
            secret_key = self.secret_key,
            paper = self.paper
        )

        '''
        trade_client.get_account()
        returns  TradeAccount object which holds various details of the account
        refer this link for details on trade account object
        https://alpaca.markets/sdks/python/api_reference/trading/models.html#alpaca.trading.models.TradeAccount
        '''

        self.account_details = trade_client.get_account()

        # set configurations of fractional trading
        if not self.fractional_trading:
            acc_config = trade_client.get_account_configurations()
            acc_config.fractional_trading = self.fractional_trading
            trade_client.set_account_configurations(account_configurations = acc_config)

        return trade_client

    '''
    Below are a few time related functions for market timings.
    
    A Clock object of alpaca has current time, next open time, next close time and a bool value of whether the market is
    open now or not.
    Refer this for more details,
    https://alpaca.markets/sdks/python/api_reference/trading/models.html#alpaca.trading.models.Clock
    
    Alpaca returns the time in eastern timezone, but it is a convention and good practise to store the time
    info in UTC time, hence the convention is followed. 
    '''
    @staticmethod
    def change_timestamps(clock: Clock) -> Clock:
        """
        A helper function to reassign all times to eastern time

        :param clock: a clock object
        :return: the same clock object with times in utc
        """

        clock.timestamp = clock.timestamp.astimezone(pytz.utc)
        clock.next_open = clock.next_open.astimezone(pytz.utc)
        clock.next_close = clock.next_close.astimezone(pytz.utc)

        return clock

    def is_open(self) -> bool:
        """
        A helper function to find out whether the market is open for trades

        :return:whether the market is open for trading
        """

        clock = self.trade_client.get_clock()
        clock = self.change_timestamps(clock = clock)

        return clock.is_open

    def next_times(self) -> Dict:
        """
        Returns the next opening and closing time of the market

        :return: dict[next_open, next_close]
        """

        clock = self.trade_client.get_clock()
        clock = self.change_timestamps(clock=clock)

        times = {
            'next_open': clock.next_open,
            'next_close': clock.next_close
        }

        return times

    ##################################################### Portfolio #############################################
    '''
    Creating portfolio related functions below
    '''
    def show_portfolio(self):

        print(self.portfolio.positions)

    def show_orders(self):
        print(self.orders.orders_df)
