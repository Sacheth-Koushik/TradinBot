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

from Finance.stockData import STOCKFRAME


class INDICATORS:

    def __init__(self, stockFrame: STOCKFRAME):
        self.stockFrame = stockFrame
        # the data needed and will be on the dataFrame is open, high, low, close, volume, trade_count, vwap, exchange

    '''
    ma, macd, rsi, adx, exponentially ma, 
    
    open interest
    PCR
    VWAP
    Fibonacci Retreatment
    RSI
    EMA
    Bollinger Bands
    IMI(Like RSI) 
    
    macd, rsi, ema, bollinger bands, supertrend (length20, factor2), adx, vwap, pcr (put call ratio), 
    anchored vwap, pivot points, atr (used with trailing stops), bollinger bands, stochastic oscillators, fibonacci

    volume profile, stochastic oscillator, donchain channel (sideways), bollinger bands ( sideways), ichimoku cloud
    
    do not use vwap on low liquid stocks, only on high liquid stocks.... 
    donot use vwap on index spots, use it on index futures as index spots have no volm in them
    
    '''


