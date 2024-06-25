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

'''
    check whether the live data streaming is subscription based:
        if subscription based
            use the loophole
        else
            use the inbuilt function
'''





import sys
import os
from dotenv import load_dotenv
import pytz

if __name__ == '__main__':
    # sys.stdout = open("output.txt", "a")
    load_dotenv()
    api_key = os.getenv("api_key")
    secret_key = os.getenv("secret_key")

    s = StockDataStream(api_key, secret_key)

    async def handler(msg: Bar):
        print(msg.symbol)
        print("open: ", msg.open)
        print("high: ", msg.high)
        print("low: ", msg.low)
        print("close: ", msg.close)
        print("volume", msg.volume)
        print("vwap", msg.vwap)
        print(datetime.now(tz=timezone.utc))
        print()

    s.subscribe_updated_bars(handler, *('AAPL', 'MSFT', 'SPY'))
    s.subscribe_bars(handler, *('AAPL', 'MSFT', 'SPY'))
    # s.subscribe_quotes(handler, *('AAPL', 'MSFT'))
    s.run()

