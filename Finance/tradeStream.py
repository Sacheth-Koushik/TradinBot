import logging
from typing import List, Dict, Tuple, Union, Any, Optional, Callable

import numpy as np
import pandas as pd
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


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
#may have to alter sleep times coz if orders r rapid dk how its gonna react

class TradeStream:
    def __init__(self, paper: bool = True, url_override: str = None, websocket_params: Optional[Dict] = None,
                 api_key: str = '', secret_key: str = ''):
        self.paper = paper
        self._api_key = api_key
        self._secret_key = secret_key
        self._end_point = url_override if url_override else 'wss://paper-api.alpaca.markets/stream' if (
            self.paper) else 'wss://data.alpaca.markets/stream'

        self._ws: Union[websockets.WebSocketClientProtocol, None] = None
        self._websocket_params = {
            "ping_interval": 10,
            "ping_timeout": 180,
            "max_queue": 1024,
        }

        if websocket_params:
            self._websocket_params = websocket_params

        self._running = False
        self._should_run = False
        self._loop = None
        self._stop_stream_queue = queue.Queue()
        self._handler = None

    async def _connect(self):
        log.info(f"Connecting to {self._end_point}")
        self._ws = await websockets.connect(self._end_point, **self._websocket_params)

    async def _auth(self):
        message = {
            'action': 'authenticate',
            'data': {
                'key_id': self._api_key,
                'secret_key': self._secret_key
            }
        }

        message = json.dumps(message)
        await self._ws.send(message)

        response = await self._ws.recv()
        response = json.loads(response)
        log.info(f"Auth response: {response}")

        if response.get('data').get('status') != 'authorized':
            raise ValueError("Authentication failed")

    async def _subscribe_trade_updates(self):
        message = {
            'action': 'listen',
            'data': {
                'streams': ['trade_updates']
            }
        }

        message = json.dumps(message)
        if self._handler:
            await self._ws.send(message)

    def subscribe_trade_updates(self, handler: Callable):
        if not asyncio.iscoroutinefunction(handler):
            raise ValueError("handler must be coroutine function.")

        self._handler = handler

        if self._running:
            asyncio.run_coroutine_threadsafe(self._subscribe_trade_updates(), self._loop).result()

    def run(self):
        try:
            asyncio.run(self._run_forever())
        except KeyboardInterrupt:
            print("Ending.")
            pass

    async def _start_ws(self):
        await self._connect()
        await self._auth()

        log.info(f"Established connection to {self._end_point}")
        await self._subscribe_trade_updates()

    async def _close_ws(self):
        if self._ws:
            await self._ws.close()
            self._ws = None
            self._running = False
            log.info("Terminating websocket connection.")

    async def _dispatch(self, response: Dict):
        stream = response.get('stream')

        if stream == 'trade_updates':
            if self._handler:
                await self._handler(response)

    async def _consume(self):
        while True:
            if not self._stop_stream_queue.empty():
                self._stop_stream_queue.get(timeout = 1)
                await self._close_ws()
                break

            else:
                try:
                    response = await asyncio.wait_for(self._ws.recv(), 5)
                    response = json.loads(response)
                    await self._dispatch(response)

                except asyncio.TimeoutError:
                    log.info("No updates yet, listening.....")
                    pass

    async def _run_forever(self):
        self._loop = asyncio.get_running_loop()

        # do not start until subscribed
        while not self._handler:
            if not self._stop_stream_queue.empty():
                self._stop_stream_queue.get(timeout = 1)
                return
            await asyncio.sleep(0.1)

        logging.info("started streaming")
        self._should_run = True
        self._running = False

        while True:
            try:
                if not self._should_run:
                    log.info("stopping trading stream.")
                    return

                if not self._running:
                    log.info("establishing websocket connection")
                    await self._start_ws()

                    self._running = True
                    await self._consume()

            except websockets.WebSocketException as wse:
                await self._close_ws()
                self._running = False

                log.warning("websocket error, restarting connection: " + str(wse))

            except Exception as e:
                log.exception("Exception during websocket: " + str(e))

            finally:
                await asyncio.sleep(0.01)

    async def _stop_ws(self):
        self._should_run = False
        if self._stop_stream_queue.empty():
            self._stop_stream_queue.put_nowait({'should_stop': True})

    def stop(self):
        if self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._stop_ws(), self._loop).result()

if __name__ == '__main__':
    t = TradeStream()
    async def handler(msg):
        print(msg)
    t.subscribe_trade_updates(handler)
    t.run()
