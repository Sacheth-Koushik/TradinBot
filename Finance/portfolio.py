from typing import List, Dict, Tuple, Union, Any, Optional, Callable

import numpy as np
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

import warnings


class PORTFOLIO:

    def __init__(self, trade_client: TradingClient) -> None:
        """
        initialises new instance of portfolio class, where info is stored
        """

        self.trade_client = trade_client

        # pl.dataframe
        self.positions: Union[pl.DataFrame, None] = self.load_existing_position

    '''
    A lil explanation about position object, to know more refer this link
    https://alpaca.markets/sdks/python/api_reference/trading/models.html#alpaca.trading.models.Position
    
    Explanation of terms:
        asset_marginable: whether this asset can be used as collateral for trading
        cost_basis: the total amount of money it cost us, qty * avg_entry_price 
        swap_rate: used in forex
        qty: quantity of the asset
        qty_available: quantity available to trade, sometimes if the asset is tied up it cannot be used to trade
        asset_exchange: the exchange from which the asset was traded
    '''

    '''
    initially get_all_positions takes 0.7 seconds subsequent runs take upto 0.2 - 0.3 sec each
    making a df virtually takes zero time
    '''
    @property
    def load_existing_position(self) -> Union[pl.DataFrame, None]:
        """
        Loads all the positions in the market to a dataframe.
        If there are no current positions then returns None

        :return: data frame if positions are occupied else None
        """

        positions = self.trade_client.get_all_positions()

        position_data = []
        for position in positions:
            # Extract the attributes and append to the position_data list
            position_data.append({
                'symbol': position.symbol,
                'asset_type': position.asset_class.value,  # Assuming asset_class is the type you want
                'asset_marginable': position.asset_marginable,
                'avg_entry_price': position.avg_entry_price,
                'qty': position.qty,
                'side': position.side.value,
                'market_value': position.market_value,
                'cost_basis': position.cost_basis,
                'unrealized_pl': position.unrealized_pl,
                'unrealized_plpc': position.unrealized_plpc,
                'unrealized_intraday_pl': position.unrealized_intraday_pl,
                'unrealized_intraday_plpc': position.unrealized_intraday_plpc,
                'current_price': position.current_price,
                'lastday_price': position.lastday_price,
                'change_today': position.change_today,
                'swap_rate': position.swap_rate,
                'avg_entry_swap_rate': position.avg_entry_swap_rate,
                'qty_available': position.qty_available,
                'usd': position.usd,
                'asset_id': position.asset_id,
                'asset_exchange': position.exchange.value
            })

        if position_data:
            position_df = pl.DataFrame(position_data)
        else:
            position_df = None

        return position_df

    def get_position(self, req: List[tuple[str, str]]) -> Union[pl.DataFrame, None]:
        """
        Gets the filtered position in the portfolio if it exists else returns Nons

        :param req: List of tuples having symbol and / or asset type

        :return:
        """

        if not req:
            raise KeyError("You must provide requests array.")

        self.positions = self.positions.lazy()
        filter_expressions = []

        for symbol, asset_type in req:
            if symbol and asset_type:
                filter_expressions.append(
                    self.positions.filter(
                        (pl.col('symbol') == symbol) &
                        (pl.col('asset_type') == asset_type)
                    )
                )

            elif symbol:
                filter_expressions.append(
                    self.positions.filter(
                        pl.col('symbol') == symbol
                    )
                )

            elif asset_type:
                filter_expressions.append(
                    self.positions.filter(
                        pl.col('asset_type') == asset_type
                    )
                )

            else:
                raise warnings.warn("Must have a symbol or asset type", UserWarning)

        if filter_expressions:
            final_expr = filter_expressions[0]
            for expr in filter_expressions[1:]:
                final_expr |= expr

            final_res = self.positions.filter(final_expr).sort(['symbol', 'asset_type']).collect()

            if final_res.is_empty():
                return None
            else:
                return final_res

        else:
            self.positions.collect()
            return None



    '''
    Loading all positions each time is inefficient instead make it so that the porfolio is updated
    whenever order are filled in.
    
    maybe a function to just fetch curr prices would be better thant o fetch all the portfolio position, dk measure it with timers
    '''

    # update portfolio once the order is filled
    def on_order_fill(self, symbol: str):
        # check if on partial fill, it is updated as a open position in api
        # if it throws an api error just continue
        try:
            new_position = self.trade_client.get_open_position(symbol_or_asset_id = symbol)
        except APIError as e:
            pass
        # implement a function to add position
        pass

    # to remove a position
    def remove_position(self):
        pass

    # to calculate varience, risk etc
    def portfolio_metrics(self):
        pass

    # gives summary of overall portfolio, graphs n stuff can be implemented for visuals
    def summary(self):
        pass

