import warnings
from typing import List, Dict, Tuple, Union, Any, Optional, Callable

import numpy as np
import polars as pl
import asyncio
import websockets
import json
import logging
from datetime import datetime, timezone

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

from Finance.portfolio import PORTFOLIO
from Finance.tradeStream import TradeStream

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ORDERS:

    def __init__(self, trade_client: TradingClient, portfolio: PORTFOLIO, api_key: str = '', secret_key: str = ''):

        self.trade_client = trade_client
        self.orders_df: pl.DataFrame = self.get_all_orders()
        self._api_key = api_key
        self._secret_key = secret_key

        self.portfolio = portfolio

    def get_all_orders(self) -> pl.DataFrame:

        log.info("starting to fetch all orders")
        orders = self.trade_client.get_orders()
        if orders:
            log.info("got some orders")

        schema = {
            "symbol": pl.Utf8,
            "asset_type": pl.Utf8,
            "status": pl.Utf8,
            "id": pl.Utf8,
            "client_order_id": pl.Utf8,
            "created_at": pl.Datetime("us", time_zone=None),
            "updated_at": pl.Datetime("us", time_zone=None),
            "submitted_at": pl.Datetime("us", time_zone=None),
            "filled_at": pl.Datetime("us", time_zone=None),
            "expired_at": pl.Datetime("us", time_zone=None),
            "canceled_at": pl.Datetime("us", time_zone=None),
            "failed_at": pl.Datetime("us", time_zone=None),
            "replaced_at": pl.Datetime("us", time_zone=None),
            "replaced_by": pl.Utf8,
            "replaces": pl.Utf8,
            "asset_id": pl.Utf8,
            "notional": pl.Float64,
            "qty": pl.Float64,
            "filled_qty": pl.Float64,
            "filled_avg_price": pl.Float64,
            "order_class": pl.Utf8,
            "type": pl.Utf8,
            "side": pl.Utf8,
            "time_in_force": pl.Utf8,
            "limit_price": pl.Float64,
            "stop_price": pl.Float64,
            "extended_hours": pl.Boolean,
            "legs": pl.List(pl.Utf8),
            "trail_percent": pl.Float64,
            "trail_price": pl.Float64,
            "hwm": pl.Float64
        }

        orders_df: pl.DataFrame = pl.DataFrame(schema = schema)

        dict_list = []
        for order in orders:
            dict_list.append(self.process_and_add_order(order, add = False))

        if dict_list:
            log.info("dict_list is non empty.")
            orders_df = pl.concat([orders_df, pl.DataFrame(dict_list)], how = 'vertical')

        return orders_df

    ################################# prepping requests ##########################################
    @staticmethod
    def _format_asset_type(asset_type: str) -> AssetClass:

        if asset_type.lower() == 'equity' or asset_type.lower() == 'stock':
            return AssetClass.US_EQUITY

        elif asset_type.lower() == 'option' or asset_type.lower() == 'options':
            return AssetClass.US_OPTION

        else:
            raise ValueError("Asset Type is not valid, kindly enter whether equity or option.")

    '''
    A brief explanation of time in force component of an order
    
    day order: A day order is valid only for the trading day on which it is placed. 
                If it is not filled by the end of the trading day, the order is canceled.
    
    good to cancel (gtc) order:  A GTC order remains active until it is either executed or explicitly 
                                canceled by the trader. It does not expire at the end of the trading day.
    
    at the opening (opg) order: An OPG order is executed at the opening of the trading session. 
                                If it is not executed at the open, it is canceled. Generally used to capture the opening
                                auction to capture the opening price of the market.
    
    at the close (cls) order: A CLS order is executed at the close of the trading session. 
                                If it is not executed at the close, it is canceled. done to capture closing price.
    
    immediate or cancel (ioc) order: An IOC order must be executed immediately upon reaching the market. 
                                        Any portion of the order that is not filled immediately is canceled.
                                        A customization where if it our request is not met immediately we cancel our
                                        order request, partial fills are permitted.
    
    fill or kill (fok) order: A FOK order must be filled in its entirety immediately upon reaching the market. 
                                If the entire order cannot be filled immediately, the order is canceled.
                                Used by traders who require complete execution of their orders without any partial 
                                fills, ensuring that either the whole order is executed at once or not at all.

    '''

    @staticmethod
    def _format_time_in_force(time_in_force: str) -> TimeInForce:
        if time_in_force.lower() == 'day':
            return TimeInForce.DAY

        elif time_in_force.lower() == 'gtc':
            return TimeInForce.GTC

        elif time_in_force.lower() == 'opg':
            return TimeInForce.OPG

        elif time_in_force.lower() == 'cls':
            return TimeInForce.CLS

        elif time_in_force.lower() == 'ioc':
            return TimeInForce.IOC

        elif time_in_force.lower() == 'fok':
            return TimeInForce.FOK

        else:
            raise ValueError("Not a valid time in force, it must be one of day, gtc, opg, cls, ioc, fok.")

    @staticmethod
    def _format_order_side(order_side: str) -> OrderSide:
        if order_side.lower() == 'buy':
            return OrderSide.BUY

        elif order_side.lower() == 'sell':
            return OrderSide.SELL

        else:
            raise ValueError("buy_or_sell must be buy or sell")

    '''
    market order:  A market order is an order to buy or sell a security immediately at the current market price.
    
    limit order: A limit order is an order to buy or sell a security at a specific price or better.
                 While selling if a limit price is set, it will sell for a price >= limit price.
                 While buying iod a limit price is set, it will buy for a price <= limit price.
                 It is possible that the order will be filled partially ( if market moves too quickly ) or not filled 
                 at all ( if the market never moves in your favour )
    
    stop order: A stop order becomes a market order once the stop price is reached.
                While buying, it will place the order to buy a stock once the price hits the stop price, so it will be
                bought at the next available price
                While selling, it will sell the order once the price hits the stop price, so it will be sold at next
                available price.
                Note that the next available price means the next available market price, so in short we can say that
                the order after satisfying the condition becomes a market order.
    
    stop limit order: A stop-limit order combines the features of a stop order and a limit order. Once the stop 
                      price is reached, the order becomes a limit order instead of a market order.
                      
                      Once the stop price type condition is satisfied, it will turn into a limit order instead of a 
                      market order, this again has the same risk as limit order after the condition is satisfied ie
                      the order may be partially filled or not filled at all.
    
    trailing stop: A trailing stop order is a stop order set at a specific percentage or dollar amount away from the 
                   current market price. It "trails" the market price as it moves.
    
    refer to this link for more clarity: https://youtube.com/watch?v=p9YndmEoJn0
    '''

    @staticmethod
    def _format_order_type(order_type: str) -> OrderType:
        if order_type.lower() == 'market' or order_type.lower() == 'mkt':
            return OrderType.MARKET

        elif order_type.lower() == 'limit' or order_type.lower() == 'lmt':
            return OrderType.LIMIT

        elif order_type.lower() == 'stop' or order_type.lower() == 'stp':
            return OrderType.STOP

        elif order_type.lower() in ['stop_limit', 'stp_lmt', 'stop limit', 'stp lmt']:
            return OrderType.STOP_LIMIT

        elif order_type.lower() == 'trailing stop' or order_type.lower() == 'trialing_stop':
            return OrderType.TRAILING_STOP

        else:
            raise ValueError("Not a valid order type, it must be one "
                             "of market or limit or stop or stop limit or trailing order")

    '''
    Brief explanation of order classes
    
    simple: A simple order is a single, standalone order that is not linked to any other orders.
    
    bracket:  bracket order consists of three orders: a primary order and two opposite-side orders 
              (a take-profit order and a stop-loss order).
    
    One cancels other (oco): An OCO order consists of two linked orders. If one order is executed, 
                             the other is automatically canceled.
    
    One triggers other (oto): An OTO order involves two linked orders where the execution of the 
                              first (primary) order triggers the second order.
    '''

    @staticmethod
    def _format_order_class(order_class: str) -> OrderClass:
        if order_class.lower() == 'simple':
            return OrderClass.SIMPLE

        elif order_class.lower() == 'bracket':
            return OrderClass.BRACKET

        elif order_class.lower() == 'oco':
            return OrderClass.OCO

        elif order_class.lower() == 'oto':
            return OrderClass.OTO

        else:
            raise ValueError("Order class must be one of simple, bracket, oco or oto.")

    @staticmethod
    def _format_position_intent(position_intent):
        if position_intent.lower() == 'buy_to_open':
            return PositionIntent.BUY_TO_OPEN

        elif position_intent.lower() == 'buy_to_close':
            return PositionIntent.BUY_TO_CLOSE

        elif position_intent.lower() == 'sell_to_open':
            return PositionIntent.SELL_TO_OPEN

        elif position_intent.lower() == 'sell_to_close':
            return PositionIntent.SELL_TO_CLOSE

        else:
            raise ValueError("Position intent must be one of buy_to_open, buy_to_close, sell_to_open, sell_to_close.")

    ########################################## end of static methods #####################################
    # id is not saved, check into it
    def process_and_add_order(self, order: Order, add: bool = True) -> Union[Dict, None]:

        new_order = {
            "symbol": order.symbol,
            "asset_type": order.asset_class.value,
            "status": order.status.value,
            "id": str(order.id),
            "client_order_id": str(order.client_order_id),
            "created_at": order.created_at.replace(tzinfo=timezone.utc) if order.created_at else None,
            "updated_at": order.updated_at.replace(tzinfo=timezone.utc) if order.updated_at else None,
            "submitted_at": order.submitted_at.replace(tzinfo=timezone.utc) if order.submitted_at else None,
            "filled_at": order.filled_at.replace(tzinfo=timezone.utc) if order.filled_at else None,
            "expired_at": order.expired_at.replace(tzinfo=timezone.utc) if order.expired_at else None,
            "canceled_at": order.canceled_at.replace(tzinfo=timezone.utc) if order.canceled_at else None,
            "failed_at": order.failed_at.replace(tzinfo=timezone.utc) if order.failed_at else None,
            "replaced_at": order.replaced_at.replace(tzinfo=timezone.utc) if order.replaced_at else None,
            "replaced_by": order.replaced_by,
            "replaces": str(order.replaces),
            "asset_id": str(order.asset_id),
            "notional": float(order.notional) if order.notional else None,
            "qty": float(order.qty) if order.qty else None,
            "filled_qty": float(order.filled_qty) if order.filled_qty else 0,
            "filled_avg_price": float(order.filled_avg_price) if order.filled_avg_price else None,
            "order_class": order.order_class.value,
            "type": order.type.value,
            "side": order.side.value,
            "time_in_force": order.time_in_force.value,
            "limit_price": float(order.limit_price) if order.limit_price else None,
            "stop_price": float(order.stop_price) if order.stop_price else None,
            "extended_hours": order.extended_hours,
            "legs": order.legs,
            "trail_percent": float(order.trail_percent) if order.trail_percent else None,
            "trail_price": float(order.trail_price) if order.trail_price else None,
            "hwm": float(order.hwm) if order.hwm else None
        }
        if not add:
            return new_order
        else:
            self.orders_df = pl.concat([self.orders_df,
                                        pl.DataFrame(new_order)], how = 'vertical')

    def new_order(self, symbol: str, buy_or_sell: str, value: float, is_qty: bool = True, order_type: str = 'market',
                  asset_type: str = 'equity', time_in_force: str = 'gtc', stop_price: float = 0.0,
                  limit_price: float = 0.0, trail_price: float = 0.0, trail_percent: float = 0.0,
                  extended_hours: bool = False, take_profit: Union[float, None] = None,
                  stop_loss: bool = False, position_intent: Union[str, None] = None,
                  order_class: str = 'simple'):
        """

        only stock trades are designed right now, and only simple and bracket order, oco and oto are not designed yet

        :param symbol: symbol of stock
        :param buy_or_sell: buying or selling, 'buy' or 'sell'
        :param value: float value of quantity or notional value
        :param is_qty: specifies whether the value is quantity or not
        :param order_type: one of market, limit, stop, stop limit, trailing stop
        :param asset_type: stock or option
        :param time_in_force: one of day, gtc (good till cancel) , opg ( at open) , cls (at close),
                              ioc(immediate or cancel), fok (fill or kill )
        :param stop_price: stop price of the order
        :param limit_price: limit price of the order
        :param trail_price: trail price of the order
        :param trail_percent: trail percentage of the order
        :param extended_hours: is extended hours needed or not
        :param take_profit: take profit price for the order
        :param stop_loss: stop loss required or not
        :param position_intent: one of buy_to_open, buy_to_close, sell_to_open, sell_to_close
        :param order_class: simple, bracket, oco (one cancels other) or oto (one triggers other)
        :return:
        """

        order_side: OrderSide = self._format_order_side(buy_or_sell)
        order_type: OrderType = self._format_order_type(order_type)
        asset_type: AssetClass = self._format_asset_type(asset_type)
        time_in_force: TimeInForce = self._format_time_in_force(time_in_force)

        # quantity or notional value
        quantity_or_notional_map = {
            True: 'qty',
            False: 'notional'
        }

        quantity_or_notional = quantity_or_notional_map[is_qty]
        req_params = {
            'symbol': symbol,
            'side': order_side,
            'time_in_force': time_in_force,
            quantity_or_notional: value,
            'type': order_type,
            'extended_hours': extended_hours
        }

        # order class handling
        if order_class:
            order_class = self._format_order_class(order_class)
            req_params['order_class'] = order_class

        # take profit handling
        if take_profit:
            take_profit = {'limit_price': take_profit}
            req_params['take_profit'] = take_profit

        # stop loss handling
        if stop_loss:
            if not stop_price:
                raise ValueError("Stop price is required.")
            stop_loss = {'stop_price': 100.0}

            # in here if the limit price is not provided, then the order will turn into a market order
            if limit_price:
                stop_loss['limit_price'] = limit_price

            req_params['stop_loss'] = stop_loss

        # position intent handling
        if position_intent:
            req_params['position_intent'] = self._format_position_intent(position_intent)

        # altering based on order class

        request: Union[OrderRequest, None] = None

        # SIMPLE ORDER
        if order_class.value == 'simple':

            if order_type.value == 'market':
                request = MarketOrderRequest(**req_params)

            elif order_type.value == 'limit':
                req_params['limit_price'] = limit_price
                request = LimitOrderRequest(**req_params)

            elif order_type.value == 'stop':
                req_params['stop_price'] = stop_price
                request = StopOrderRequest(**req_params)

            elif order_type.value == 'stop_limit':
                req_params['limit_price'] = limit_price
                req_params['stop_price'] = stop_price
                request = StopLimitOrderRequest(**req_params)

            elif order_type.value == 'trailing_stop':
                req_params['trail_price'] = trail_price
                req_params['trail_percent'] = trail_percent
                request = TrailingStopOrderRequest(**req_params)

        # BRACKET ORDER
        elif order_class.value == 'bracket':
            if not stop_price or not limit_price or not take_profit:
                raise ValueError("The bracket order must include, take profit, stop price and limit price.")

            req_params['type'] = self._format_order_type('market')

        ##################### oco and oto are not implemented in api properly #######################################


        ################## order submission and updation #########################################

        # submission
        nueva_new_order = self.trade_client.submit_order(order_data = request)

        # add to orders df
        self.process_and_add_order(nueva_new_order)

    ###################### trade updates ##########################
    async def _update_handler(self, response: Dict):

        if response.get('data').get('event') in ['fill', 'partial_fill']:

            self.portfolio.on_order_fill(symbol = response.get('data').get('symbol'))

            # on fill delete it from orders_df
            if response.get('data').get('event') == 'fill':
                self.orders_df = self.orders_df.filter(
                    pl.col('id') != response.get('data').get('id')
                )

        else:

            # remove the order if it can never be executed
            if response.get('data').get('event') in ['canceled', 'expired', 'rejected', 'suspended']:
                log.info(f"Order response for {response.get('data').get('symbol')} is "
                         f"{response.get('data').get('event')} and is being removed from active orders.")

                self.orders_df: pl.DataFrame = self.orders_df.filter(
                    (pl.col('id') != response.get('data').get('id'))
                )

                return

            else:

                new_order_dict = self.process_and_add_order(add = False)

                # add the new_order dict replacing the id if it already exists
                self.orders_df = pl.concat(
                    [self.orders_df.filter( pl.col('id') != new_order_dict.get('id') ), pl.DataFrame(new_order_dict)],
                    how='vertical'
                )
        pass

    def take_updates(self):
        self._trade_stream = TradeStream(api_key = self._api_key, secret_key = self._secret_key)
        self._trade_stream.subscribe_trade_updates(handler = self._update_handler)
        self._trade_stream.run()

    def stop_taking_updates(self):
        self._trade_stream.stop()

    def cancel_order(self, id: Union[UUID, str]):
        self.trade_client.cancel_order_by_id(id)
        # this call of api should ideally return cancel order response but it is not implemeted in api
        # once that it done this part needs to be modified
        # although ig this will be handled due to trade updates

    def update_order(self, id: Union[UUID, str], qty: int = 0, time_in_force: str = None, limit_price: float = 0.0,
                     stop_price: float = 0.0, trail: float = 0.0):

        # check if order exists
        tmp = self.orders_df.filter(
            pl.col('id') == id
        )

        if tmp.is_empty():
            warnings.warn("No such order exists", UserWarning)
            log.warning("Order ID given for cancellation does not exist.")
            return

        # if asset type mentioned
        order_type = tmp.filter( pl.col('type') ).to_series()[0]

        # req dict
        req_dict = {}
        if time_in_force:
            time_in_force = self._format_time_in_force(time_in_force)
            req_dict['time_in_force'] = time_in_force

        # load price acc to orders
        if qty:
            req_dict['qty'] = qty

        if order_type == 'market':
            pass

        elif order_type == 'limit':
            if limit_price:
                req_dict['limit_price'] = limit_price

        elif order_type == 'stop':
            if stop_price:
                req_dict['stop_price'] = stop_price

        elif order_type == 'stop_limit':
            if limit_price:
                req_dict['limit_price'] = limit_price
            if stop_price:
                req_dict['stop_price'] = stop_price

        elif order_type == 'trailing_stop':
            if trail:
                req_dict['trail'] = trail

        # prepare req
        req = ReplaceOrderRequest(**req_dict)
        # many order modifications can be done here, but is not required in the basic version

        # check what error is given if given
        try:
            new_order = self.trade_client.replace_order_by_id(order_id=id, order_data=req)
        except Exception as e:
            log.error(f"error in replacing order: {e}")
            raise warnings.warn(f"error in replacing order: {e}")
        new_order = self.process_and_add_order(new_order, add=False)
        self.orders_df = pl.concat( [self.orders_df.filter( pl.col('id') != id ), pl.DataFrame(new_order)],
                                    how='vertical')


    # adds stop loss to existing order, ie places limit or stop sell order... figure it out
    def add_stop_loss(self):
        pass





if __name__ == '__main__':
    api_key = "PK1W74P0496MXUUAD8NC"
    secret_key = "wJH4Zg4OSwCRplkxzhWHp21YMb7QvzHviXxRwxAF"
    trade_client = TradingClient(api_key, secret_key)
    portfolio = PORTFOLIO(trade_client)
    o = ORDERS(api_key=api_key, secret_key=secret_key, portfolio=portfolio, trade_client=trade_client)










