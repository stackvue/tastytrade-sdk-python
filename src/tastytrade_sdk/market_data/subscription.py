import logging
import threading
import time
from itertools import product
from math import floor
from typing import Callable, Optional

import ujson
from websockets.exceptions import ConnectionClosedOK
from websockets.sync.client import connect, ClientConnection

from tastytrade_sdk.exceptions import TastytradeSdkException, InvalidArgument
from tastytrade_sdk.market_data.event import FIELD_MAPPINGS, EventType, event_from_stream
from tastytrade_sdk.market_data.streamer_symbol_translation import StreamerSymbolTranslations


class LoopThread(threading.Thread):
    def __init__(self, activity: Callable, timeout_seconds: int = 0):
        threading.Thread.__init__(self)
        self.__running = True
        self.__activity = activity
        self.__timeout_seconds = timeout_seconds
        super().start()

    def run(self):
        while self.__running:
            self.__activity()
            self.__pause()

    def __pause(self):
        if not self.__timeout_seconds:
            return
        start = time.time()
        while self.__running and time.time() - start <= self.__timeout_seconds:
            continue

    def stop(self):
        self.__running = False


class Subscription:
    __websocket: Optional[ClientConnection] = None
    __keepalive_thread: Optional[LoopThread]
    __receive_thread: Optional[LoopThread]
    __is_authorized: bool = False

    def __init__(self, url: str, token: str, subscriptions: list[dict],
                 streamer_symbol_translations: StreamerSymbolTranslations,
                 on_candle: Callable[[list[dict]], None] = None,
                 on_greeks: Callable[[list[dict]], None] = None,
                 on_quote: Callable[[list[dict]], None] = None,
                 on_trade: Callable[[list[dict]], None] = None
                 ):
        """@private"""

        if not (on_quote or on_candle or on_greeks or on_trade):
            raise InvalidArgument('At least one feed event handler must be provided')

        self.__url = url
        self.__token = token
        self.__subscriptions = subscriptions
        self.__streamer_symbol_translations = streamer_symbol_translations
        self.__on_quote = on_quote
        self.__on_candle = on_candle
        self.__on_greeks = on_greeks
        self.__on_trade = on_trade

    def open(self) -> 'Subscription':
        """Start listening for feed events"""
        self.__websocket = connect(self.__url)
        self.__receive_thread = LoopThread(self.__receive)

        if self.__subscriptions is not None:
            subscriptions = self.__subscriptions
        else:
            subscription_types = []
            if self.__on_quote:
                subscription_types.append('Quote')
            if self.__on_candle:
                subscription_types.append('Candle')
            if self.__on_greeks:
                subscription_types.append('Greeks')
            if self.__on_trade:
                subscription_types.append('Trade')

            subscriptions = [{'symbol': s, 'type': t} for s, t in
                             product(self.__streamer_symbol_translations.streamer_symbols, subscription_types)]

        self.__send('SETUP', version='0.1-js/1.0.0')
        self.__send('AUTH', token=self.__token)
        while not self.__is_authorized:
            continue
        self.__send('CHANNEL_REQUEST', channel=1, service='FEED', parameters={'contract': 'AUTO'})
        self.__send('FEED_SETUP', channel=1, acceptAggregationPeriod=10, acceptDataFormat='COMPACT',
                    acceptEventFields=FIELD_MAPPINGS)
        self.__send('FEED_SUBSCRIPTION', channel=1, add=subscriptions)
        return self

    def close(self) -> None:
        """Close the stream connection"""
        if self.__keepalive_thread:
            self.__keepalive_thread.stop()
        if self.__receive_thread:
            self.__receive_thread.stop()
        if self.__websocket:
            self.__websocket.close()

    def __receive(self) -> None:
        if not self.__websocket:
            return
        try:
            message = ujson.loads(self.__websocket.recv())
        except ConnectionClosedOK:
            return
        _type = message['type']
        if _type == 'ERROR':
            raise StreamerException(message['error'], message['message'])
        if _type == 'SETUP':
            keepalive_interval = floor(message['keepaliveTimeout'] / 2)
            self.__keepalive_thread = LoopThread(lambda: self.__send('KEEPALIVE'), keepalive_interval)
        elif _type == 'AUTH_STATE':
            self.__is_authorized = message['state'] == 'AUTHORIZED'
        elif _type == 'FEED_DATA':
            self._map_message(message['data'])
        else:
            logging.debug('Unhandled message type: %s', _type)

    def _map_message(self, message) -> None:
        """
                Takes the raw JSON data, parses the events and places them into their
                respective callbacks.

                :param message: raw JSON data from the websocket
                """
        logging.debug('received message: %s', message)
        if isinstance(message[0], str):
            msg_type = message[0]
        else:
            msg_type = message[0][0]
        data = message[1]
        events = event_from_stream(data, msg_type)
        self.__handle_feed_event(events, msg_type)
        # parse type or warn for unknown type

    def __handle_feed_event(self, events: list[dict], msg_type: EventType) -> None:
        event_type = msg_type
        if event_type == EventType.QUOTE and self.__on_quote:
            self.__on_quote(events)
        elif event_type == EventType.CANDLE and self.__on_candle:
            self.__on_candle(events)
        elif event_type == EventType.GREEKS and self.__on_greeks:
            self.__on_greeks(events)
        elif event_type == EventType.TRADE and self.__on_trade:
            self.__on_trade(events)
        else:
            logging.debug('Unhandled feed event type %s for symbol %s', event_type, original_symbol)

    def __send(self, _type: str, channel: Optional[int] = 0, **kwargs) -> None:
        self.__websocket.send(ujson.dumps({
            **{'type': _type, 'channel': channel},
            **kwargs
        }))


class StreamerException(TastytradeSdkException):
    def __init__(self, error: str, message: str):
        super().__init__(f'{error}: {message}')
