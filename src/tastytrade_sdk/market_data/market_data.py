from typing import List, Callable

from injector import inject

from tastytrade_sdk.api import Api
from tastytrade_sdk.market_data.streamer_symbol_translation import StreamerSymbolTranslationsFactory
from tastytrade_sdk.market_data.subscription import Subscription


class MarketData:
    """
    Submodule for streaming market data
    """

    @inject
    def __init__(self, api: Api, streamer_symbol_translations_factory: StreamerSymbolTranslationsFactory):
        """@private"""
        self.__api = api
        self.__streamer_symbol_translations_factory = streamer_symbol_translations_factory

    def subscribe(self,
                  subscriptions: List[dict] = None,
                  symbols: List[str] = None,
                  on_candle: Callable[[list[dict]], None] = None,
                  on_greeks: Callable[[list[dict]], None] = None,
                  on_quote: Callable[[list[dict]], None] = None,
                  on_trade: Callable[[list[dict]], None] = None
                  ) -> Subscription:
        """
        Subscribe to live feed data
        :param subscriptions: Subscription dict as per dx feed api
        :param symbols: Symbols to subscribe to. Can be across multiple instrument types.
        :param on_candle: Handler for candle events
        :param on_greeks: Handler for greeks events
        :param on_quote: Handler for quote events
        :param on_trade: Handler for trade events
        """
        data = self.__api.get('/quote-streamer-tokens')['data']
        return Subscription(
            data['dxlink-url'],
            data['token'],
            subscriptions,
            self.__streamer_symbol_translations_factory.create(symbols) if subscriptions is None and symbols else None,
            on_candle=on_candle,
            on_greeks=on_greeks,
            on_quote=on_quote,
            on_trade=on_trade
        )
