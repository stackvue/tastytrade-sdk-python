import os
from enum import Enum
from typing import List


class EventType(str, Enum):
    """
    This is an :class:`~enum.Enum` that contains the valid subscription types
    for the data streamer.

    Information on different types of events, their uses and their properties
    can be found at the `dxfeed Knowledge Base.
    <https://kb.dxfeed.com/en/data-model/dxfeed-api-market-events.html>`_.
    """

    CANDLE = 'Candle'
    GREEKS = 'Greeks'
    QUOTE = 'Quote'
    TRADE = 'Trade'


DEFAULT_QUOTE_FIELDS = ['eventSymbol', 'eventTime', 'sequence', 'timeNanoPart', 'bidTime', 'bidExchangeCode', 'askTime',
                        'askExchangeCode', 'bidPrice', 'askPrice', 'bidSize', 'askSize']
DEFAULT_CANDLE_FIELDS = ['eventSymbol', 'eventTime', 'eventFlags', 'index', 'time', 'sequence', 'count', 'open', 'high',
                         'low', 'close', 'volume', 'vwap', 'bidVolume', 'askVolume', 'impVolatility', 'openInterest']
DEFAULT_TRADE_FIELDS = ['eventSymbol', 'eventTime', 'time', 'timeNanoPart', 'sequence', 'exchangeCode', 'dayId',
                        'tickDirection', 'extendedTradingHours', 'price', 'change', 'size', 'dayVolume', 'dayTurnover']
DEFAULT_GREEKS_FIELDS = ['eventSymbol', 'eventTime', 'eventFlags', 'index', 'time', 'sequence', 'price', 'volatility',
                         'delta', 'gamma', 'theta', 'rho', 'vega']

QUOTE_FIELDS = os.getenv('QUOTE_FIELDS', DEFAULT_QUOTE_FIELDS)
CANDLE_FIELDS = os.getenv('CANDLE_FIELDS', DEFAULT_CANDLE_FIELDS)
TRADE_FIELDS = os.getenv('TRADE_FIELDS', DEFAULT_TRADE_FIELDS)
GREEKS_FIELDS = os.getenv('GREEKS_FIELDS', DEFAULT_GREEKS_FIELDS)

FIELD_MAPPINGS = {
    EventType.QUOTE: QUOTE_FIELDS,
    EventType.CANDLE: CANDLE_FIELDS,
    EventType.TRADE: TRADE_FIELDS,
    EventType.GREEKS: GREEKS_FIELDS
}


def event_from_stream(data: list, msg_type: EventType) -> List['dict']:  # pragma: no cover
    """
    Makes a list of event objects from a list of raw trade data fetched by
    a :class:`~tastyworks.streamer.DXFeedStreamer`.

    :param data: list of raw quote data from streamer
    :param msg_type: enum `EventType`

    :return: list of event objects from data
    """
    objs = []
    keys = FIELD_MAPPINGS[msg_type]
    size = len(keys)
    multiples = len(data) / size
    if not multiples.is_integer():
        msg = 'Mapper data input values are not a multiple of the key size'
        raise RuntimeError(msg)
    for i in range(int(multiples)):
        offset = i * size
        local_values = data[offset:(i + 1) * size]
        event_dict = dict(zip(keys, local_values))
        objs.append(event_dict)
    return objs
