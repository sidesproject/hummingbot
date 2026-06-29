from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "aster_perpetual"
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "aster_perpetual_testnet"

PERPETUAL_BASE_URL = "https://fapi.asterdex.com/fapi/"
TESTNET_BASE_URL = "https://fapi.asterdex-testnet.com/fapi/"

PERPETUAL_WS_URL = "wss://fstream.asterdex.com/"
TESTNET_WS_URL = "wss://fstream.asterdex-testnet.com/"

PUBLIC_WS_ENDPOINT = "stream"   # wss://fstream.asterdex.com/stream?streams=... (combined)
MARKET_WS_ENDPOINT = "stream"   # same host, combined streams for aggTrade / markPrice
PRIVATE_WS_ENDPOINT = "ws"      # wss://fstream.asterdex.com/ws/<listenKey>

# EIP-712 Signing Domain
EIP712_DOMAIN_NAME = "AsterSignTransaction"
EIP712_DOMAIN_VERSION = "1"
EIP712_VERIFYING_CONTRACT = "0x0000000000000000000000000000000000000000"
DEFAULT_CHAIN_ID = 1666
TESTNET_CHAIN_ID = 714

# Time in force
TIME_IN_FORCE_GTC = "GTC"
TIME_IN_FORCE_GTX = "GTX"
TIME_IN_FORCE_IOC = "IOC"
TIME_IN_FORCE_FOK = "FOK"

# Public API v3 Endpoints
SNAPSHOT_REST_URL = "v3/depth"
TICKER_PRICE_URL = "v3/ticker/bookTicker"
TICKER_PRICE_CHANGE_URL = "v3/ticker/24hr"
EXCHANGE_INFO_URL = "v3/exchangeInfo"
RECENT_TRADES_URL = "v3/trades"
PING_URL = "v3/ping"
MARK_PRICE_URL = "v3/premiumIndex"
SERVER_TIME_PATH_URL = "v3/time"
FUNDING_RATE_URL = "v3/fundingRate"

# Private API v3 Endpoints
ORDER_URL = "v3/order"
CANCEL_ALL_OPEN_ORDERS_URL = "v3/allOpenOrders"
SET_LEVERAGE_URL = "v3/leverage"
LISTEN_KEY_URL = "v3/listenKey"
CHANGE_POSITION_MODE_URL = "v3/positionSide/dual"

POST_POSITION_MODE_LIMIT_ID = f"POST{CHANGE_POSITION_MODE_URL}"
GET_POSITION_MODE_LIMIT_ID = f"GET{CHANGE_POSITION_MODE_URL}"

# Private API v2 Endpoints
ACCOUNT_INFO_URL = "v2/account"
POSITION_INFORMATION_URL = "v2/positionRisk"

# Private API v1 Endpoints
ACCOUNT_TRADE_LIST_URL = "v1/userTrades"
GET_INCOME_HISTORY_URL = "v1/income"

# Funding Settlement Time Span
FUNDING_SETTLEMENT_DURATION = (0, 30)

# Order Statuses
ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "CANCELED": OrderState.CANCELED,
    "EXPIRED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED_IN_MATCH": OrderState.FAILED,
}

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS_1MIN = "ORDERS_1MIN"
ORDERS_1SEC = "ORDERS_1SEC"

DIFF_STREAM_ID = 1
TRADE_STREAM_ID = 2
FUNDING_INFO_STREAM_ID = 3
HEARTBEAT_TIME_INTERVAL = 30.0

# Rate Limit time intervals
ONE_HOUR = 3600
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 2400

RATE_LIMITS = [
    # Pool Limits
    RateLimit(limit_id=REQUEST_WEIGHT, limit=2400, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_1MIN, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_1SEC, limit=300, time_interval=10),
    # Weight Limits for individual endpoints
    RateLimit(limit_id=SNAPSHOT_REST_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=20)]),
    RateLimit(limit_id=TICKER_PRICE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=2)]),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=EXCHANGE_INFO_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=40)]),
    RateLimit(limit_id=RECENT_TRADES_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=LISTEN_KEY_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=PING_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ORDER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1),
                             LinkedLimitWeightPair(ORDERS_1MIN, weight=1),
                             LinkedLimitWeightPair(ORDERS_1SEC, weight=1)]),
    RateLimit(limit_id=CANCEL_ALL_OPEN_ORDERS_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ACCOUNT_TRADE_LIST_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=5)]),
    RateLimit(limit_id=SET_LEVERAGE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=GET_INCOME_HISTORY_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=30)]),
    RateLimit(limit_id=POST_POSITION_MODE_LIMIT_ID, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=GET_POSITION_MODE_LIMIT_ID, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=30)]),
    RateLimit(limit_id=ACCOUNT_INFO_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=5)]),
    RateLimit(limit_id=POSITION_INFORMATION_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=5)]),
    RateLimit(limit_id=MARK_PRICE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=FUNDING_RATE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
]

ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"
