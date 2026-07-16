from typing import Callable, Optional

import hummingbot.connector.exchange.binance.binance_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _is_cross_margin(domain: str) -> bool:
    return domain == CONSTANTS.CROSS_MARGIN_DOMAIN


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the Binance domain to connect to
    :return: the full URL to the endpoint
    """
    # Public endpoints always use api.binance.com even for cross margin
    actual_domain = "com"
    return CONSTANTS.REST_URL.format(actual_domain) + CONSTANTS.PUBLIC_API_VERSION + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: the Binance domain to connect to
    :return: the full URL to the endpoint
    """
    if _is_cross_margin(domain):
        # Unified account: trading goes through /margin/order,
        # account-level endpoints (balance, listenKey, myTrades) go through papi/ directly
        clean = path_url.lstrip("/")
        papi_direct_prefixes = ("balance", "listenKey")
        if clean in papi_direct_prefixes or \
           any(clean.endswith("/" + p) or clean == p for p in papi_direct_prefixes):
            return CONSTANTS.PAPI_BASE_URL + clean
        return CONSTANTS.CROSS_MARGIN_REST_URL + clean
    return CONSTANTS.REST_URL.format(domain) + CONSTANTS.PRIVATE_API_VERSION + path_url


class BinanceRESTPreProcessor(RESTPreProcessorBase):
    """Override framework default: papi margin/order requires form-encoded, not JSON."""

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        if request.method == RESTMethod.POST:
            if request.headers is None:
                request.headers = {}
            request.headers["Content-Type"] = "application/x-www-form-urlencoded"
        return request


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None, ) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))
    pre_processors = [
        TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
    ]
    if _is_cross_margin(domain):
        pre_processors.append(BinanceRESTPreProcessor())
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=pre_processors,
    )
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    server_time = response["serverTime"]
    return server_time
