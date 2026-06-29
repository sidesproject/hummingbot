import asyncio
import time
from typing import TYPE_CHECKING, Optional

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_auth import AsterPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import (
        AsterPerpetualDerivative,
    )


class AsterPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800
    HEARTBEAT_TIME_INTERVAL = 30.0
    LISTEN_KEY_RETRY_INTERVAL = 5.0
    MAX_RETRIES = 3
    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: AsterPerpetualAuth,
            connector: 'AsterPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._connector = connector
        self._current_listen_key = None
        self._last_listen_key_ping_ts = None
        self._manage_listen_key_task = None
        self._listen_key_initialized_event = asyncio.Event()

    async def _get_ws_assistant(self) -> WSAssistant:
        return await self._api_factory.get_ws_assistant()

    async def _get_listen_key(self, max_retries: int = MAX_RETRIES) -> str:
        retry_count = 0
        backoff_time = 1.0
        while True:
            try:
                data = await self._connector._api_post(
                    path_url=CONSTANTS.LISTEN_KEY_URL,
                    is_auth_required=True,
                )
                return data["listenKey"]
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                retry_count += 1
                if retry_count > max_retries:
                    raise IOError(
                        f"Error fetching user stream listen key after {max_retries} retries. Error: {exception}")
                self.logger().warning(
                    f"Retry {retry_count}/{max_retries} fetching user stream listen key. Error: {repr(exception)}")
                await self._sleep(backoff_time)
                backoff_time *= 2

    async def _ping_listen_key(self) -> bool:
        try:
            data = await self._connector._api_put(
                path_url=CONSTANTS.LISTEN_KEY_URL,
                params={"listenKey": self._current_listen_key},
                is_auth_required=True,
                return_err=True)
            if "code" in data:
                self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {data}")
                return False
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {exception}")
            return False
        return True

    async def _manage_listen_key_task_loop(self):
        self.logger().info("Starting listen key management task...")
        while True:
            try:
                now = int(time.time())
                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self._last_listen_key_ping_ts = now
                    self._listen_key_initialized_event.set()
                    self.logger().info(f"Successfully obtained listen key {self._current_listen_key}")
                if now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    success = await self._ping_listen_key()
                    if success:
                        self.logger().info(f"Successfully refreshed listen key {self._current_listen_key}")
                        self._last_listen_key_ping_ts = now
                    else:
                        self.logger().error(
                            f"Failed to refresh listen key {self._current_listen_key}. Getting new key...")
                        raise
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)
            except asyncio.CancelledError:
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                raise
            except Exception as e:
                self.logger().error(f"Error occurred renewing listen key ... {e}")
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)

    async def _ensure_listen_key_task_running(self):
        if self._manage_listen_key_task is not None and not self._manage_listen_key_task.done():
            return
        if self._manage_listen_key_task is not None:
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._ensure_listen_key_task_running()
        await self._listen_key_initialized_event.wait()
        ws = await self._get_ws_assistant()
        url = f"{web_utils.wss_url(CONSTANTS.PRIVATE_WS_ENDPOINT, self._domain)}?listenKey={self._current_listen_key}"
        self.logger().info(f"Connecting to user stream with listen key {self._current_listen_key}")
        await ws.connect(ws_url=url, ping_timeout=self.HEARTBEAT_TIME_INTERVAL)
        self.logger().info("Successfully connected to user stream")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        pass  # No explicit subscription needed for listen key

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        self.logger().info("User stream interrupted. Cleaning up...")
        if self._manage_listen_key_task and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._manage_listen_key_task = None
        websocket_assistant and await websocket_assistant.disconnect()
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()
