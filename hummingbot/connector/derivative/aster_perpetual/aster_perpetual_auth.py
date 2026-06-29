import time
from collections import OrderedDict
from typing import Any, Dict
from urllib.parse import urlencode

from eth_account import Account
from eth_account.messages import encode_typed_data

import hummingbot.connector.derivative.aster_perpetual.aster_perpetual_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class AsterPerpetualAuth(AuthBase):
    """
    Auth class for Aster Perpetual API V3 using EIP-712 signing.
    Signs URL-encoded parameters with the AsterSignTransaction domain.
    """

    def __init__(self, main_address: str, trading_private_key: str, chain_id: int = 1666):
        self._main_address = main_address.lower()
        self._trading_wallet = Account.from_key(trading_private_key)
        self._trading_address = self._trading_wallet.address.lower()
        self._chain_id = chain_id
        self._last_nonce_us = 0

    @property
    def trading_address(self) -> str:
        return self._trading_address

    @property
    def main_address(self) -> str:
        return self._main_address

    def _get_eip712_domain(self) -> Dict[str, Any]:
        return {
            "name": CONSTANTS.EIP712_DOMAIN_NAME,
            "version": CONSTANTS.EIP712_DOMAIN_VERSION,
            "chainId": self._chain_id,
            "verifyingContract": CONSTANTS.EIP712_VERIFYING_CONTRACT,
        }

    def _next_nonce(self) -> int:
        now_us = int(time.time() * 1_000_000)
        self._last_nonce_us = max(now_us, self._last_nonce_us + 1)
        return self._last_nonce_us

    def _sign_params(self, params: Dict[str, Any]) -> str:
        encoded_params = urlencode(params)
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [
                    {"name": "msg", "type": "string"},
                ],
            },
            "primaryType": "Message",
            "domain": self._get_eip712_domain(),
            "message": {
                "msg": encoded_params,
            },
        }
        structured_msg = encode_typed_data(typed_data)
        signed = self._trading_wallet.sign_message(structured_msg)
        return signed.signature.hex()

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if request.is_auth_required:
            timestamp = self._next_nonce()

            if request.method == RESTMethod.POST:
                import json
                params = json.loads(request.data) if request.data else {}
            else:
                params = dict(request.params or {})

            params["user"] = self._main_address
            params["signer"] = self._trading_address
            params["nonce"] = str(timestamp)

            request.params = OrderedDict(params)
            signature = self._sign_params(request.params)
            request.params["signature"] = signature

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request  # pass-through; user stream uses listen key
