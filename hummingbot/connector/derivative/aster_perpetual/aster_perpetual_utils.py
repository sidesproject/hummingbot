from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr, field_validator

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0004"),
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

BROKER_ID = "HBOT"


class AsterPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "aster_perpetual"
    aster_main_address: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your main wallet address (e.g., 0x...)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_trading_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your API trading wallet private key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_chain_id: int = Field(
        default=1666,
        json_schema_extra={
            "prompt": "Enter the chain ID (1666=Aster mainnet, 714=Aster testnet)",
            "is_secure": False, "is_connect_key": True, "prompt_on_new": True}
    )

    @field_validator("aster_main_address", mode="before")
    @classmethod
    def validate_address(cls, value: str):
        if isinstance(value, str):
            return value.strip().lower()
        return value


KEYS = AsterPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["aster_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"aster_perpetual_testnet": "aster_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"aster_perpetual_testnet": "BTC-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"aster_perpetual_testnet": [0.02, 0.04]}


class AsterPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "aster_perpetual_testnet"
    aster_perpetual_testnet_main_address: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your main wallet address (e.g., 0x...)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_testnet_trading_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your API trading wallet private key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_testnet_chain_id: int = Field(
        default=714,
        json_schema_extra={
            "prompt": "Enter the chain ID (714 for Aster testnet)",
            "is_secure": False, "is_connect_key": True, "prompt_on_new": True}
    )
    model_config = ConfigDict(title="aster_perpetual")

    @field_validator("aster_perpetual_testnet_main_address", mode="before")
    @classmethod
    def validate_address(cls, value: str):
        if isinstance(value, str):
            return value.strip().lower()
        return value


OTHER_DOMAINS_KEYS = {"aster_perpetual_testnet": AsterPerpetualTestnetConfigMap.model_construct()}
