from decimal import Decimal

from hummingbot.client.config.config_validators import (
    validate_connector,
    validate_decimal,
    validate_derivative,
    validate_int,
)
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.settings import required_exchanges, requried_connector_trading_pairs


def exchange_on_validated(value: str) -> None:
    required_exchanges.add(value)


def tokens_on_validated(value: str) -> None:
    tokens = [t.strip() for t in value.split(",")]
    spot_conn = spot_perpetual_funding_arbitrage_config_map.get("spot_connector").value
    perp_conn = spot_perpetual_funding_arbitrage_config_map.get("perpetual_connector").value

    spot_quote = _get_quote(spot_conn)
    perp_quote = _get_quote(perp_conn)

    spot_pairs = [f"{t}-{spot_quote}" for t in tokens]
    perp_pairs = [f"{t}-{perp_quote}" for t in tokens]

    requried_connector_trading_pairs[spot_conn] = spot_pairs
    requried_connector_trading_pairs[perp_conn] = perp_pairs


def _get_quote(connector: str) -> str:
    if connector in ("binance_perpetual", "binance"):
        return "USDT"
    if "hyperliquid" in connector:
        return "USD"
    return "USDT"


spot_perpetual_funding_arbitrage_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="spot_perpetual_funding_arbitrage"),
    "spot_connector": ConfigVar(
        key="spot_connector",
        prompt="Enter a spot connector (Exchange/AMM/CLOB) >>> ",
        prompt_on_new=True,
        validator=validate_connector,
        on_validated=exchange_on_validated),
    "perpetual_connector": ConfigVar(
        key="perpetual_connector",
        prompt="Enter a derivative connector >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "tokens": ConfigVar(
        key="tokens",
        prompt="Enter the tokens to trade, separated by commas (e.g. BTC,ETH) >>> ",
        prompt_on_new=True,
        type_str="str",
        on_validated=tokens_on_validated),
    "total_order_amount_quote": ConfigVar(
        key="total_order_amount_quote",
        prompt="Total quote asset (e.g. USDT) to allocate across all tokens. "
               "Will be split proportionally by funding rate. >>> ",
        type_str="decimal",
        prompt_on_new=True,
        default=Decimal("100")),
    "perpetual_leverage": ConfigVar(
        key="perpetual_leverage",
        prompt="How much leverage on the perpetual exchange? (1 = 1X) >>> ",
        type_str="int",
        default=1,
        validator=lambda v: validate_int(v),
        prompt_on_new=True),
    "min_funding_rate_pct": ConfigVar(
        key="min_funding_rate_pct",
        prompt="Minimum funding rate (%) to open a position. "
               "(e.g. 0.01 means funding rate >= 0.01% triggers entry) >>> ",
        prompt_on_new=True,
        default=Decimal("0.01"),
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False),
        type_str="decimal"),
    "exit_funding_rate_pct": ConfigVar(
        key="exit_funding_rate_pct",
        prompt="At what funding rate (%) should the position be closed? "
               "(e.g. 0.001 means exit when funding rate drops to 0.001%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.001"),
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True),
        type_str="decimal"),
    "take_profit_pct": ConfigVar(
        key="take_profit_pct",
        prompt="Take profit percentage of position value. "
               "(e.g. 1.0 means close when total PnL >= 1% of position value) >>> ",
        prompt_on_new=True,
        default=Decimal("1.0"),
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False),
        type_str="decimal"),
    "stop_loss_pct": ConfigVar(
        key="stop_loss_pct",
        prompt="Stop loss percentage of position value. "
               "(e.g. -0.5 means close when total PnL <= -0.5%. Enter a negative value) >>> ",
        prompt_on_new=True,
        default=Decimal("-0.5"),
        validator=lambda v: validate_decimal(v, Decimal(-100), Decimal(0), inclusive=False),
        type_str="decimal"),
    "spot_market_slippage_buffer": ConfigVar(
        key="spot_market_slippage_buffer",
        prompt="Slippage buffer on spot orders (Enter 1 for 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "perpetual_market_slippage_buffer": ConfigVar(
        key="perpetual_market_slippage_buffer",
        prompt="Slippage buffer on perpetual orders (Enter 1 for 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "next_arbitrage_opening_delay": ConfigVar(
        key="next_arbitrage_opening_delay",
        prompt="Delay before opening the next position after closing (in seconds) >>> ",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False),
        default=120),
    "min_holding_hours": ConfigVar(
        key="min_holding_hours",
        prompt="Minimum holding time (hours) before exit due to funding rate drop. "
               "(e.g. 4 means hold at least 4h. TP/SL always apply.) >>> ",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=True),
        default=4),
    "reopen_cooldown_hours": ConfigVar(
        key="reopen_cooldown_hours",
        prompt="Cooldown time (hours) after closing before re-entering. >>> ",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=True),
        default=2),
    "max_entry_spread_pct": ConfigVar(
        key="max_entry_spread_pct",
        prompt="Max spot-perp spread (%) allowed when entering. "
               "(e.g. 0.05 means reject if spread > 0.05%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False),
        type_str="decimal"),
    "max_spread_to_funding_ratio": ConfigVar(
        key="max_spread_to_funding_ratio",
        prompt="Max ratio of entry spread to funding rate. "
               "(e.g. 3 means reject if spread/funding > 3 cycles to recover) >>> ",
        prompt_on_new=True,
        default=Decimal("3"),
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False),
        type_str="decimal"),
    "max_exit_spread_pct": ConfigVar(
        key="max_exit_spread_pct",
        prompt="Max exit spread (%) when closing. Stop loss ignores this. >>> ",
        prompt_on_new=True,
        default=Decimal("0.08"),
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False),
        type_str="decimal"),
    "check_interval_seconds": ConfigVar(
        key="check_interval_seconds",
        prompt="How often (in seconds) should the strategy check for new entry opportunities? "
               "(e.g. 30 = check every 30 seconds. Monitoring of open positions always runs every second.) >>> ",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=1, inclusive=True),
        default=30),
    "health_check_interval_seconds": ConfigVar(
        key="health_check_interval_seconds",
        prompt="How often (in seconds) to run the health check that detects and closes "
               "one-sided/orphaned positions? (e.g. 60 = every 60 seconds) >>> ",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=10, inclusive=True),
        default=60),
}
