from decimal import Decimal

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.spot_perpetual_funding_arbitrage.spot_perpetual_funding_arbitrage import (
    SpotPerpetualFundingArbitrageStrategy,
)
from hummingbot.strategy.spot_perpetual_funding_arbitrage.spot_perpetual_funding_arbitrage_config_map import (
    spot_perpetual_funding_arbitrage_config_map,
)


_QUOTE_MAP = {
    "binance": "USDT",
    "binance_perpetual": "USDT",
    "bybit": "USDT",
    "bybit_perpetual": "USDT",
    "okx": "USDT",
    "okx_perpetual": "USDT",
    "hyperliquid_perpetual": "USD",
}


async def start(self):
    spot_connector = spot_perpetual_funding_arbitrage_config_map.get("spot_connector").value.lower()
    perpetual_connector = spot_perpetual_funding_arbitrage_config_map.get("perpetual_connector").value.lower()
    tokens_raw = spot_perpetual_funding_arbitrage_config_map.get("tokens").value
    tokens = [t.strip() for t in tokens_raw.split(",")]

    total_order_amount_quote = spot_perpetual_funding_arbitrage_config_map.get("total_order_amount_quote").value
    perpetual_leverage = spot_perpetual_funding_arbitrage_config_map.get("perpetual_leverage").value
    # Percent params: config values are in "percentage as typed" (e.g. 0.05 = 0.05%).
    # Strategy code already handles them in percentage form, so pass directly.
    min_funding_rate_pct = spot_perpetual_funding_arbitrage_config_map.get("min_funding_rate_pct").value
    exit_funding_rate_pct = spot_perpetual_funding_arbitrage_config_map.get("exit_funding_rate_pct").value
    take_profit_pct = spot_perpetual_funding_arbitrage_config_map.get("take_profit_pct").value
    stop_loss_pct = spot_perpetual_funding_arbitrage_config_map.get("stop_loss_pct").value
    max_entry_spread_pct = spot_perpetual_funding_arbitrage_config_map.get("max_entry_spread_pct").value
    max_exit_spread_pct = spot_perpetual_funding_arbitrage_config_map.get("max_exit_spread_pct").value
    # Slippage params: used as multipliers (1 + buffer), so /100 is correct
    spot_slippage = spot_perpetual_funding_arbitrage_config_map.get("spot_market_slippage_buffer").value / Decimal("100")
    perp_slippage = spot_perpetual_funding_arbitrage_config_map.get("perpetual_market_slippage_buffer").value / Decimal("100")
    max_spread_to_funding_ratio = spot_perpetual_funding_arbitrage_config_map.get("max_spread_to_funding_ratio").value
    next_delay = spot_perpetual_funding_arbitrage_config_map.get("next_arbitrage_opening_delay").value
    min_holding_hours = spot_perpetual_funding_arbitrage_config_map.get("min_holding_hours").value
    reopen_cooldown_hours = spot_perpetual_funding_arbitrage_config_map.get("reopen_cooldown_hours").value
    check_interval_seconds = spot_perpetual_funding_arbitrage_config_map.get("check_interval_seconds").value
    health_check_interval_seconds = spot_perpetual_funding_arbitrage_config_map.get("health_check_interval_seconds").value

    spot_quote = _QUOTE_MAP.get(spot_connector, "USDT")
    perp_quote = _QUOTE_MAP.get(perpetual_connector, "USDT")

    spot_pairs = [f"{t}-{spot_quote}" for t in tokens]
    perp_pairs = [f"{t}-{perp_quote}" for t in tokens]

    await self.initialize_markets([(spot_connector, spot_pairs), (perpetual_connector, perp_pairs)])

    first_spot_pair = spot_pairs[0]
    first_perp_pair = perp_pairs[0]
    base_1, quote_1 = first_spot_pair.split("-")
    base_2, quote_2 = first_perp_pair.split("-")

    spot_market_info = MarketTradingPairTuple(
        self.markets[spot_connector], first_spot_pair, base_1, quote_1,
    )
    perpetual_market_info = MarketTradingPairTuple(
        self.markets[perpetual_connector], first_perp_pair, base_2, quote_2,
    )

    self.market_trading_pair_tuples = [spot_market_info, perpetual_market_info]
    self.strategy = SpotPerpetualFundingArbitrageStrategy()
    self.strategy.init_params(
        spot_market_info,
        perpetual_market_info,
        tokens,
        total_order_amount_quote,
        perpetual_leverage,
        min_funding_rate_pct,
        exit_funding_rate_pct,
        take_profit_pct,
        stop_loss_pct,
        spot_slippage,
        perp_slippage,
        next_delay,
        min_holding_hours,
        reopen_cooldown_hours,
        max_entry_spread_pct,
        max_spread_to_funding_ratio,
        max_exit_spread_pct,
        check_interval_seconds,
        health_check_interval_seconds,
    )
