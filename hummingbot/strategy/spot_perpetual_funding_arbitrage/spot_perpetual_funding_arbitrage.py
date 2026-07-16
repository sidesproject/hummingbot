import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple

import pandas as pd

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.position import Position
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    FundingPaymentCompletedEvent,
    MarketOrderFailureEvent,
    PositionModeChangeEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase

NaN = float("nan")
s_decimal_zero = Decimal(0)
spfa_logger = None


class StrategyState(Enum):
    Closed = 0
    Opening = 1
    Opened = 2
    Closing = 3


@dataclass
class TokenState:
    """Per-token state that tracks one independent arbitrage position."""
    token: str
    spot_trading_pair: str
    perp_trading_pair: str
    spot_tuple: MarketTradingPairTuple = None
    perp_tuple: MarketTradingPairTuple = None
    state: StrategyState = StrategyState.Closed
    entry_funding_rate: Decimal = Decimal(0)
    execution_purpose: str = ""  # "open" | "add" | "close"
    accumulated_funding: Decimal = Decimal(0)
    position_opened_ts: float = 0
    last_closed_ts: float = 0
    next_arbitrage_opening_ts: float = 0
    execution_tracker: Dict[str, dict] = field(default_factory=dict)
    execution_expected_count: int = 0
    execution_started_ts: float = 0  # for stuck-in-flight detection



STUCK_IN_FLIGHT_TIMEOUT = 300  # seconds after which stuck Opening/Closing is force-reset


class ExecPurpose:
    OPEN = "open"
    ADD = "add"
    CLOSE = "close"


class SpotPerpetualFundingArbitrageStrategy(StrategyPyBase):
    """
    Delta-neutral funding rate arbitrage between spot and perpetual markets.

    Supports multiple tokens simultaneously. Each token runs an independent
    state machine: when funding rate is positive, SHORT perpetual + LONG spot.

    Entry guards:
      - funding_rate >= min_funding_rate_pct
      - spot-perp spread <= max_entry_spread_pct
      - spread / funding_rate <= max_spread_to_funding_ratio

    Exit conditions (per token):
      1. Funding rate drops below exit_funding_rate_pct (after min_holding_hours)
      2. Total PnL >= take_profit_pct of position value
      3. Total PnL <= stop_loss_pct (always closes immediately, ignores spread)

    Exit guard:
      - exit spread <= max_exit_spread_pct (skipped for stop loss)
    """

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global spfa_logger
        if spfa_logger is None:
            spfa_logger = logging.getLogger(__name__)
        return spfa_logger

    def init_params(
        self,
        spot_market_info: MarketTradingPairTuple,
        perp_market_info: MarketTradingPairTuple,
        tokens: List[str],
        total_order_amount_quote: Decimal,
        perp_leverage: int,
        min_funding_rate_pct: Decimal,
        exit_funding_rate_pct: Decimal,
        take_profit_pct: Decimal,
        stop_loss_pct: Decimal,
        spot_market_slippage_buffer: Decimal = Decimal("0"),
        perp_market_slippage_buffer: Decimal = Decimal("0"),
        next_arbitrage_opening_delay: float = 120,
        min_holding_hours: float = 4,
        reopen_cooldown_hours: float = 2,
        max_entry_spread_pct: Decimal = Decimal("0.05"),
        max_spread_to_funding_ratio: Decimal = Decimal("3"),
        max_exit_spread_pct: Decimal = Decimal("0.08"),
        check_interval_seconds: float = 30,
        health_check_interval_seconds: float = 60,
        status_report_interval: float = 10,
    ):
        self._spot_market_info = spot_market_info
        self._perp_market_info = perp_market_info
        self._spot_market = spot_market_info.market
        self._perp_market = perp_market_info.market
        self._tokens = tokens
        self._total_order_amount_quote = total_order_amount_quote
        self._perp_leverage = perp_leverage
        self._min_funding_rate_pct = min_funding_rate_pct
        self._exit_funding_rate_pct = exit_funding_rate_pct
        self._take_profit_pct = take_profit_pct
        self._stop_loss_pct = stop_loss_pct
        self._spot_market_slippage_buffer = spot_market_slippage_buffer
        self._perp_market_slippage_buffer = perp_market_slippage_buffer
        self._next_arbitrage_opening_delay = next_arbitrage_opening_delay
        self._min_holding_seconds = min_holding_hours * 3600
        self._reopen_cooldown_seconds = reopen_cooldown_hours * 3600
        self._max_entry_spread_pct = max_entry_spread_pct
        self._max_spread_to_funding_ratio = max_spread_to_funding_ratio
        self._max_exit_spread_pct = max_exit_spread_pct
        self._check_interval_seconds = check_interval_seconds
        self._health_check_interval_seconds = health_check_interval_seconds
        self._status_report_interval = status_report_interval

        spot_connector_name = spot_market_info.market.name
        perp_connector_name = perp_market_info.market.name
        self._quote_map = {
            spot_connector_name: spot_market_info.quote_asset,
            perp_connector_name: perp_market_info.quote_asset,
        }

        self._token_states: Dict[str, TokenState] = {}
        for token in tokens:
            spot_pair = f"{token}-{spot_market_info.quote_asset}"
            perp_pair = f"{token}-{perp_market_info.quote_asset}"
            spot_tup = MarketTradingPairTuple(
                spot_market_info.market, spot_pair,
                token, spot_market_info.quote_asset,
            )
            perp_tup = MarketTradingPairTuple(
                perp_market_info.market, perp_pair,
                token, perp_market_info.quote_asset,
            )
            self._token_states[token] = TokenState(
                token=token,
                spot_trading_pair=spot_pair,
                perp_trading_pair=perp_pair,
                spot_tuple=spot_tup,
                perp_tuple=perp_tup,
            )

        self._all_markets_ready = False
        self._last_timestamp = 0
        self.add_markets([spot_market_info.market, perp_market_info.market])

        self._main_task = None
        self._ready_to_start = False
        self._post_ready_warmup_ticks = 0
        self._last_check_ts: float = 0
        self._last_health_check_ts: float = 0
        self._first_seen_one_sided: Dict[str, float] = {}  # token → first detection time
        self._last_log_ts: Dict[str, float] = {}
        self._position_mode_ready = False
        self._position_mode_not_ready_counter = 0
        self._trading_started = False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ts_for(self, token: str) -> TokenState:
        return self._token_states[token]

    def all_markets_ready(self):
        return all([market.ready for market in self.active_markets])

    @property
    def perp_positions(self) -> List[Position]:
        return [
            s for s in self._perp_market.account_positions.values()
            if s.amount != s_decimal_zero
        ]

    def _perp_position_for_token(self, token: str) -> Optional[Position]:
        pair = self._ts_for(token).perp_trading_pair
        for pos in self.perp_positions:
            if pos.trading_pair == pair:
                return pos
        return None

    def get_funding_info(self, trading_pair: str) -> Optional[FundingInfo]:
        try:
            return self._perp_market.get_funding_info(trading_pair)
        except Exception:
            return None

    def _calc_dynamic_allocation(self, entering_token: str, funding_rate_pct: Decimal) -> Decimal:
        """Allocate remaining capital to a token proportional to funding rate.

        Already-opened positions are NOT rebalanced. Their locked capital is
        deducted from the total. The remaining capital is distributed among
        currently eligible Closed tokens (including the one about to enter)
        in proportion to their funding rates.
        """
        remaining = self._total_order_amount_quote - self._locked_capital()
        if remaining <= s_decimal_zero:
            return s_decimal_zero

        # Sum funding rates of all eligible Closed tokens
        pool_rate = s_decimal_zero
        for token in self._tokens:
            ts = self._ts_for(token)
            if ts.state != StrategyState.Closed:
                continue
            fi = self.get_funding_info(ts.perp_trading_pair)
            if fi is None:
                continue
            fr = fi.rate * Decimal("100")
            if fr < self._min_funding_rate_pct:
                continue
            pool_rate += fr

        if pool_rate == s_decimal_zero:
            return s_decimal_zero

        share = funding_rate_pct / pool_rate
        return remaining * share

    def _quote_to_base(self, token: str, quote_amount: Decimal, price: Decimal) -> Decimal:
        raw = quote_amount / price if price != s_decimal_zero else s_decimal_zero
        ts = self._ts_for(token)

        # Quantize to perp step size (usually the coarser of the two sides)
        amt = self._perp_market.quantize_order_amount(ts.perp_trading_pair, raw)
        if amt == s_decimal_zero:
            return s_decimal_zero

        # Ensure amt satisfies perp min Qty — if raw falls between 0 and minQty,
        # quantize may floor to 0. In that case, bump to minQty.
        try:
            rule = self._perp_market.trading_rules.get(ts.perp_trading_pair)
            if rule is not None and amt < rule.min_order_size:
                amt = rule.min_order_size
        except Exception:
            pass

        # Also ensure spot quantisation yields the same amount
        spot_amt = self._spot_market.quantize_order_amount(ts.spot_trading_pair, amt)
        return max(min(amt, spot_amt), s_decimal_zero)

    def _locked_capital(self) -> Decimal:
        locked = s_decimal_zero
        for token in self._tokens:
            ts = self._ts_for(token)
            if ts.state in (StrategyState.Opened, StrategyState.Opening):
                pos = self._perp_position_for_token(token)
                if pos is not None:
                    fi = self.get_funding_info(ts.perp_trading_pair)
                    mark = fi.mark_price if fi else s_decimal_zero
                    if mark != s_decimal_zero:
                        locked += abs(pos.amount) * mark
        return locked

    def _can_afford_on_both_sides(self, token: str, order_amount: Decimal) -> bool:
        """Quick balance check before submitting orders, to avoid noisy failures.

        Detects unified margin by checking if both connectors report the same
        balance (same account pool). In that case only one est_cost is needed.
        """
        spot_quote = self._ts_for(token).spot_trading_pair.split("-")[1]
        perp_quote = self._ts_for(token).perp_trading_pair.split("-")[1]

        spot_bal = self._spot_market.get_available_balance(spot_quote)
        perp_bal = self._perp_market.get_available_balance(perp_quote)

        # Same balance on both sides = unified margin (shared account pool)
        unified_margin = (spot_bal > s_decimal_zero and spot_bal == perp_bal)

        fi = self.get_funding_info(self._ts_for(token).perp_trading_pair)
        if fi is None:
            return False
        est_cost = order_amount * fi.index_price * Decimal("1.01")

        if unified_margin:
            if spot_bal < est_cost:
                return False
        else:
            if spot_bal < est_cost:
                return False
            if perp_bal < est_cost:
                return False
        return True

    def apply_initial_settings(self):
        for token in self._tokens:
            pair = self._ts_for(token).perp_trading_pair
            self._perp_market.set_leverage(pair, self._perp_leverage)
        self._perp_market.set_position_mode(PositionMode.ONEWAY)

    def _resume_existing_positions(self):
        """Scan exchange positions and restore token states after restart."""
        for token in self._tokens:
            ts = self._ts_for(token)
            pos = self._perp_position_for_token(token)
            if pos is None or pos.amount == s_decimal_zero:
                continue

            amount = abs(pos.amount)
            spot_bal = self._spot_market.get_available_balance(token)
            if pos.amount < 0:
                # Existing SHORT — check if spot hedge is present
                if spot_bal == s_decimal_zero:
                    self.logger().warning(
                        f"[{token}] Found existing SHORT position ({amount:.6f}) but NO spot "
                        f"{token} balance. Hedge is broken — keeping Closed, health check "
                        f"will force-close the naked short."
                    )
                    continue
                ts.state = StrategyState.Opened
                ts.position_opened_ts = self.current_timestamp
                fi = self.get_funding_info(ts.perp_trading_pair)
                if fi is not None:
                    ts.entry_funding_rate = fi.rate * Decimal("100")
                self.logger().info(
                    f"[{token}] Resumed existing SHORT position: {amount:.6f} "
                    f"@ {pos.entry_price}. Spot {token}: {spot_bal:.6f}. "
                    f"Funding rate: {ts.entry_funding_rate:.4f}%."
                )
            else:
                # Existing LONG — unexpected for this strategy
                self.logger().warning(
                    f"[{token}] Found existing LONG position ({amount:.6f}). "
                    f"This strategy only opens SHORTs. Please manually close this position."
                )

            self.logger().info(
                f"[{token}] Spot balance ({token}): "
                f"{self._spot_market.get_available_balance(token):.6f} available."
            )

    # ------------------------------------------------------------------
    # Tick / Main loop
    # ------------------------------------------------------------------

    def tick(self, timestamp: float):
        if not self._all_markets_ready or not self._position_mode_ready or not self._trading_started:
            self._all_markets_ready = self.all_markets_ready()
            if not self._all_markets_ready:
                return
            self.logger().info("Markets are ready.")

            if not self._position_mode_ready:
                self._position_mode_not_ready_counter += 1
                if self._position_mode_not_ready_counter == 10:
                    self._perp_market.set_position_mode(PositionMode.ONEWAY)
                    self._position_mode_not_ready_counter = 0
                return
            self._position_mode_not_ready_counter = 0

            self.logger().info("Trading started.")
            self._trading_started = True

            if not self.check_budget_available():
                self.logger().info("Trading not possible.")
                return

            # Resume: scan existing exchange positions and restore token states
            self._resume_existing_positions()

            self._ready_to_start = True

        if self._ready_to_start:
            # Wait 5 ticks (5s) for WS to push the first predicted funding rate
            # before allowing main() to run. This prevents using the stale
            # historical rate from the REST init.
            self._post_ready_warmup_ticks += 1
            if self._post_ready_warmup_ticks < 5:
                return

        if self._ready_to_start and (self._main_task is None or self._main_task.done()):
            self._main_task = safe_ensure_future(self.main(timestamp))

    async def main(self, timestamp):
        self.update_all_strategy_states()

        # Monitoring ALWAYS runs for opened positions (TP/SL checks)
        for token in self._tokens:
            ts = self._ts_for(token)
            if ts.state != StrategyState.Opened:
                continue
            funding_info = self.get_funding_info(ts.perp_trading_pair)
            if funding_info is None:
                continue
            self._monitor_open_position(token, ts, funding_info)

        # Health check: detect and fix one-sided exposures
        if self.current_timestamp - self._last_health_check_ts >= self._health_check_interval_seconds:
            self._last_health_check_ts = self.current_timestamp
            self._health_check()

        # Entry checks are throttled to check_interval_seconds
        if self.current_timestamp - self._last_check_ts < self._check_interval_seconds:
            return
        self._last_check_ts = self.current_timestamp

        # ── Phase 1: collect eligible tokens (pass all entry guards) ──
        eligible = []
        for token in self._tokens:
            ts = self._ts_for(token)
            if ts.state in (StrategyState.Opening, StrategyState.Closing):
                continue
            if ts.execution_expected_count > 0:
                continue
            if ts.state == StrategyState.Closed:
                if ts.next_arbitrage_opening_ts > self.current_timestamp:
                    continue
                if self.current_timestamp - ts.last_closed_ts < self._reopen_cooldown_seconds:
                    continue

            funding_info = self.get_funding_info(ts.perp_trading_pair)
            if funding_info is None:
                continue
            current_funding_rate_pct = funding_info.rate * Decimal("100")

            if current_funding_rate_pct <= s_decimal_zero:
                continue
            if current_funding_rate_pct < self._min_funding_rate_pct:
                continue

            # Phase-1 spread check using index/mark — avoids per-token API call
            mark_price = funding_info.mark_price
            idx_price = funding_info.index_price
            if mark_price is None or idx_price is None or mark_price == s_decimal_zero:
                continue
            spread_pct = (idx_price - mark_price) / mark_price * Decimal("100")
            if spread_pct > self._max_entry_spread_pct:
                self._log_throttled(token,
                    f"Entry rejected: idx({idx_price:.2f}) - mark({mark_price:.2f}) "
                    f"spread ({spread_pct:.4f}%) exceeds max_entry_spread_pct "
                    f"({self._max_entry_spread_pct:.4f}%).")
                continue
            if current_funding_rate_pct > s_decimal_zero:
                ratio = spread_pct / current_funding_rate_pct
                if ratio > self._max_spread_to_funding_ratio:
                    self._log_throttled(token,
                        f"Entry rejected: spread ({spread_pct:.4f}%) / funding "
                        f"({current_funding_rate_pct:.4f}%) ratio ({ratio:.1f}) exceeds "
                        f"max_spread_to_funding_ratio ({self._max_spread_to_funding_ratio:.1f}).")
                    continue

            eligible.append({
                "token": token,
                "ts": ts,
                "funding_info": funding_info,
                "funding_rate_pct": current_funding_rate_pct,
                "is_addition": ts.state == StrategyState.Opened,
            })

        if not eligible:
            self.logger().info(
                f"Entry scan: checked {len(self._tokens)} token(s), 0 eligible. "
                f"(Rate < {self._min_funding_rate_pct:.4f}% or spread too wide)"
            )

        # ── Phase 2: allocate & execute among eligible pool ──
        pool_rate = s_decimal_zero
        for e in eligible:
            pool_rate += e["funding_rate_pct"]
        for token in self._tokens:
            ts = self._ts_for(token)
            if ts.state in (StrategyState.Opened, StrategyState.Opening):
                pool_rate += ts.entry_funding_rate

        if pool_rate == s_decimal_zero:
            return

        remaining = self._total_order_amount_quote - self._locked_capital()
        if remaining <= s_decimal_zero:
            return

        for e in eligible:
            token = e["token"]
            ts = e["ts"]
            funding_info = e["funding_info"]
            funding_rate_pct = e["funding_rate_pct"]
            is_addition = e["is_addition"]

            alloc_quote = remaining * (funding_rate_pct / pool_rate)
            if alloc_quote == s_decimal_zero:
                continue

            order_amount = self._quote_to_base(token, alloc_quote, funding_info.index_price)
            if order_amount == s_decimal_zero:
                self.logger().info(
                    f"[{token}] Skipping: alloc {alloc_quote:.2f} quote quantized to 0 "
                    f"(price={funding_info.index_price:.2f}, below min order size)."
                )
                continue

            if not self._can_afford_on_both_sides(token, order_amount):
                self._log_throttled(token,
                    f"Skipping {token}: insufficient balance on one or both sides "
                    f"for {order_amount:.6f}. Will retry when balance is available.")
                continue

            price_perp, price_spot = await self._get_order_prices(
                ts.spot_trading_pair, ts.perp_trading_pair,
                perp_is_buy=False, spot_is_buy=True, order_amount=order_amount,
            )
            if price_perp is None or price_spot is None:
                self._log_throttled(token,
                    f"Failed to get order prices for {order_amount:.6f} {token}. "
                    f"Will retry next check cycle.")
                continue

            label = "Adding to" if is_addition else "Opening"
            self.logger().info(
                f"[{token}] {label} position: rate {funding_rate_pct:.4f}%, "
                f"alloc {alloc_quote:.2f} quote ({order_amount:.6f} {token})."
            )

            proposal = (True, False, price_spot, price_perp, order_amount)
            proposal = self.apply_slippage_buffers(proposal, ts)
            if not self.check_budget_constraint(proposal, ts, order_amount):
                self._log_throttled(token,
                    f"Budget constraint failed for {order_amount:.6f} {token}. "
                    f"Check spot/perpetual balances.")
                continue
            self._execute_arb_parallel(
                ts, proposal,
                purpose=ExecPurpose.ADD if is_addition else ExecPurpose.OPEN,
            )

    def _log_throttled(self, token: str, msg: str, interval: float = 15):
        last = self._last_log_ts.get(token, 0)
        if self.current_timestamp - last >= interval:
            self.logger().info(f"[{token}] {msg}")
            self._last_log_ts[token] = self.current_timestamp

    # ------------------------------------------------------------------
    # Order price helpers
    # ------------------------------------------------------------------

    async def _get_order_prices(
        self, spot_pair: str, perp_pair: str,
        perp_is_buy: bool, spot_is_buy: bool, order_amount: Decimal,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        try:
            price_spot = await self._spot_market.get_order_price(
                spot_pair, spot_is_buy, order_amount
            )
            price_perp = await self._perp_market.get_order_price(
                perp_pair, perp_is_buy, order_amount
            )
            return price_perp, price_spot
        except Exception as e:
            self.logger().warning(f"Failed to get order prices: {e}")
            return None, None

    # ------------------------------------------------------------------
    # Monitoring & close
    # ------------------------------------------------------------------

    def _monitor_open_position(self, token: str, ts: TokenState, funding_info: FundingInfo):
        current_funding_rate_pct = funding_info.rate * Decimal("100")
        perp_position = self._perp_position_for_token(token)
        if perp_position is None:
            self.logger().warning(f"[{token}] No perpetual position found while in Opened state. Resetting.")
            ts.state = StrategyState.Closed
            return

        position_amount = abs(perp_position.amount)
        position_value = position_amount * funding_info.mark_price
        unrealized = perp_position.unrealized_pnl
        total_pnl = unrealized + ts.accumulated_funding
        total_pnl_pct = (total_pnl / position_value * Decimal("100")) if position_value != s_decimal_zero else s_decimal_zero

        should_close = False
        is_stop_loss = False

        holding_seconds = self.current_timestamp - ts.position_opened_ts
        holding_elapsed = holding_seconds >= self._min_holding_seconds

        if current_funding_rate_pct < self._exit_funding_rate_pct:
            if not holding_elapsed:
                remaining = self._min_holding_seconds - holding_seconds
                self._log_throttled(token,
                    f"Funding rate ({current_funding_rate_pct:.4f}%) below exit, "
                    f"but holding not met ({remaining:.0f}s remaining).")
                return
            should_close = True
            self._log_throttled(token,
                f"Exit: funding rate ({current_funding_rate_pct:.4f}%) < exit "
                f"({self._exit_funding_rate_pct:.4f}%) after {holding_seconds:.0f}s.")

        elif total_pnl_pct >= self._take_profit_pct:
            should_close = True
            self._log_throttled(token,
                f"Take profit: total PnL ({total_pnl_pct:.4f}%) >= "
                f"take_profit ({self._take_profit_pct:.4f}%).")

        elif total_pnl_pct <= self._stop_loss_pct:
            should_close = True
            is_stop_loss = True
            self._log_throttled(token,
                f"Stop loss: total PnL ({total_pnl_pct:.4f}%) <= "
                f"stop_loss ({self._stop_loss_pct:.4f}%).")

        if should_close:
            self.logger().info(
                f"[{token}] PnL — Unrealized: {unrealized:.4f}, "
                f"Funding: {ts.accumulated_funding:.4f}, "
                f"Total: {total_pnl:.4f} ({total_pnl_pct:.4f}%)"
            )
            safe_ensure_future(self._close_position(token, ts, is_stop_loss))

    async def _close_position(self, token: str, ts: TokenState, is_stop_loss: bool = False):
        if ts.state != StrategyState.Opened:
            return
        perp_position = self._perp_position_for_token(token)
        if perp_position is None:
            return

        order_amount = abs(perp_position.amount)
        open_perp_is_short = perp_position.amount < 0
        close_perp_is_buy = open_perp_is_short
        close_spot_is_buy = not open_perp_is_short

        price_perp, price_spot = await self._get_order_prices(
            ts.spot_trading_pair, ts.perp_trading_pair,
            perp_is_buy=close_perp_is_buy, spot_is_buy=close_spot_is_buy,
            order_amount=order_amount,
        )
        if price_perp is None or price_spot is None:
            self.logger().warning(f"[{token}] Failed to get close prices. Retry next tick.")
            return

        # Exit spread guard (skip for stop loss)
        if price_perp != s_decimal_zero and not is_stop_loss:
            exit_spread_pct = (price_perp - price_spot) / price_perp * Decimal("100")
            if exit_spread_pct > self._max_exit_spread_pct:
                self._log_throttled(token,
                    f"Exit postponed: perp_buy({price_perp:.2f}) - spot_sell({price_spot:.2f}) "
                    f"exit spread ({exit_spread_pct:.4f}%) > max ({self._max_exit_spread_pct:.4f}%).")
                return

        self.logger().info(f"[{token}] Closing position ({order_amount:.6f} {token})...")

        proposal = (close_spot_is_buy, close_perp_is_buy, price_spot, price_perp, order_amount)
        proposal = self.apply_slippage_buffers(proposal, ts)
        if self.check_budget_constraint(proposal, ts, order_amount):
            self._execute_arb_parallel(ts, proposal, purpose=ExecPurpose.CLOSE)

    # ------------------------------------------------------------------
    # State machine (per token)
    # ------------------------------------------------------------------

    def update_all_strategy_states(self):
        """State transitions are handled inside _check_alignment_and_finalize."""
        pass

    # ------------------------------------------------------------------
    # Health check — detect & fix one-sided exposures
    # ------------------------------------------------------------------

    _HEALTH_GRACE_SECONDS = 60  # ignore transient one-sidedness during opening/closing

    def _health_check(self):
        """Position-based safety scan. Ignores strategy state — only looks at
        actual exchange positions. One-sided exposures must persist for at least
        _HEALTH_GRACE_SECONDS before being force-closed, to avoid interfering
        with normal opening/closing alignment.
        """
        now = self.current_timestamp
        for token in self._tokens:
            ts = self._ts_for(token)
            pos = self._perp_position_for_token(token)
            has_perp = pos is not None and pos.amount != s_decimal_zero
            perp_amount = abs(pos.amount) if has_perp else s_decimal_zero
            spot_bal = self._spot_market.get_available_balance(token)
            has_spot = spot_bal > s_decimal_zero

            # Balanced: both sides match within min_order_size tolerance
            diff = spot_bal - perp_amount
            spot_min = s_decimal_zero
            try:
                rule = self._spot_market.trading_rules.get(ts.spot_trading_pair)
                if rule is not None:
                    spot_min = rule.min_order_size
            except Exception:
                pass
            if spot_min == s_decimal_zero:
                spot_min = self._spot_market.quantize_order_amount(ts.spot_trading_pair, Decimal("0.00000001"))
                if spot_min == s_decimal_zero:
                    spot_min = Decimal("0.00000001")

            if abs(diff) < spot_min:
                self._first_seen_one_sided.pop(token, None)
                continue

            # One-sided or mismatched: track when first detected
            key = token
            if key not in self._first_seen_one_sided:
                self._first_seen_one_sided[key] = now
                side = "PERP-ONLY" if has_perp and not has_spot else \
                       "SPOT-ONLY" if has_spot and not has_perp else \
                       "MISMATCH"
                self.logger().info(
                    f"[{token}] HEALTH CHECK: unbalanced ({side}), "
                    f"spot={spot_bal:.6f}, perp={perp_amount:.6f}, diff={diff:.6f}. "
                    f"Will force-close if not resolved in {self._HEALTH_GRACE_SECONDS}s."
                )
                continue

            elapsed = now - self._first_seen_one_sided[key]
            if elapsed < self._HEALTH_GRACE_SECONDS:
                continue

            # Grace expired — force-close
            if has_perp and not has_spot:
                self.logger().warning(
                    f"[{token}] HEALTH CHECK: perp position ({pos.amount:.6f}) with no spot "
                    f"for {elapsed:.0f}s — force-closing perp."
                )
                safe_ensure_future(self._force_close_perp_exposure(token, ts, pos))
            elif has_spot and not has_perp:
                self.logger().warning(
                    f"[{token}] HEALTH CHECK: spot balance ({spot_bal:.6f}) with no perp "
                    f"for {elapsed:.0f}s — force-selling spot."
                )
                safe_ensure_future(self._force_close_spot_exposure(token, ts, spot_bal))
            elif has_spot and has_perp and abs(diff) >= spot_min:
                # Both sides exist but amounts don't match
                if diff > 0:
                    # Spot > Perp: sell excess spot
                    excess = diff
                    self.logger().warning(
                        f"[{token}] HEALTH CHECK: spot > perp by {excess:.6f} "
                        f"for {elapsed:.0f}s — selling excess spot."
                    )
                    safe_ensure_future(self._force_close_spot_exposure(token, ts, excess))
                else:
                    # Perp > Spot: close excess perp
                    excess = abs(diff)
                    self.logger().warning(
                        f"[{token}] HEALTH CHECK: perp > spot by {excess:.6f} "
                        f"for {elapsed:.0f}s — closing excess perp."
                    )
                    # Partially close perp (sell excess amount)
                    side_text = "BUY" if pos.amount < 0 else "SELL"
                    pa = PositionAction.CLOSE
                    fn = self.buy_with_specific_market if pos.amount < 0 else self.sell_with_specific_market
                    self.logger().warning(
                        f"[{token}] Submitting emergency perp partial close: {side_text} {excess:.6f}."
                    )
                    fn(ts.perp_tuple, excess, self._perp_market.get_taker_order_type(),
                       position_action=pa)

            del self._first_seen_one_sided[key]

    async def _force_close_perp_exposure(self, token: str, ts: TokenState, pos: Position):
        """Submit a market order to close an orphaned perp position."""
        is_short = pos.amount < 0
        close_is_buy = is_short
        amount = abs(pos.amount)

        self.logger().warning(
            f"[{token}] Submitting emergency perp close: "
            f"{'BUY' if close_is_buy else 'SELL'} {amount:.6f} to close orphan position."
        )
        fn = self.buy_with_specific_market if close_is_buy else self.sell_with_specific_market
        fn(
            ts.perp_tuple,
            amount,
            self._perp_market.get_taker_order_type(),
            position_action=PositionAction.CLOSE,
        )
        ts.state = StrategyState.Closed
        ts.accumulated_funding = Decimal(0)

    async def _force_close_spot_exposure(self, token: str, ts: TokenState, amount: Decimal):
        """Sell spot to close an orphaned long position."""
        self.logger().warning(
            f"[{token}] Submitting emergency spot SELL: {amount:.6f} to close orphan spot."
        )
        self.sell_with_specific_market(
            ts.spot_tuple,
            amount,
            self._spot_market.get_taker_order_type(),
        )
        ts.state = StrategyState.Closed
        ts.accumulated_funding = Decimal(0)

    # ------------------------------------------------------------------
    # Execution: parallel orders + alignment + rollback
    # ------------------------------------------------------------------

    def _execute_arb_parallel(self, ts: TokenState, proposal: Tuple, purpose: str):
        spot_is_buy, perp_is_buy, spot_price, perp_price, order_amount = proposal
        if order_amount == s_decimal_zero:
            return

        ts.execution_tracker.clear()
        ts.execution_purpose = purpose
        ts.execution_expected_count = 2
        ts.execution_started_ts = self.current_timestamp

        side_s = "BUY" if spot_is_buy else "SELL"
        self.logger().info(
            f"[{ts.token}] {side_s} {order_amount:.6f} spot @ {spot_price:.2f}"
        )
        spot_fn = self.buy_with_specific_market if spot_is_buy else self.sell_with_specific_market
        spot_fn(
            ts.spot_tuple,
            order_amount,
            self._spot_market.get_taker_order_type(),
            spot_price,
        )

        side_p = "BUY" if perp_is_buy else "SELL"
        pa = PositionAction.CLOSE if purpose == ExecPurpose.CLOSE else PositionAction.OPEN
        self.logger().info(
            f"[{ts.token}] {side_p} {order_amount:.6f} perp @ {perp_price:.2f} ({pa.name})"
        )
        perp_fn = self.buy_with_specific_market if perp_is_buy else self.sell_with_specific_market
        perp_fn(
            ts.perp_tuple,
            order_amount,
            self._perp_market.get_taker_order_type(),
            perp_price,
            position_action=pa,
        )

        if purpose == ExecPurpose.OPEN:
            ts.state = StrategyState.Opening
        elif purpose == ExecPurpose.CLOSE:
            ts.state = StrategyState.Closing
        # For ADD, keep current state (Opened)

        self.logger().info(
            f"[{ts.token}] 2 orders placed (purpose={purpose}). "
            f"Awaiting fills for {order_amount:.6f} {ts.token}."
        )

    def _record_order_created(self, ts: TokenState, order_id: str, side: str, direction: TradeType):
        if ts.execution_expected_count == 0:
            return
        ts.execution_tracker[order_id] = {
            "side": side,
            "direction": direction,
            "filled_amount": s_decimal_zero,
            "completed": False,
        }

    def _record_order_fill(self, ts: TokenState, order_id: str, filled_amount: Decimal):
        if order_id not in ts.execution_tracker:
            ts.execution_tracker[order_id] = {
                "side": "unknown",
                "direction": None,
                "filled_amount": s_decimal_zero,
                "completed": False,
            }
        ts.execution_tracker[order_id]["filled_amount"] = filled_amount
        ts.execution_tracker[order_id]["completed"] = True

        filled_count = sum(1 for v in ts.execution_tracker.values() if v["completed"])
        if filled_count >= ts.execution_expected_count:
            self._check_alignment_and_finalize(ts)

    def _handle_order_failure(self, ts: TokenState, order_id: str):
        if order_id not in ts.execution_tracker:
            return
        failed_side = ts.execution_tracker[order_id]["side"]
        purpose = ts.execution_purpose
        self.logger().error(f"[{ts.token}] Order {order_id} ({failed_side}) FAILED (purpose={purpose}). Rolling back.")

        for oid, info in ts.execution_tracker.items():
            if oid != order_id and info["completed"] and info["filled_amount"] > s_decimal_zero:
                other_side = info["side"]
                other_amount = info["filled_amount"]
                other_direction = info["direction"]
                self.logger().warning(f"[{ts.token}] Rolling back {other_side} — reverse order for {other_amount}.")
                self._submit_rollback_order(ts, other_side, other_direction, other_amount)
                break

        ts.execution_tracker.clear()
        ts.execution_expected_count = 0
        if purpose == ExecPurpose.OPEN:
            ts.state = StrategyState.Closed
            self.logger().info(f"[{ts.token}] Entry rollback. State → Closed.")
        elif purpose == ExecPurpose.CLOSE:
            ts.state = StrategyState.Opened
            self.logger().warning(f"[{ts.token}] Close rollback. State → Opened, will retry.")
        elif purpose == ExecPurpose.ADD:
            # Addition failed — just reset, stay Opened
            self.logger().info(f"[{ts.token}] Addition rollback. State stays Opened.")

    def _check_alignment_and_finalize(self, ts: TokenState):
        spot_filled = s_decimal_zero
        perp_filled = s_decimal_zero
        spot_direction = None
        perp_direction = None

        for oid, info in ts.execution_tracker.items():
            if info["side"] == "spot":
                spot_filled += info["filled_amount"]
                spot_direction = info["direction"]
            elif info["side"] == "perp":
                perp_filled += info["filled_amount"]
                perp_direction = info["direction"]
            else:
                self.logger().warning(f"[{ts.token}] Unknown side '{info['side']}' for order {oid}. Skipping alignment.")
                ts.execution_tracker.clear()
                ts.execution_expected_count = 0
                return

        diff = spot_filled - perp_filled
        self.logger().info(f"[{ts.token}] Fill check — Spot: {spot_filled:.8f}, Perp: {perp_filled:.8f}, Diff: {diff:.8f}")

        # If diff is smaller than spot min order size, accept as aligned —
        # a catch-up order would be rejected by the exchange anyway.
        spot_min = self._spot_market.quantize_order_amount(ts.spot_trading_pair, s_decimal_zero)
        if spot_min == s_decimal_zero:
            try:
                rule = self._spot_market.trading_rules.get(ts.spot_trading_pair)
                if rule is not None:
                    spot_min = rule.min_order_size
            except Exception:
                pass
        if spot_min == s_decimal_zero:
            spot_min = Decimal("0.00000001")

        abs_diff = abs(diff)
        if abs_diff == s_decimal_zero:
            self.logger().info(f"[{ts.token}] Both sides exactly aligned.")
        elif abs_diff < spot_min:
            self.logger().info(
                f"[{ts.token}] Diff ({abs_diff:.8f}) < min order size ({spot_min:.8f}) "
                f"— treating as aligned."
            )
        else:
            if diff > 0:
                self.logger().info(f"[{ts.token}] Perp short by {abs_diff:.8f}. Catching up.")
                self._submit_catch_up_perpetual(ts, perp_direction, abs_diff)
            else:
                self.logger().info(f"[{ts.token}] Spot short by {abs_diff:.8f}. Catching up.")
                self._submit_catch_up_spot(ts, spot_direction, abs_diff)

        self._finalize_state(ts)

    def _finalize_state(self, ts: TokenState):
        purpose = ts.execution_purpose
        if purpose == ExecPurpose.OPEN:
            ts.state = StrategyState.Opened
            ts.accumulated_funding = Decimal(0)
            ts.position_opened_ts = self.current_timestamp
            fi = self.get_funding_info(ts.perp_trading_pair)
            if fi is not None:
                ts.entry_funding_rate = fi.rate * Decimal("100")
            self.logger().info(f"[{ts.token}] Position OPENED (entry rate: {ts.entry_funding_rate:.4f}%).")
        elif purpose == ExecPurpose.ADD:
            self.logger().info(f"[{ts.token}] Position ADDITION confirmed. Still Opened.")
        elif purpose == ExecPurpose.CLOSE:
            ts.state = StrategyState.Closed
            ts.next_arbitrage_opening_ts = self.current_timestamp + self._next_arbitrage_opening_delay
            ts.last_closed_ts = self.current_timestamp
            self.logger().info(
                f"[{ts.token}] Position CLOSED. Funding: {ts.accumulated_funding:.4f}. "
                f"Next open after {self._next_arbitrage_opening_delay:.0f}s."
            )
            ts.accumulated_funding = Decimal(0)
            ts.entry_funding_rate = Decimal(0)

        ts.execution_tracker.clear()
        ts.execution_expected_count = 0

    def _submit_catch_up_spot(self, ts: TokenState, direction: TradeType, amount: Decimal):
        is_buy = direction == TradeType.BUY
        fn = self.buy_with_specific_market if is_buy else self.sell_with_specific_market
        fn(ts.spot_tuple, amount, self._spot_market.get_taker_order_type())

    def _submit_catch_up_perpetual(self, ts: TokenState, direction: TradeType, amount: Decimal):
        is_buy = direction == TradeType.BUY
        pa = PositionAction.CLOSE if ts.execution_purpose == ExecPurpose.CLOSE else PositionAction.OPEN
        fn = self.buy_with_specific_market if is_buy else self.sell_with_specific_market
        fn(ts.perp_tuple, amount, self._perp_market.get_taker_order_type(), position_action=pa)

    def _submit_rollback_order(self, ts: TokenState, side: str, original_direction: TradeType, amount: Decimal):
        reverse_buy = original_direction != TradeType.BUY
        if side == "spot":
            fn = self.buy_with_specific_market if reverse_buy else self.sell_with_specific_market
            fn(ts.spot_tuple, amount, self._spot_market.get_taker_order_type())
        else:
            fn = self.buy_with_specific_market if reverse_buy else self.sell_with_specific_market
            fn(ts.perp_tuple, amount, self._perp_market.get_taker_order_type(),
               position_action=PositionAction.CLOSE)
        self.logger().warning(f"[{ts.token}] Rollback: {side} {'BUY' if reverse_buy else 'SELL'} {amount}.")

    # ------------------------------------------------------------------
    # Slippage / budget
    # ------------------------------------------------------------------

    def apply_slippage_buffers(self, proposal: Tuple, ts: TokenState):
        spot_is_buy, perp_is_buy, spot_price, perp_price, order_amount = proposal
        spot_buf = self._spot_market_slippage_buffer if spot_is_buy else -self._spot_market_slippage_buffer
        perp_buf = self._perp_market_slippage_buffer if perp_is_buy else -self._perp_market_slippage_buffer
        new_spot = spot_price * (Decimal("1") + spot_buf)
        new_perp = perp_price * (Decimal("1") + perp_buf)
        new_spot = self._spot_market.quantize_order_price(ts.spot_trading_pair, new_spot)
        new_perp = self._perp_market.quantize_order_price(ts.perp_trading_pair, new_perp)
        return (spot_is_buy, perp_is_buy, new_spot, new_perp, order_amount)

    def check_budget_available(self) -> bool:
        spot_quote = self._spot_market_info.quote_asset
        perp_quote = self._perp_market_info.quote_asset
        spot_bal = self._spot_market.get_available_balance(spot_quote)
        perp_bal = self._perp_market.get_available_balance(perp_quote)

        # Unified margin: perp can be 0 as long as spot has enough
        if spot_bal == s_decimal_zero:
            self.logger().info(f"Cannot trade: spot {spot_quote} balance is 0.")
            return False
        if perp_bal == s_decimal_zero:
            self.logger().info(
                f"Perp {perp_quote} balance is 0 — assuming unified margin mode "
                f"(all margin drawn from spot)."
            )
        return True

    def check_budget_constraint(self, proposal: Tuple, ts: TokenState, order_amount: Decimal) -> bool:
        spot_is_buy, perp_is_buy, spot_price, perp_price = proposal[:4]
        return self._check_spot_budget(ts, spot_is_buy, spot_price, order_amount) and \
            self._check_perp_budget(ts, perp_is_buy, perp_price, order_amount)

    def _check_spot_budget(self, ts: TokenState, is_buy: bool, price: Decimal, amount: Decimal) -> bool:
        bc = self._spot_market.budget_checker
        candidate = OrderCandidate(
            trading_pair=ts.spot_trading_pair,
            is_maker=False,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY if is_buy else TradeType.SELL,
            amount=amount,
            price=price,
        )
        adj = bc.adjust_candidate(candidate, all_or_none=True)
        if adj.amount < amount:
            self.logger().info(f"[{ts.token}] Spot balance insufficient.")
            return False
        return True

    def _check_perp_budget(self, ts: TokenState, is_buy: bool, price: Decimal, amount: Decimal) -> bool:
        perp_quote = ts.perp_trading_pair.split("-")[1]
        perp_bal = self._perp_market.get_available_balance(perp_quote)
        spot_quote = ts.spot_trading_pair.split("-")[1]
        spot_bal = self._spot_market.get_available_balance(spot_quote)

        # Unified margin: both connectors report same balance from shared pool.
        # The spot budget check + _can_afford_on_both_sides already validated.
        if spot_bal > s_decimal_zero and spot_bal == perp_bal:
            return True

        bc = self._perp_market.budget_checker
        pos = self._perp_position_for_token(ts.token)
        position_close = False
        if pos and abs(pos.amount) == amount:
            cur_short = pos.amount < 0
            cur_buy = not cur_short
            if is_buy != cur_buy:
                position_close = True

        candidate = PerpetualOrderCandidate(
            trading_pair=ts.perp_trading_pair,
            is_maker=False,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY if is_buy else TradeType.SELL,
            amount=amount,
            price=price,
            leverage=Decimal(self._perp_leverage),
            position_close=position_close,
        )
        adj = bc.adjust_candidate(candidate, all_or_none=True)
        if adj.amount < amount:
            self.logger().info(f"[{ts.token}] Perp balance insufficient.")
            return False
        return True

    # ------------------------------------------------------------------
    # Status display
    # ------------------------------------------------------------------

    def active_positions_df(self) -> pd.DataFrame:
        columns = ["Token", "Type", "Entry Price", "Amount", "Leverage", "Unrealized PnL"]
        data = []
        for pos in self.perp_positions:
            token = pos.trading_pair.split("-")[0]
            data.append([
                token,
                "LONG" if pos.amount > 0 else "SHORT",
                pos.entry_price,
                pos.amount,
                pos.leverage,
                pos.unrealized_pnl,
            ])
        return pd.DataFrame(data=data, columns=columns)

    async def format_status(self) -> str:
        lines = []
        lines.extend(["", f"  Spot: {self._spot_market.display_name} | "
                     f"Perp: {self._perp_market.display_name} | "
                     f"Tokens: {', '.join(self._tokens)}"])

        # Positions
        positions = self.perp_positions
        if positions:
            df = self.active_positions_df()
            lines.extend(["", "  Positions:"] + ["    " + l for l in df.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active positions."])

        # Per-token status
        for token in self._tokens:
            ts = self._ts_for(token)
            fi = self.get_funding_info(ts.perp_trading_pair)
            fr = f"{fi.rate * 100:.4f}%" if fi else "N/A"
            mp = f"{fi.mark_price:.2f}" if fi else "N/A"

            alloc = self._calc_dynamic_allocation(token, fi.rate * Decimal("100")) if fi else s_decimal_zero
            lines.extend(["", f"  ── {token} ──",
                          f"    State: {ts.state.name} | Funding Rate: {fr} | Mark: {mp} | "
                          f"Alloc: {alloc:.2f} / {self._total_order_amount_quote:.2f}"])

            if ts.state == StrategyState.Opened:
                pos = self._perp_position_for_token(token)
                if pos:
                    unreal = pos.unrealized_pnl
                    total = unreal + ts.accumulated_funding
                    pv = abs(pos.amount) * (fi.mark_price if fi else s_decimal_zero)
                    pct = total / pv * 100 if pv != s_decimal_zero else s_decimal_zero
                    held = self.current_timestamp - ts.position_opened_ts
                    lines.append(f"    Funding: {ts.accumulated_funding:.4f} | Unreal: {unreal:.4f} | "
                                 f"Total PnL: {total:.4f} ({pct:.4f}%) | Held: {held:.0f}s")
            elif ts.state == StrategyState.Closed and ts.last_closed_ts > 0:
                since = self.current_timestamp - ts.last_closed_ts
                if since < self._reopen_cooldown_seconds:
                    lines.append(f"    Cooldown: {(self._reopen_cooldown_seconds - since):.0f}s remaining")

        # Strategy params
        lines.extend([
            "",
            f"  Params: Min Funding {self._min_funding_rate_pct:.4f}% | Exit Funding {self._exit_funding_rate_pct:.4f}%",
            f"          Take Profit {self._take_profit_pct:.2f}% | Stop Loss {self._stop_loss_pct:.2f}%",
            f"          Total Capital {self._total_order_amount_quote} {self._spot_market_info.quote_asset} | Leverage {self._perp_leverage}x",
            f"          Max Entry Spread {self._max_entry_spread_pct:.4f}% | Max Exit Spread {self._max_exit_spread_pct:.4f}%",
            f"          Spread/Funding Ratio {self._max_spread_to_funding_ratio:.1f}",
            f"          Check Interval {self._check_interval_seconds:.0f}s | Min Holding {self._min_holding_seconds / 3600:.1f}h | Reopen Cooldown {self._reopen_cooldown_seconds / 3600:.1f}h",
        ])

        assets_df = self.wallet_balance_data_frame([self._spot_market_info, self._perp_market_info])
        lines.extend(["", "  Assets:"] + ["    " + l for l in str(assets_df).split("\n")])

        wl = self.network_warning([self._spot_market_info]) + self.network_warning([self._perp_market_info])
        wl += self.balance_warning([self._spot_market_info]) + self.balance_warning([self._perp_market_info])
        if wl:
            lines.extend(["", "*** WARNINGS ***"] + wl)

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def tracked_market_orders(self) -> List[Tuple[ConnectorBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    @property
    def tracked_limit_orders(self) -> List[Tuple[ConnectorBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    def start(self, clock: Clock, timestamp: float):
        self._ready_to_start = False
        self.apply_initial_settings()

    def stop(self, clock: Clock):
        if self._main_task is not None:
            self._main_task.cancel()
            self._main_task = None
        self._ready_to_start = False
        self._post_ready_warmup_ticks = 0

    # ------------------------------------------------------------------
    # Event callbacks
    # ------------------------------------------------------------------

    def _find_token_by_pair(self, trading_pair: str) -> Optional[str]:
        for token in self._tokens:
            ts = self._ts_for(token)
            if trading_pair in (ts.spot_trading_pair, ts.perp_trading_pair):
                return token
        return None

    def _find_token_state_by_order(self, order_id: str) -> Optional[TokenState]:
        for token in self._tokens:
            ts = self._ts_for(token)
            if order_id in ts.execution_tracker:
                return ts
        return None

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        token = self._find_token_by_pair(event.trading_pair)
        if token is None:
            return
        ts = self._ts_for(token)
        side = "spot" if event.trading_pair == ts.spot_trading_pair else "perp"
        self._record_order_created(ts, event.order_id, side, TradeType.BUY)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        token = self._find_token_by_pair(event.trading_pair)
        if token is None:
            return
        ts = self._ts_for(token)
        side = "spot" if event.trading_pair == ts.spot_trading_pair else "perp"
        self._record_order_created(ts, event.order_id, side, TradeType.SELL)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        for token in self._tokens:
            ts = self._ts_for(token)
            if event.order_id in ts.execution_tracker:
                self._record_order_fill(ts, event.order_id, event.base_asset_amount)
                return

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        for token in self._tokens:
            ts = self._ts_for(token)
            if event.order_id in ts.execution_tracker:
                self._record_order_fill(ts, event.order_id, event.base_asset_amount)
                return

    def did_fail_order(self, event: MarketOrderFailureEvent):
        ts = self._find_token_state_by_order(event.order_id)
        if ts is not None:
            self._handle_order_failure(ts, event.order_id)

    def did_complete_funding_payment(self, event: FundingPaymentCompletedEvent):
        token = event.trading_pair.split("-")[0]
        if token not in self._token_states:
            return
        ts = self._ts_for(token)
        if ts.state != StrategyState.Opened:
            return
        ts.accumulated_funding += event.amount
        action = "received" if event.amount > 0 else "paid"
        self.logger().info(
            f"[{token}] Funding payment {action}: {abs(event.amount):.4f} "
            f"(total: {ts.accumulated_funding:.4f})"
        )

    def did_change_position_mode_succeed(self, event: PositionModeChangeEvent):
        if event.position_mode is PositionMode.ONEWAY:
            self.logger().info("Position mode ONEWAY succeeded.")
            self._position_mode_ready = True
        else:
            self.logger().warning("Position mode is not ONEWAY.")
            self._position_mode_ready = False

    def did_change_position_mode_fail(self, event: PositionModeChangeEvent):
        self.logger().error(f"Position mode change failed: {event.message}.")
        self._position_mode_ready = False
