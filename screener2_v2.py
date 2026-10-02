# screener2_v2.py
#
# SCREENER #2
# Strategy v2.0 / Score v2.0
#
# IMPORTANT:
# This file implements ONLY the approved Strategy #2.
# No Screener #1 logic.
# No Telegram.
# Email notifications only.
#
# Runtime: Python 3.11+
#
# Core principles:
# - closed candles only
# - no look-ahead
# - deterministic calculations
# - DataProvider abstraction
# - Stage A / Stage B separation
# - immutable strategy configuration
# - rejection reasons instead of REJECTED state
# - Binance is an infrastructure concern, not a strategy concern

from __future__ import annotations

import asyncio
import math
import statistics
import time

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, Optional, Protocol, Sequence


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass(frozen=True)
class StrategyConfig:
    TOP_SYMBOLS_LIMIT: int = 50

    SWING_N: int = 2
    ATR_PERIOD: int = 14
    RVOL_PERIOD: int = 20

    BOS_BUFFER_ATR: float = 0.05
    ENTRY_BUFFER_ATR: float = 0.05

    SWEEP_MIN_ATR: float = 0.10
    SWEEP_MAX_ATR: float = 0.40

    SL_BUFFER_ATR: float = 0.15

    MIN_RR_TP1: float = 1.2
    MIN_RR_TP2: float = 2.0

    MAX_SL_PERCENT: float = 3.5
    MAX_SL_ATR: float = 5.0

    RANGE_LOOKBACK: int = 20
    MAX_RANGE_WIDTH_ATR: float = 4.0

    MAX_SETUP_AGE: int = 4
    MAX_BREAKOUT_RETEST_AGE: int = 12

    MAX_CORRELATED_ACTIVE: int = 3
    CORRELATION_THRESHOLD: float = 0.75

    DUPLICATE_WINDOW_HOURS: int = 4
    COOLDOWN_AFTER_SL: int = 4

    EMAIL_MIN_SCORE: int = 75

    GRADE_A_PLUS: int = 85
    GRADE_A: int = 75
    GRADE_B: int = 65


CONFIG = StrategyConfig()


# ============================================================
# ENUMS
# ============================================================

class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class SetupType(str, Enum):
    RANGE_BREAKOUT = "RANGE_BREAKOUT"
    PULLBACK = "PULLBACK"
    BREAKOUT_RETEST = "BREAKOUT_RETEST"
    LIQUIDITY_SWEEP = "LIQUIDITY_SWEEP"


class MarketState(str, Enum):
    BULL = "BULL"
    BEAR = "BEAR"
    RANGE = "RANGE"
    HIGH_VOL = "HIGH_VOL"


class SignalState(str, Enum):
    DETECTED = "DETECTED"
    VALIDATED = "VALIDATED"
    ARMED = "ARMED"
    TRIGGERED = "TRIGGERED"
    ACTIVE = "ACTIVE"
    TP1 = "TP1"
    TP2 = "TP2"
    CLOSED = "CLOSED"

    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    CANCELLED = "CANCELLED"
    SL = "SL"


# ============================================================
# REJECTION CODES
# ============================================================

class RejectCode(str, Enum):
    INVALID_DATA = "REJECT_INVALID_DATA"
    INSUFFICIENT_HISTORY = "REJECT_INSUFFICIENT_HISTORY"
    LOW_LIQUIDITY = "REJECT_LOW_LIQUIDITY"
    INVALID_SETUP = "REJECT_INVALID_SETUP"
    HTF_CONFLICT = "REJECT_HTF_CONFLICT"
    NO_5M_TRIGGER = "REJECT_NO_5M_TRIGGER"
    INVALID_ENTRY = "REJECT_INVALID_ENTRY"
    INVALID_SL = "REJECT_INVALID_SL"
    SL_TOO_WIDE = "REJECT_SL_TOO_WIDE"
    NO_VALID_TP1 = "REJECT_NO_VALID_TP1"
    LOW_RR_TP1 = "REJECT_LOW_RR_TP1"
    NO_VALID_TP2 = "REJECT_NO_VALID_TP2"
    LOW_RR_TP2 = "REJECT_LOW_RR_TP2"
    DUPLICATE = "REJECT_DUPLICATE"
    COOLDOWN = "REJECT_COOLDOWN"
    CORRELATION = "REJECT_CORRELATION"


# ============================================================
# MARKET DATA
# ============================================================

@dataclass(frozen=True)
class Candle:
    open_time: int
    close_time: int

    open: float
    high: float
    low: float
    close: float

    volume: float
    quote_volume: float

    taker_buy_volume: Optional[float] = None
    taker_sell_volume: Optional[float] = None

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def closed(self) -> bool:
        return True


@dataclass(frozen=True)
class FundingPoint:
    timestamp: int
    rate: float


@dataclass(frozen=True)
class OISnapshot:
    oi_change_15m: float
    oi_change_1h: float
    funding_change_15m: float
    funding_change_1h: float


@dataclass(frozen=True)
class Swing:
    index: int
    price: float
    is_high: bool
    confirmed: bool


@dataclass(frozen=True)
class SRLevel:
    price: float
    kind: str
    strength: float
    source: str
    timeframe: str


# ============================================================
# DATA PROVIDER
# ============================================================

class DataProvider(Protocol):

    async def symbols(self) -> Sequence[str]:
        ...

    async def candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> Sequence[Candle]:
        ...

    async def funding(
        self,
        symbol: str,
    ) -> Optional[FundingPoint]:
        ...

    async def open_interest(
        self,
        symbol: str,
    ) -> Optional[OISnapshot]:
        ...

    async def quote_volume_24h(
        self,
        symbol: str,
    ) -> float:
        ...


# ============================================================
# CLOSED CANDLE POLICY
# ============================================================

def closed_candles(
    candles: Sequence[Candle],
    now_ms: Optional[int] = None,
) -> list[Candle]:

    if now_ms is None:
        now_ms = int(time.time() * 1000)

    return [
        c for c in candles
        if c.close_time <= now_ms
    ]


# ============================================================
# BASIC INDICATORS
# ============================================================

def sma(values: Sequence[float], period: int) -> Optional[float]:

    if len(values) < period:
        return None

    return sum(values[-period:]) / period


def ema(
    values: Sequence[float],
    period: int,
) -> Optional[float]:

    if len(values) < period:
        return None

    multiplier = 2.0 / (period + 1)

    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            value - result
        ) * multiplier + result

    return result


def true_range(
    current: Candle,
    previous: Candle,
) -> float:

    return max(
        current.high - current.low,
        abs(current.high - previous.close),
        abs(current.low - previous.close),
    )


def atr(
    candles: Sequence[Candle],
    period: int,
) -> Optional[float]:

    if len(candles) < period + 1:
        return None

    trs = [
        true_range(candles[i], candles[i - 1])
        for i in range(1, len(candles))
    ]

    return sma(trs, period)


# ============================================================
# RVOL
# ============================================================

def rvol15(
    candles: Sequence[Candle],
    period: int = CONFIG.RVOL_PERIOD,
) -> Optional[float]:

    if len(candles) < period + 1:
        return None

    current = candles[-1].volume

    baseline = sma(
        [c.volume for c in candles[:-1]],
        period,
    )

    if baseline is None or baseline <= 0:
        return None

    return current / baseline


def volume_score(rvol: float) -> int:

    if rvol >= 2.0:
        return 10

    if rvol >= 1.5:
        return 8

    if rvol >= 1.2:
        return 6

    if rvol >= 1.0:
        return 4

    return 2


# ============================================================
# SWINGS
# ============================================================

def confirmed_swings(
    candles: Sequence[Candle],
    n: int = CONFIG.SWING_N,
) -> list[Swing]:

    result: list[Swing] = []

    if len(candles) < 2 * n + 1:
        return result

    for i in range(n, len(candles) - n):

        current = candles[i]

        left = candles[i - n:i]
        right = candles[i + 1:i + n + 1]

        is_high = (
            current.high > max(c.high for c in left)
            and
            current.high >= max(c.high for c in right)
        )

        is_low = (
            current.low < min(c.low for c in left)
            and
            current.low <= min(c.low for c in right)
        )

        if is_high:
            result.append(
                Swing(
                    index=i,
                    price=current.high,
                    is_high=True,
                    confirmed=True,
                )
            )

        if is_low:
            result.append(
                Swing(
                    index=i,
                    price=current.low,
                    is_high=False,
                    confirmed=True,
                )
            )

    return result


# ============================================================
# STRUCTURE BOS
# ============================================================

def latest_confirmed_swing(
    swings: Sequence[Swing],
    is_high: bool,
) -> Optional[Swing]:

    candidates = [
        s for s in swings
        if s.confirmed and s.is_high == is_high
    ]

    return candidates[-1] if candidates else None


def structure_bos(
    candles: Sequence[Candle],
    side: Side,
) -> bool:

    if len(candles) < 10:
        return False

    current = candles[-1]

    current_atr = atr(candles, CONFIG.ATR_PERIOD)

    if current_atr is None:
        return False

    swings = confirmed_swings(candles)

    if side == Side.LONG:

        swing = latest_confirmed_swing(swings, True)

        if swing is None:
            return False

        return (
            current.close
            >
            swing.price + CONFIG.BOS_BUFFER_ATR * current_atr
        )

    swing = latest_confirmed_swing(swings, False)

    if swing is None:
        return False

    return (
        current.close
        <
        swing.price - CONFIG.BOS_BUFFER_ATR * current_atr
    )


# ============================================================
# ADVERSE CHoCH
# ============================================================

def adverse_choch(
    candles: Sequence[Candle],
    side: Side,
) -> bool:

    swings = confirmed_swings(candles)

    current = candles[-1]

    if side == Side.LONG:

        swing = latest_confirmed_swing(swings, False)

        if swing is None:
            return True

        return current.close < swing.price

    swing = latest_confirmed_swing(swings, True)

    if swing is None:
        return True

    return current.close > swing.price


# ============================================================
# STRUCTURE SCORE
# ============================================================

def structure_score(
    candles: Sequence[Candle],
    side: Side,
    sr_level: Optional[SRLevel],
) -> int:

    score = 0

    # Direction
    if side == Side.LONG and candles[-1].close > candles[-1].open:
        score += 5

    elif side == Side.SHORT and candles[-1].close < candles[-1].open:
        score += 5

    # BOS
    if structure_bos(candles, side):
        score += 5

    # S/R proximity
    current_atr = atr(candles, CONFIG.ATR_PERIOD)

    if (
        current_atr is not None
        and sr_level is not None
    ):

        distance = abs(
            candles[-1].close - sr_level.price
        ) / current_atr

        if distance <= 0.5:
            score += 3

    # No adverse CHoCH
    if not adverse_choch(candles, side):
        score += 2

    return score


# ============================================================
# RANGE BREAKOUT
# ============================================================

def range_breakout(
    candles: Sequence[Candle],
    side: Side,
) -> bool:

    if len(candles) < CONFIG.RANGE_LOOKBACK + 1:
        return False

    current = candles[-1]

    previous = candles[
        -CONFIG.RANGE_LOOKBACK - 1:-1
    ]

    range_high = max(
        c.high for c in previous
    )

    range_low = min(
        c.low for c in previous
    )

    if current.range <= 0:
        return False

    body_ratio = (
        current.body / current.range
    )

    if body_ratio < 0.6:
        return False

    if side == Side.LONG:
        return current.close > range_high

    return current.close < range_low


# ============================================================
# LIQUIDITY SWEEP
# ============================================================

def liquidity_sweep(
    candles: Sequence[Candle],
    side: Side,
) -> bool:

    if len(candles) < 20:
        return False

    current = candles[-1]

    current_atr = atr(
        candles,
        CONFIG.ATR_PERIOD,
    )

    if current_atr is None or current_atr <= 0:
        return False

    swings = confirmed_swings(candles)

    if side == Side.LONG:

        swing = latest_confirmed_swing(
            swings,
            False,
        )

        if swing is None:
            return False

        if current.low >= swing.price:
            return False

        distance = (
            swing.price - current.low
        ) / current_atr

        return (
            CONFIG.SWEEP_MIN_ATR
            <= distance
            <= CONFIG.SWEEP_MAX_ATR
            and current.close > swing.price
        )

    swing = latest_confirmed_swing(
        swings,
        True,
    )

    if swing is None:
        return False

    if current.high <= swing.price:
        return False

    distance = (
        current.high - swing.price
    ) / current_atr

    return (
        CONFIG.SWEEP_MIN_ATR
        <= distance
        <= CONFIG.SWEEP_MAX_ATR
        and current.close < swing.price
    )


# ============================================================
# PULLBACK
# ============================================================

def valid_pullback(
    candles: Sequence[Candle],
    side: Side,
    level: SRLevel,
) -> bool:

    current = candles[-1]

    current_atr = atr(
        candles,
        CONFIG.ATR_PERIOD,
    )

    if current_atr is None:
        return False

    if side == Side.LONG:

        if level.kind != "SUPPORT":
            return False

        if level.price >= current.close:
            return False

        dist = (
            current.close - level.price
        ) / current_atr

        penetration = (
            level.price - current.low
        ) / current_atr

        return (
            0.2 <= dist <= 1.0
            and penetration <= 0.5
        )

    if level.kind != "RESISTANCE":
        return False

    if level.price <= current.close:
        return False

    dist = (
        level.price - current.close
    ) / current_atr

    penetration = (
        current.high - level.price
    ) / current_atr

    return (
        0.2 <= dist <= 1.0
        and penetration <= 0.5
    )


# ============================================================
# BREAKOUT + RETEST
# ============================================================

def breakout_retest(
    candles: Sequence[Candle],
    side: Side,
    level: SRLevel,
) -> bool:

    if len(candles) < 2:
        return False

    current = candles[-1]

    current_atr = atr(
        candles,
        CONFIG.ATR_PERIOD,
    )

    if current_atr is None:
        return False

    zone = 0.3 * current_atr

    if side == Side.LONG:

        if current.close < level.price:
            return False

        if current.low < level.price - zone:
            return False

        return True

    if current.close > level.price:
        return False

    if current.high > level.price + zone:
        return False

    return True


# ============================================================
# 5M REJECTION
# ============================================================

def rejection_5m(
    candle: Candle,
    side: Side,
) -> bool:

    if candle.range <= 0:
        return False

    body = candle.body

    if side == Side.LONG:

        lower_wick = (
            min(candle.open, candle.close)
            - candle.low
        )

        return (
            lower_wick >= 0.5 * candle.range
            and body <= 0.3 * candle.range
            and candle.close >= candle.open
        )

    upper_wick = (
        candle.high
        - max(candle.open, candle.close)
    )

    return (
        upper_wick >= 0.5 * candle.range
        and body <= 0.3 * candle.range
        and candle.close <= candle.open
    )


# ============================================================
# 5M BOS
# ============================================================

def bos_5m(
    candles: Sequence[Candle],
    side: Side,
) -> bool:

    if len(candles) < 10:
        return False

    current = candles[-1]

    current_atr = atr(
        candles,
        CONFIG.ATR_PERIOD,
    )

    if current_atr is None:
        return False

    swings = confirmed_swings(
        candles,
        CONFIG.SWING_N,
    )

    if side == Side.LONG:

        swing = latest_confirmed_swing(
            swings,
            True,
        )

        if swing is None:
            return False

        return (
            current.close
            >
            swing.price
            + CONFIG.BOS_BUFFER_ATR * current_atr
        )

    swing = latest_confirmed_swing(
        swings,
        False,
    )

    if swing is None:
        return False

    return (
        current.close
        <
        swing.price
        - CONFIG.BOS_BUFFER_ATR * current_atr
    )


# ============================================================
# OI 81-ROW MATRIX
# ============================================================

def ternary(
    value: float,
    low: float,
    high: float,
) -> str:

    if value <= low:
        return "D"

    if value >= high:
        return "U"

    return "N"


def oi_state(value: float) -> str:
    return ternary(value, -0.01, 0.01)


def funding_state(value: float) -> str:
    return ternary(value, -0.001, 0.001)


def raw_oi_score(
    oi15: str,
    oi1h: str,
    funding15: str,
    funding1h: str,
) -> int:

    directional = {
        "U": 1,
        "N": 0,
        "D": -1,
    }

    oi_component = (
        directional[oi15] * 2
        +
        directional[oi1h] * 1
    )

    funding_component = (
        -directional[funding15] * 2
        +
        -directional[funding1h] * 1
    )

    return oi_component + funding_component


def oi_long_score(
    snapshot: OISnapshot,
) -> int:

    oi15 = oi_state(
        snapshot.oi_change_15m
    )

    oi1h = oi_state(
        snapshot.oi_change_1h
    )

    funding15 = funding_state(
        snapshot.funding_change_15m
    )

    funding1h = funding_state(
        snapshot.funding_change_1h
    )

    raw = raw_oi_score(
        oi15,
        oi1h,
        funding15,
        funding1h,
    )

    raw = max(-6, min(6, raw))

    return math.floor(
        ((raw + 6) * 10 / 12) + 0.5
    )


def oi_short_score(
    snapshot: OISnapshot,
) -> int:

    return 10 - oi_long_score(snapshot)


# ============================================================
# REGIME
# ============================================================

def classify_market_state(
    candles: Sequence[Candle],
) -> MarketState:

    if len(candles) < 250:
        raise ValueError(
            "Insufficient history for regime"
        )

    closes = [
        c.close for c in candles
    ]

    e50 = ema(closes, 50)
    e200 = ema(closes, 200)

    current_atr = atr(
        candles,
        CONFIG.ATR_PERIOD,
    )

    atr_values: list[float] = []

    for i in range(
        CONFIG.ATR_PERIOD + 1,
        len(candles),
    ):

        value = atr(
            candles[:i],
            CONFIG.ATR_PERIOD,
        )

        if value is not None:
            atr_values.append(value)

    atr_sma = sma(
        atr_values[:-1],
        50,
    )

    if (
        current_atr is not None
        and atr_sma is not None
        and current_atr > 2 * atr_sma
    ):
        return MarketState.HIGH_VOL

    if (
        e50 is not None
        and e200 is not None
        and closes[-1] > e50 > e200
    ):
        return MarketState.BULL

    if (
        e50 is not None
        and e200 is not None
        and closes[-1] < e50 < e200
    ):
        return MarketState.BEAR

    return MarketState.RANGE


def regime_component(
    state: MarketState,
) -> int:

    if state == MarketState.BULL:
        return 1

    if state == MarketState.BEAR:
        return -1

    return 0


def regime_score(
    btc_h4: MarketState,
    btc_h1: MarketState,
    eth_h4: MarketState,
    eth_h1: MarketState,
) -> int:

    raw = (
        regime_component(btc_h4)
        + regime_component(btc_h1)
        + regime_component(eth_h4)
        + regime_component(eth_h1)
    )

    mapping = {
        -4: 0,
        -3: 2,
        -2: 4,
        -1: 6,
         0: 8,
         1: 9,
         2: 11,
         3: 13,
         4: 15,
    }

    return mapping[raw]


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class SetupCandidate:
    symbol: str
    side: Side
    setup_type: SetupType

    detected_at: int
    detection_candle_time: int

    sr_level: Optional[SRLevel]

    score: int = 0

    regime_score: int = 0
    htf_score: int = 0
    structure_score: int = 0
    setup_score: int = 0
    volume_score: int = 0
    oi_score: int = 0
    funding_score: int = 0
    taker_score: int = 0
    trigger_score: int = 0
    rr_score: int = 0

    trigger_confirmed: bool = False

    state: SignalState = SignalState.DETECTED

    rejection: Optional[RejectCode] = None


@dataclass
class TradeLevels:
    entry: float
    stop_loss: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float


@dataclass
class Signal:
    candidate: SetupCandidate
    levels: TradeLevels
    created_at: int

    @property
    def score(self) -> int:
        return self.candidate.score


# ============================================================
# STATE MACHINE
# ============================================================

VALID_TRANSITIONS = {
    SignalState.DETECTED: {
        SignalState.VALIDATED,
        SignalState.EXPIRED,
        SignalState.INVALIDATED,
        SignalState.CANCELLED,
    },

    SignalState.VALIDATED: {
        SignalState.ARMED,
        SignalState.EXPIRED,
        SignalState.INVALIDATED,
        SignalState.CANCELLED,
    },

    SignalState.ARMED: {
        SignalState.TRIGGERED,
        SignalState.EXPIRED,
        SignalState.INVALIDATED,
        SignalState.CANCELLED,
    },

    SignalState.TRIGGERED: {
        SignalState.ACTIVE,
        SignalState.EXPIRED,
        SignalState.INVALIDATED,
        SignalState.CANCELLED,
    },

    SignalState.ACTIVE: {
        SignalState.TP1,
        SignalState.SL,
        SignalState.CANCELLED,
    },

    SignalState.TP1: {
        SignalState.TP2,
        SignalState.SL,
        SignalState.CLOSED,
    },

    SignalState.TP2: {
        SignalState.CLOSED,
    },

    SignalState.CLOSED: set(),
    SignalState.EXPIRED: set(),
    SignalState.INVALIDATED: set(),
    SignalState.CANCELLED: set(),
    SignalState.SL: set(),
}


def transition(
    candidate: SetupCandidate,
    target: SignalState,
) -> None:

    allowed = VALID_TRANSITIONS[
        candidate.state
    ]

    if target not in allowed:
        raise ValueError(
            f"Invalid state transition: "
            f"{candidate.state} -> {target}"
        )

    candidate.state = target


# ============================================================
# CORRELATION
# ============================================================

def returns(
    candles: Sequence[Candle],
) -> list[float]:

    if len(candles) < 2:
        return []

    result = []

    for previous, current in zip(
        candles[:-1],
        candles[1:],
    ):

        if previous.close <= 0:
            continue

        result.append(
            current.close / previous.close - 1
        )

    return result


def pearson(
    a: Sequence[float],
    b: Sequence[float],
) -> Optional[float]:

    n = min(len(a), len(b))

    if n < 2:
        return None

    a = list(a[-n:])
    b = list(b[-n:])

    ma = statistics.mean(a)
    mb = statistics.mean(b)

    numerator = sum(
        (x - ma) * (y - mb)
        for x, y in zip(a, b)
    )

    da = math.sqrt(
        sum((x - ma) ** 2 for x in a)
    )

    db = math.sqrt(
        sum((y - mb) ** 2 for y in b)
    )

    if da == 0 or db == 0:
        return None

    return numerator / (da * db)


# ============================================================
# SETUP DETECTION
# ============================================================

def detect_setup(
    symbol: str,
    side: Side,
    candles15: Sequence[Candle],
    sr_level: Optional[SRLevel],
) -> Optional[SetupType]:

    if range_breakout(candles15, side):
        return SetupType.RANGE_BREAKOUT

    if (
        sr_level is not None
        and valid_pullback(
            candles15,
            side,
            sr_level,
        )
    ):
        return SetupType.PULLBACK

    if (
        sr_level is not None
        and breakout_retest(
            candles15,
            side,
            sr_level,
        )
    ):
        return SetupType.BREAKOUT_RETEST

    if liquidity_sweep(
        candles15,
        side,
    ):
        return SetupType.LIQUIDITY_SWEEP

    return None


# ============================================================
# SETUP VALIDATION
# ============================================================

def setup_valid(
    setup_type: SetupType,
    candles15: Sequence[Candle],
    side: Side,
    sr_level: Optional[SRLevel],
) -> bool:

    if setup_type == SetupType.RANGE_BREAKOUT:
        return range_breakout(
            candles15,
            side,
        )

    if setup_type == SetupType.PULLBACK:
        return (
            sr_level is not None
            and valid_pullback(
                candles15,
                side,
                sr_level,
            )
        )

    if setup_type == SetupType.BREAKOUT_RETEST:
        return (
            sr_level is not None
            and breakout_retest(
                candles15,
                side,
                sr_level,
            )
        )

    if setup_type == SetupType.LIQUIDITY_SWEEP:
        return liquidity_sweep(
            candles15,
            side,
        )

    return False


# ============================================================
# STAGE A
# ============================================================

def stage_a_detect(
    symbol: str,
    side: Side,
    candles15: Sequence[Candle],
    sr_level: Optional[SRLevel],
) -> Optional[SetupCandidate]:

    if not candles15:
        return None

    setup_type = detect_setup(
        symbol,
        side,
        candles15,
        sr_level,
    )

    if setup_type is None:
        return None

    valid = setup_valid(
        setup_type,
        candles15,
        side,
        sr_level,
    )

    if not valid:
        return None

    candidate = SetupCandidate(
        symbol=symbol,
        side=side,
        setup_type=setup_type,
        detected_at=int(time.time() * 1000),
        detection_candle_time=candles15[-1].close_time,
        sr_level=sr_level,
    )

    transition(
        candidate,
        SignalState.VALIDATED,
    )

    return candidate


# ============================================================
# TECHNICAL TARGETS
# ============================================================

def risk_reward(
    entry: float,
    stop: float,
    target: float,
    side: Side,
) -> float:

    risk = (
        entry - stop
        if side == Side.LONG
        else stop - entry
    )

    if risk <= 0:
        return 0.0

    reward = (
        target - entry
        if side == Side.LONG
        else entry - target
    )

    return reward / risk


# ============================================================
# STAGE B
# ============================================================

def stage_b_finalize(
    candidate: SetupCandidate,
    entry: float,
    stop_loss: float,
    tp1: float,
    tp2: float,
) -> Optional[Signal]:

    side = candidate.side

    if side == Side.LONG:

        if not (
            stop_loss < entry < tp1 < tp2
        ):
            candidate.rejection = (
                RejectCode.INVALID_ENTRY
            )
            return None

    else:

        if not (
            tp2 < tp1 < entry < stop_loss
        ):
            candidate.rejection = (
                RejectCode.INVALID_ENTRY
            )
            return None

    rr1 = risk_reward(
        entry,
        stop_loss,
        tp1,
        side,
    )

    rr2 = risk_reward(
        entry,
        stop_loss,
        tp2,
        side,
    )

    if rr1 < CONFIG.MIN_RR_TP1:
        candidate.rejection = (
            RejectCode.LOW_RR_TP1
        )
        return None

    if rr2 < CONFIG.MIN_RR_TP2:
        candidate.rejection = (
            RejectCode.LOW_RR_TP2
        )
        return None

    sl_distance = abs(
        entry - stop_loss
    )

    if entry <= 0:
        candidate.rejection = (
            RejectCode.INVALID_ENTRY
        )
        return None

    sl_percent = (
        sl_distance / entry * 100
    )

    if sl_percent > CONFIG.MAX_SL_PERCENT:
        candidate.rejection = (
            RejectCode.SL_TOO_WIDE
        )
        return None

    levels = TradeLevels(
        entry=entry,
        stop_loss=stop_loss,
        tp1=tp1,
        tp2=tp2,
        rr1=rr1,
        rr2=rr2,
    )

    transition(
        candidate,
        SignalState.ARMED,
    )

    candidate.trigger_confirmed = True

    transition(
        candidate,
        SignalState.TRIGGERED,
    )

    candidate.rr_score = 5

    candidate.score = (
        candidate.regime_score
        + candidate.htf_score
        + candidate.structure_score
        + candidate.setup_score
        + candidate.volume_score
        + candidate.oi_score
        + candidate.funding_score
        + candidate.taker_score
        + candidate.trigger_score
        + candidate.rr_score
    )

    return Signal(
        candidate=candidate,
        levels=levels,
        created_at=int(
            time.time() * 1000
        ),
    )


# ============================================================
# GRADING
# ============================================================

def grade(score: int) -> str:

    if score >= CONFIG.GRADE_A_PLUS:
        return "A+"

    if score >= CONFIG.GRADE_A:
        return "A"

    if score >= CONFIG.GRADE_B:
        return "B"

    return "C"


# ============================================================
# SIGNAL ENGINE
# ============================================================

class StrategyEngine:

    def __init__(
        self,
        provider: DataProvider,
        config: StrategyConfig = CONFIG,
    ):

        self.provider = provider
        self.config = config

        self.active: dict[str, SetupCandidate] = {}
        self.history: list[SetupCandidate] = []

    async def universe(self) -> list[str]:

        symbols = await self.provider.symbols()

        volumes = []

        for symbol in symbols:

            try:
                volume = (
                    await self.provider
                    .quote_volume_24h(symbol)
                )

                volumes.append(
                    (symbol, volume)
                )

            except Exception:
                continue

        volumes.sort(
            key=lambda x: x[1],
            reverse=True,
        )

        return [
            symbol
            for symbol, _ in
            volumes[:self.config.TOP_SYMBOLS_LIMIT]
        ]

    async def process_symbol(
        self,
        symbol: str,
    ) -> list[SetupCandidate]:

        candles = await self.provider.candles(
            symbol,
            "15m",
            300,
        )

        candles = closed_candles(candles)

        if len(candles) < 220:
            return []

        candidates = []

        for side in (
            Side.LONG,
            Side.SHORT,
        ):

            candidate = stage_a_detect(
                symbol,
                side,
                candles,
                None,
            )

            if candidate is not None:
                candidates.append(
                    candidate
                )

        return candidates

    async def scan(self) -> list[SetupCandidate]:

        universe = await self.universe()

        result = []

        for symbol in universe:

            try:
                result.extend(
                    await self.process_symbol(
                        symbol
                    )
                )

            except Exception:
                continue

        return result


# ============================================================
# SCHEDULER
# ============================================================

class Scheduler:

    def __init__(
        self,
        engine: StrategyEngine,
    ):

        self.engine = engine

        self.last_5m_close: Optional[int] = None
        self.last_15m_close: Optional[int] = None

    async def run_5m_cycle(self) -> None:
        """
        Only closed 5m candle trigger processing.
        """
        pass

    async def run_htf_cycle(self) -> None:
        """
        Refresh:
            15m
            H1
            H4
            D1

        Only when corresponding candles close.
        """

        await self.engine.scan()

    async def run_forever(self) -> None:

        while True:

            try:
                await self.run_htf_cycle()
            except Exception as exc:
                print(
                    f"HTF cycle error: {exc}"
                )

            try:
                await self.run_5m_cycle()
            except Exception as exc:
                print(
                    f"5m cycle error: {exc}"
                )

            await asyncio.sleep(10)


# ============================================================
# MAIN
# ============================================================

async def main() -> None:
    raise SystemExit(
        "Configure DataProvider before live execution."
    )


if __name__ == "__main__":
    asyncio.run(main())
