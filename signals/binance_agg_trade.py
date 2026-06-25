"""Binance USDT-M futures aggTrade feed — rolling price buffer for momentum signals."""

from __future__ import annotations

import asyncio
import json
import time as t
from collections import deque
from typing import Deque, Dict, Optional, Tuple

import websockets  # type: ignore

STALE_CUTOFF_SECS = 5.0
_MAX_BUFFER_SECS = 120.0
_CONNECT_TIMEOUT_SECS = 15.0
_FUTURES_WS_BASE = "wss://fstream.binance.com"


class BinanceAggTradeSignal:
    """One futures aggTrade connection per symbol; shared across workers on that asset."""

    _instances: Dict[str, "BinanceAggTradeSignal"] = {}
    _instance_lock = asyncio.Lock()

    @classmethod
    async def get_or_create(cls, symbol: str) -> "BinanceAggTradeSignal":
        async with cls._instance_lock:
            sym = symbol.upper()
            if sym not in cls._instances:
                inst = cls(sym)
                cls._instances[sym] = inst
                asyncio.create_task(inst._run())
            return cls._instances[sym]

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.last_price = 0.0
        self.last_update = 0.0
        self._prices: Deque[Tuple[float, float]] = deque()
        self._running = False

    @property
    def is_stale(self) -> bool:
        if self.last_update <= 0:
            return True
        return (t.time() - self.last_update) >= STALE_CUTOFF_SECS

    @property
    def is_fresh(self) -> bool:
        return not self.is_stale

    def price_delta(self, lookback_secs: float) -> Optional[float]:
        """Percent move over lookback_secs. None if insufficient data."""
        if lookback_secs <= 0 or self.last_price <= 0:
            return None
        now = t.time()
        cutoff = now - lookback_secs
        self._trim(now)

        oldest_price = None
        for ts, px in self._prices:
            if ts >= cutoff:
                oldest_price = px
                break
        if oldest_price is None or oldest_price <= 0:
            if len(self._prices) >= 2:
                oldest_price = self._prices[0][1]
            else:
                return None
        if oldest_price <= 0:
            return None
        return (self.last_price - oldest_price) / oldest_price

    def _trim(self, now: Optional[float] = None) -> None:
        now = now if now is not None else t.time()
        max_cutoff = now - _MAX_BUFFER_SECS
        while self._prices and self._prices[0][0] < max_cutoff:
            self._prices.popleft()

    def _on_trade(self, price: float, ts: Optional[float] = None) -> None:
        now = ts if ts is not None else t.time()
        self.last_price = price
        self.last_update = now
        self._prices.append((now, price))
        self._trim(now)

    @staticmethod
    def _parse_trade_message(raw: str) -> Optional[Tuple[float, float]]:
        msg = json.loads(raw)
        data = msg.get("data", msg)
        px = float(data.get("p", 0))
        if px <= 0:
            return None
        trade_ts = float(data.get("T", 0)) / 1000.0
        return px, trade_ts if trade_ts > 0 else t.time()

    async def _run(self) -> None:
        if self._running:
            return
        self._running = True
        stream = f"{self.symbol.lower()}usdt@aggTrade"
        url = f"{_FUTURES_WS_BASE}/ws/{stream}"
        while True:
            try:
                print(
                    f"📡 [Binance futures aggTrade] {self.symbol}: "
                    f"connecting to {url} (timeout {_CONNECT_TIMEOUT_SECS}s)..."
                )
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=15,
                    open_timeout=_CONNECT_TIMEOUT_SECS,
                ) as ws:
                    print(f"📡 [Binance futures aggTrade] Connected: {stream}")
                    async for raw in ws:
                        parsed = self._parse_trade_message(raw)
                        if parsed is None:
                            continue
                        px, trade_ts = parsed
                        self._on_trade(px, trade_ts)
            except Exception as e:
                print(
                    f"⚠️ [Binance aggTrade] {self.symbol}: {e} — "
                    f"reconnecting in 3s (endpoint: {_FUTURES_WS_BASE})"
                )
                await asyncio.sleep(3)
