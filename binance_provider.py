import time
import asyncio
import requests
from typing import Sequence, Optional
from screener2_v2 import Candle, FundingPoint, OISnapshot

class BinanceLiveDataProvider:
    def __init__(self):
        self.base_url = "https://fapi.binance.com"
        self.timeout = 5

    async def symbols(self) -> Sequence[str]:
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=self.timeout)
        )
        response.raise_for_status()
        data = response.json()
        return [
            s["symbol"] for s in data["symbols"] 
            if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
        ]

    async def candles(self, symbol: str, timeframe: str, limit: int) -> Sequence[Candle]:
        url = f"{self.base_url}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": timeframe, "limit": limit}
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, lambda: requests.get(url, params=params, timeout=self.timeout)
        )
        response.raise_for_status()
        raw = response.json()

        result = []
        for r in raw:
            result.append(Candle(
                open_time=int(r[0]),
                close_time=int(r[6]),
                open=float(r[1]),
                high=float(r[2]),
                low=float(r[3]),
                close=float(r[4]),
                volume=float(r[5]),
                quote_volume=float(r[7]),
                taker_buy_volume=float(r[9]),
                taker_sell_volume=float(r[10])
            ))
        return result

    async def funding(self, symbol: str) -> Optional[FundingPoint]:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}
        loop = asyncio.get_running_loop()
        try:
            response = await loop.run_in_executor(
                None, lambda: requests.get(url, params=params, timeout=self.timeout)
            )
            response.raise_for_status()
            data = response.json()
            return FundingPoint(
                timestamp=int(data.get("time", time.time() * 1000)),
                rate=float(data.get("lastFundingRate", 0.0))
            )
        except Exception:
            return None

    async def open_interest(self, symbol: str) -> Optional[OISnapshot]:
        url = f"{self.base_url}/fapi/v1/openInterestHist"
        params = {"symbol": symbol, "period": "5m", "limit": 12}
        loop = asyncio.get_running_loop()
        try:
            response = await loop.run_in_executor(
                None, lambda: requests.get(url, params=params, timeout=self.timeout)
            )
            response.raise_for_status()
            data = response.json()
            if len(data) >= 2:
                oi_now = float(data[-1]["sumOpenInterest"])
                oi_prev_15m = float(data[-4]["sumOpenInterest"]) if len(data) >= 4 else oi_now
                oi_prev_1h = float(data[0]["sumOpenInterest"])

                change_15m = (oi_now - oi_prev_15m) / oi_prev_15m if oi_prev_15m > 0 else 0.0
                change_1h = (oi_now - oi_prev_1h) / oi_prev_1h if oi_prev_1h > 0 else 0.0

                return OISnapshot(
                    oi_change_15m=change_15m,
                    oi_change_1h=change_1h,
                    funding_change_15m=0.0,
                    funding_change_1h=0.0
                )
        except Exception:
            pass
        return OISnapshot(0.0, 0.0, 0.0, 0.0)

    async def quote_volume_24h(self, symbol: str) -> float:
        url = f"{self.base_url}/fapi/v1/ticker/24hr"
        params = {"symbol": symbol}
        loop = asyncio.get_running_loop()
        try:
            response = await loop.run_in_executor(
                None, lambda: requests.get(url, params=params, timeout=self.timeout)
            )
            response.raise_for_status()
            data = response.json()
            return float(data.get("quoteVolume", 0.0))
        except Exception:
            return 0.0
