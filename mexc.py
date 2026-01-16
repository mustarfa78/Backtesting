from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from http_client import get_json


LOGGER = logging.getLogger(__name__)
BASE_URL = "https://contract.mexc.com"


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    close: float


@dataclass(frozen=True)
class ContractInfo:
    symbol: str
    base_asset: str


class MexcFuturesClient:
    def __init__(self, session):
        self.session = session

    def list_contracts(self) -> List[ContractInfo]:
        url = f"{BASE_URL}/api/v1/contract/ticker"
        data = get_json(self.session, url)
        contracts = []
        for item in data.get("data", []):
            symbol = item.get("symbol")
            if not symbol:
                continue
            base_asset = symbol.split("_")[0]
            contracts.append(ContractInfo(symbol=symbol, base_asset=base_asset))
        return contracts

    def map_ticker_to_symbol(self, ticker: str, contracts: Iterable[ContractInfo]) -> Optional[str]:
        for contract in contracts:
            if contract.base_asset.upper() == ticker.upper():
                return contract.symbol
        return None

    def fetch_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "Min1",
    ) -> List[Candle]:
        start_ts = int(start_time.replace(tzinfo=timezone.utc).timestamp())
        end_ts = int(end_time.replace(tzinfo=timezone.utc).timestamp())
        url = f"{BASE_URL}/api/v1/contract/kline/{symbol}"
        params = {"interval": interval, "start": start_ts, "end": end_ts}
        data = get_json(self.session, url, params=params)
        candles = []
        for item in data.get("data", []):
            try:
                ts = datetime.fromtimestamp(int(item[0]) / 1000, tz=timezone.utc)
                close = float(item[4])
            except (IndexError, ValueError, TypeError):
                continue
            candles.append(Candle(timestamp=ts, close=close))
        candles.sort(key=lambda c: c.timestamp)
        return candles

    def has_candle_covering(self, candles: List[Candle], target: datetime) -> bool:
        target = target.replace(tzinfo=timezone.utc)
        return any(c.timestamp == target for c in candles)

    def get_close_at(self, candles: List[Candle], target: datetime) -> Optional[float]:
        for candle in candles:
            if candle.timestamp == target:
                return candle.close
        return None

    def ensure_trading(self, symbol: str, at_time: datetime) -> Tuple[bool, List[Candle]]:
        start_time = at_time.replace(tzinfo=timezone.utc) - timedelta(minutes=10)
        end_time = at_time.replace(tzinfo=timezone.utc)
        candles = self.fetch_klines(symbol, start_time=start_time, end_time=end_time)
        exists = self.has_candle_covering(candles, at_time.replace(second=0, microsecond=0) - timedelta(minutes=1))
        if not exists:
            LOGGER.info("No candle for %s at %s", symbol, at_time)
        return exists, candles


from datetime import timedelta  # noqa: E402
