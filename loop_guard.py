from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LoopDetected(RuntimeError):
    adapter: str
    reason: str
    signature: str

    def __str__(self) -> str:
        return f"loop_detected adapter={self.adapter} reason={self.reason} signature={self.signature}"


class LoopGuard:
    def __init__(self, adapter: str) -> None:
        self.adapter = adapter
        self._request_counts: dict[str, int] = {}
        self._content_counts: dict[str, int] = {}

    def record_request(self, signature: str) -> None:
        count = self._request_counts.get(signature, 0) + 1
        self._request_counts[signature] = count
        if count > 2:
            raise LoopDetected(self.adapter, "repeated_request", signature)

    def record_content(self, signature: str) -> None:
        count = self._content_counts.get(signature, 0) + 1
        self._content_counts[signature] = count
        if count > 1:
            raise LoopDetected(self.adapter, "repeating_page_content", signature)
