"""Cross-exchange market matcher.

Loads a YAML configuration that maps canonical market names to their
exchange-specific identifiers, allowing the pipeline to find
counterpart markets across Polymarket, Opinion, and Limitless.

Expected YAML format::

    markets:
      - name: "US Presidential Election 2024"
        outcome: "yes"
        ids:
          polymarket: "abc123"
          opinion: "def456"
          limitless: "ghi789"
      - name: "BTC > 100k by Dec 2024"
        ids:
          polymarket: "xyz111"
          limitless: "xyz222"
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class MarketMatch:
    """A matched market outcome across exchanges."""

    canonical_name: str
    exchange_market_ids: dict[str, str]  # exchange_id -> market_id
    outcome: str = "yes"


class MarketMatcher:
    """Loads and queries cross-exchange market matches from YAML config.

    Usage::

        matcher = MarketMatcher()
        matcher.load_from_yaml("config/markets.yaml")
        counterparts = matcher.find_counterparts("abc123")
        paired_id = matcher.get_paired_market_id("abc123", "limitless")
    """

    def __init__(self) -> None:
        self._matches: list[MarketMatch] = []
        self._index: dict[str, list[MarketMatch]] = {}  # market_id -> matches

    # -- Loading ------------------------------------------------------------

    def load_from_yaml(self, path: str | Path) -> None:
        """Parse a YAML file and build the internal match index."""
        with open(path) as fh:
            data = yaml.safe_load(fh)

        self._matches = []
        self._index = {}

        for entry in data.get("markets", []):
            match = MarketMatch(
                canonical_name=entry["name"],
                exchange_market_ids=entry.get("ids", {}),
                outcome=entry.get("outcome", "yes"),
            )
            self._matches.append(match)
            for market_id in match.exchange_market_ids.values():
                self._index.setdefault(market_id, []).append(match)

    def load_from_list(self, matches: list[MarketMatch]) -> None:
        """Populate the matcher from a pre-built list of ``MarketMatch``."""
        self._matches = list(matches)
        self._index = {}
        for match in self._matches:
            for market_id in match.exchange_market_ids.values():
                self._index.setdefault(market_id, []).append(match)

    # -- Queries ------------------------------------------------------------

    def find_counterparts(self, market_id: str) -> list[MarketMatch]:
        """Return all ``MarketMatch`` entries that include *market_id*."""
        return self._index.get(market_id, [])

    def get_paired_market_id(
        self, market_id: str, target_exchange: str,
    ) -> str | None:
        """Find the counterpart market ID on *target_exchange*.

        Returns ``None`` if no pairing exists.
        """
        for match in self._index.get(market_id, []):
            target = match.exchange_market_ids.get(target_exchange)
            if target and target != market_id:
                return target
        return None

    @property
    def all_matches(self) -> list[MarketMatch]:
        """Return a copy of all loaded matches."""
        return list(self._matches)
