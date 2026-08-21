"""Small FRED REST client that keeps API keys out of errors and logs."""

from __future__ import annotations

import datetime as dt
import json
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any

from fred.settings import FRED_API_BASE_URL


class FredApiError(RuntimeError):
    """Raised when a FRED request or response cannot be handled."""


class FredClient:
    """Client for the official FRED series metadata and observation endpoints."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = FRED_API_BASE_URL,
        timeout: float = 30.0,
    ) -> None:
        normalized_key = api_key.strip()
        if not normalized_key:
            raise FredApiError("FRED API key is empty.")
        self._api_key = normalized_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def fetch_series_metadata(self, series_id: str) -> dict[str, Any]:
        """Fetch one FRED series metadata record."""

        normalized_id = _normalize_series_id(series_id)
        payload = self._get_json("/series", {"series_id": normalized_id})
        records = payload.get("seriess")
        if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict):
            raise FredApiError(
                f"FRED metadata response for {normalized_id!r} must contain one series."
            )
        return records[0]

    def fetch_series_observations(
        self,
        series_id: str,
        *,
        start_date: dt.date,
        end_date: dt.date,
    ) -> list[dict[str, Any]]:
        """Fetch an inclusive observation window for one FRED series."""

        normalized_id = _normalize_series_id(series_id)
        payload = self._get_json(
            "/series/observations",
            {
                "series_id": normalized_id,
                "observation_start": start_date.isoformat(),
                "observation_end": end_date.isoformat(),
                "sort_order": "asc",
            },
        )
        observations = payload.get("observations")
        if not isinstance(observations, list):
            raise FredApiError(
                f"FRED observations response for {normalized_id!r} is missing observations."
            )
        return [item for item in observations if isinstance(item, dict)]

    def _get_json(self, endpoint: str, params: Mapping[str, str]) -> dict[str, Any]:
        query = urllib.parse.urlencode(
            {**dict(params), "api_key": self._api_key, "file_type": "json"}
        )
        request = urllib.request.Request(
            f"{self.base_url}{endpoint}?{query}",
            headers={"Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except Exception:
            raise FredApiError(f"FRED request failed for endpoint {endpoint!r}.") from None

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            raise FredApiError("FRED response was not valid JSON.") from None
        if not isinstance(payload, dict):
            raise FredApiError("FRED response JSON must be an object.")
        if payload.get("error_code") or payload.get("error_message"):
            raise FredApiError(str(payload.get("error_message") or "FRED returned an error."))
        return payload


def _normalize_series_id(series_id: str) -> str:
    normalized = str(series_id).strip()
    if not normalized:
        raise FredApiError("A non-empty FRED series id is required.")
    return normalized


__all__ = ["FredApiError", "FredClient"]
