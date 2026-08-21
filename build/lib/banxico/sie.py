"""Small Banxico SIE REST client used by fixing producers."""

from __future__ import annotations

import datetime as dt
import json
import urllib.parse
import urllib.request
from collections.abc import Iterable
from typing import Any

from banxico.settings import BANXICO_SIE_BASE_URL


class BanxicoSieError(RuntimeError):
    """Raised when a Banxico SIE request or payload cannot be handled."""


class BanxicoSieClient:
    """Banxico SIE REST client that never logs or serializes the API token."""

    def __init__(
        self,
        *,
        token: str,
        base_url: str = BANXICO_SIE_BASE_URL,
        timeout: float = 30.0,
    ) -> None:
        normalized_token = token.strip()
        if not normalized_token:
            raise BanxicoSieError("Banxico SIE token is empty.")
        self._token = normalized_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def fetch_series_metadata(
        self,
        series_ids: Iterable[str],
        *,
        locale: str = "es",
    ) -> list[dict[str, Any]]:
        """Fetch Banxico metadata for one or more SIE series ids."""

        clean_series_ids = _normalize_series_ids(series_ids)
        query = urllib.parse.urlencode({"mediaType": "json", "locale": locale})
        path = f"/series/{','.join(clean_series_ids)}?{query}"
        payload = self._get_json(path)
        return _series_payload(payload)

    def fetch_series_data(
        self,
        series_ids: Iterable[str],
        *,
        start_date: dt.date,
        end_date: dt.date,
    ) -> list[dict[str, Any]]:
        """Fetch Banxico observations for one or more SIE series ids."""

        clean_series_ids = _normalize_series_ids(series_ids)
        start = _format_api_date(start_date)
        end = _format_api_date(end_date)
        query = urllib.parse.urlencode({"mediaType": "json"})
        path = f"/series/{','.join(clean_series_ids)}/datos/{start}/{end}?{query}"
        payload = self._get_json(path)
        return _series_payload(payload)

    def _get_json(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            headers={
                "Accept": "application/json",
                "Bmx-Token": self._token,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except Exception as exc:  # pragma: no cover - exact urllib errors vary by platform.
            raise BanxicoSieError(f"Banxico SIE request failed for path {path!r}.") from exc

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            raise BanxicoSieError("Banxico SIE response was not valid JSON.") from exc

        if isinstance(payload, dict) and "error" in payload:
            message = payload["error"].get("mensaje") if isinstance(payload["error"], dict) else None
            raise BanxicoSieError(message or "Banxico SIE returned an error payload.")
        if not isinstance(payload, dict):
            raise BanxicoSieError("Banxico SIE response JSON must be an object.")
        return payload


def _normalize_series_ids(series_ids: Iterable[str]) -> list[str]:
    normalized = [item.strip() for item in series_ids]
    if not normalized or any(not item for item in normalized):
        raise BanxicoSieError("At least one non-empty Banxico SIE series id is required.")
    if len(normalized) > 20:
        raise BanxicoSieError("Banxico SIE accepts at most 20 series per request.")
    return normalized


def _format_api_date(value: dt.date) -> str:
    if isinstance(value, dt.datetime):
        value = value.date()
    return value.isoformat()


def _series_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    bmx = payload.get("bmx")
    if not isinstance(bmx, dict):
        raise BanxicoSieError("Banxico SIE payload is missing the bmx object.")
    series = bmx.get("series")
    if not isinstance(series, list):
        raise BanxicoSieError("Banxico SIE payload is missing the bmx.series list.")
    return [item for item in series if isinstance(item, dict)]


__all__ = ["BanxicoSieClient", "BanxicoSieError"]
