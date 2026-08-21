from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from typing import Any

import pandas as pd


def clean_valmer_identifiers(values: Iterable[object]) -> list[str]:
    """Return unique, non-empty Valmer identifiers preserving input order."""

    identifiers: list[str] = []
    seen: set[str] = set()
    for value in values:
        identifier = string_or_none(value)
        if identifier is None:
            continue
        lower_identifier = identifier.lower()
        if lower_identifier in {"nan", "none"} or identifier in seen:
            continue
        seen.add(identifier)
        identifiers.append(identifier)
    return identifiers


def string_or_none(value: Any) -> str | None:
    """Return ``value`` as a stripped string unless it is missing."""

    if value in (None, ""):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def to_utc_datetime(value: dt.datetime | pd.Timestamp | None) -> dt.datetime | None:
    """Normalize a timestamp-like value to a UTC datetime."""

    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()
