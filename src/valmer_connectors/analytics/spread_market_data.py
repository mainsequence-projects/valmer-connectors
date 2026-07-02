from __future__ import annotations

import datetime as dt
from collections.abc import Iterable

import pandas as pd
from msm.settings import ASSET_IDENTIFIER_DIMENSION

from valmer_connectors.queries import (
    clean_valmer_identifiers,
    read_valmer_last_observation,
    read_valmer_yield_history,
)
from valmer_connectors.queries._normalization import to_utc_datetime

SPREAD_SNAPSHOT_COLUMNS = (
    "yield_rate",
    "dirty_price",
    "clean_price",
    "duration",
    "macaulay_duration",
    "monetary_duration",
    "convexity",
    "spread",
)


def default_start_date() -> dt.datetime:
    """Return a default five-year UTC history start date for spread analytics."""

    return (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)).to_pydatetime()


def fetch_yield_history(
    asset_identifiers: Iterable[object],
    start_date: dt.datetime | pd.Timestamp | None = None,
    end_date: dt.datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return wide Valmer yield history indexed by ``time_index``."""

    identifiers = clean_valmer_identifiers(asset_identifiers)
    if not identifiers:
        return pd.DataFrame()
    start = to_utc_datetime(start_date) or default_start_date()
    frame = read_valmer_yield_history(
        identifiers,
        start_date=start,
        end_date=to_utc_datetime(end_date),
    )
    if frame.empty or "yield_rate" not in frame.columns:
        return pd.DataFrame()
    out = frame.copy()
    out["yield_rate"] = pd.to_numeric(out["yield_rate"], errors="coerce")
    return (
        out.pivot_table(
            index="time_index",
            columns=ASSET_IDENTIFIER_DIMENSION,
            values="yield_rate",
            aggfunc="last",
        )
        .sort_index()
        .rename_axis(columns=None)
    )


def fetch_market_snapshot(
    asset_identifiers: Iterable[object],
    as_of: dt.datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return latest Valmer market fields used for spread analytics."""

    identifiers = clean_valmer_identifiers(asset_identifiers)
    if not identifiers:
        return pd.DataFrame()
    frame = read_valmer_last_observation(
        identifiers,
        as_of=to_utc_datetime(as_of),
        columns=SPREAD_SNAPSHOT_COLUMNS,
    )
    if frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    for column in SPREAD_SNAPSHOT_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out
