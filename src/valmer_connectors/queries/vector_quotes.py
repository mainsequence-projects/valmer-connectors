from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd
from msm.settings import ASSET_IDENTIFIER_DIMENSION
from sqlalchemy import Float

from valmer_connectors.data_nodes.valmer_vector_storage import (
    ValmerVectorPricesStorage,
)
from valmer_connectors.queries._normalization import (
    clean_valmer_identifiers,
    to_utc_datetime,
)

DEFAULT_LATEST_SEARCH_START = dt.datetime(1900, 1, 1, tzinfo=dt.UTC)
DEFAULT_VALMER_VECTOR_COLUMNS = (
    "dirty_price",
    "clean_price",
    "yield_rate",
    "duration",
    "macaulay_duration",
    "monetary_duration",
    "convexity",
    "spread",
)
VALMER_VECTOR_INDEX_COLUMNS = ("time_index", ASSET_IDENTIFIER_DIMENSION)


def valmer_vector_node_identifier() -> str:
    """Return the published Valmer vector DataNode identifier."""

    return str(ValmerVectorPricesStorage.__metatable_identifier__)


def valmer_vector_node() -> Any:
    """Return a read-only APIDataNode for Valmer vector storage."""

    from mainsequence.meta_tables import APIDataNode

    return APIDataNode.build_from_identifier(
        identifier=valmer_vector_node_identifier(),
    )


def valmer_vector_storage_columns() -> frozenset[str]:
    """Return columns exposed by the Valmer vector storage model."""

    return frozenset(str(column.name) for column in ValmerVectorPricesStorage.__table__.columns)


def filter_valmer_vector_columns(columns: Sequence[str] | None) -> list[str]:
    """Return requested Valmer vector columns restricted to the storage schema."""

    requested = columns or DEFAULT_VALMER_VECTOR_COLUMNS
    candidates = list(dict.fromkeys([*VALMER_VECTOR_INDEX_COLUMNS, *requested]))
    available = valmer_vector_storage_columns()
    return [column for column in candidates if column in available]


def read_valmer_history(
    asset_identifiers: Iterable[object],
    start_date: dt.datetime | pd.Timestamp,
    end_date: dt.datetime | pd.Timestamp | None = None,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read Valmer vector history scoped by canonical asset identifier."""

    identifiers = clean_valmer_identifiers(asset_identifiers)
    if not identifiers:
        return pd.DataFrame()
    start = to_utc_datetime(start_date)
    if start is None:
        raise ValueError("start_date is required for Valmer vector history reads.")
    frame = valmer_vector_node().get_df_between_dates(
        start_date=start,
        end_date=to_utc_datetime(end_date),
        dimension_filters={ASSET_IDENTIFIER_DIMENSION: identifiers},
        columns=filter_valmer_vector_columns(columns),
    )
    return normalize_valmer_quote_frame(frame)


def read_valmer_last_observation(
    asset_identifiers: Iterable[object],
    as_of: dt.datetime | pd.Timestamp | None = None,
    columns: Sequence[str] | None = None,
    latest_search_start: dt.datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Read the latest Valmer row per asset at or before ``as_of``."""

    identifiers = clean_valmer_identifiers(asset_identifiers)
    if not identifiers:
        return pd.DataFrame()
    end = to_utc_datetime(as_of) or dt.datetime.now(dt.UTC)
    start = to_utc_datetime(latest_search_start) or DEFAULT_LATEST_SEARCH_START
    frame = read_valmer_history(
        identifiers,
        start_date=start,
        end_date=end,
        columns=columns,
    )
    if frame.empty:
        return frame
    return (
        frame.sort_values("time_index")
        .groupby(ASSET_IDENTIFIER_DIMENSION, as_index=False, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )


def latest_dirty_price_by_identifier(
    asset_identifiers: Iterable[object],
    as_of: dt.datetime | pd.Timestamp | None = None,
) -> dict[str, float]:
    """Return latest Valmer dirty prices keyed by canonical asset identifier."""

    frame = read_valmer_last_observation(
        asset_identifiers,
        as_of=as_of,
        columns=["dirty_price"],
    )
    if frame.empty or "dirty_price" not in frame.columns:
        return {}
    return {
        str(row.asset_identifier): float(row.dirty_price)
        for row in frame.itertuples(index=False)
        if pd.notna(row.dirty_price)
    }


def read_valmer_yield_history(
    asset_identifiers: Iterable[object],
    start_date: dt.datetime | pd.Timestamp,
    end_date: dt.datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Return Valmer yield history and related risk fields."""

    return read_valmer_history(
        asset_identifiers,
        start_date=start_date,
        end_date=end_date,
        columns=["yield_rate", "duration", "monetary_duration"],
    )


def normalize_valmer_quote_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Normalize Valmer vector rows to ordinary columns and quote dtypes."""

    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if any(name in VALMER_VECTOR_INDEX_COLUMNS for name in out.index.names):
        out = out.reset_index()
    if "time_index" in out.columns:
        out["time_index"] = pd.to_datetime(out["time_index"], utc=True, errors="coerce")
    for column in _numeric_valmer_vector_columns():
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _numeric_valmer_vector_columns() -> frozenset[str]:
    return frozenset(
        str(column.name)
        for column in ValmerVectorPricesStorage.__table__.columns
        if isinstance(column.type, Float)
    )
