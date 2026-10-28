"""Request and response models of the Valmer data API (ADR 0010).

These models are the API's wire contract. Routes change only by addition;
removing or renaming a field needs a new API version. Tabular routes answer
with the ms-markets ``core.tabular_frame@v1`` frame (``TabularFrameResponse``),
whose ``meta`` carries route context such as the resolved valuation time.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_IDENTIFIERS = 500
"""Most asset identifiers one request may name."""
MAX_PAGE_SIZE = 25_000
"""Largest ``limit`` one paged request accepts."""
DEFAULT_PAGE_SIZE = 5_000
MAX_VALUATION_DATES = 2_000
MAX_COMPARISON_ROWS = 2_000

BondSortField = Literal[
    "instrument",
    "issuer",
    "series",
    "name",
    "rating",
    "spread",
    "yield_rate",
    "clean_price",
    "dirty_price",
    "duration",
    "maturity_date",
    "liquidity",
]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _clean_strings(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("Expected an array of strings.")
    cleaned: list[str] = []
    for item in value:
        text = str(item).strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


class _Paged(_Request):
    limit: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    offset: int = Field(default=0, ge=0)


class VectorSnapshotRequest(_Paged):
    """Vector rows observed on one valuation date."""

    valuation_date: dt.date | None = None
    """``None`` selects the latest valuation date."""
    asset_identifiers: list[str] | None = Field(default=None, max_length=MAX_IDENTIFIERS)
    """``None`` reads every instrument observed on the date."""
    columns: list[str] | None = None
    """Vector columns besides ``time_index`` and ``asset_identifier``."""

    _clean = field_validator("asset_identifiers", "columns", mode="before")(_clean_strings)


class VectorHistoryRequest(_Paged):
    """Vector history of named instruments."""

    asset_identifiers: list[str] = Field(min_length=1, max_length=MAX_IDENTIFIERS)
    start_date: dt.datetime | None = None
    """``None`` reads from the first observation."""
    end_date: dt.datetime | None = None
    columns: list[str] | None = None

    _clean = field_validator("asset_identifiers", "columns", mode="before")(_clean_strings)


class VectorLatestRequest(_Request):
    """The latest vector row per instrument at or before ``as_of``."""

    asset_identifiers: list[str] = Field(min_length=1, max_length=MAX_IDENTIFIERS)
    as_of: dt.datetime | None = None
    columns: list[str] | None = None
    fallback_lookback_days: int = Field(default=14, ge=1, le=366)
    """If the latest-observation read fails, reduce this many days of history instead."""

    _clean = field_validator("asset_identifiers", "columns", mode="before")(_clean_strings)


class AssetDetailsRequest(_Paged):
    """Rows of the Valmer asset-details table."""

    asset_identifiers: list[str] | None = Field(default=None, max_length=MAX_IDENTIFIERS)
    """Valmer identifiers; ``None`` reads every row."""
    columns: list[str] | None = None
    """Detail columns besides ``asset_identifier``."""

    _clean = field_validator("asset_identifiers", "columns", mode="before")(_clean_strings)


class AssetMaturityRequest(_Request):
    """Static maturity fields, keyed by Valmer and Markets identifiers alike."""

    asset_identifiers: list[str] = Field(min_length=1, max_length=MAX_IDENTIFIERS)

    _clean = field_validator("asset_identifiers", mode="before")(_clean_strings)


class BondSnapshotRequest(_Paged):
    """Asset details joined with the vector on one valuation date."""

    valuation_date: dt.date | None = None
    security_types: list[str] | None = None
    issuers: list[str] | None = None
    asset_identifiers: list[str] | None = Field(default=None, max_length=MAX_IDENTIFIERS)
    detail_columns: list[str] | None = None
    vector_columns: list[str] | None = None
    require_yield: bool = False
    """Keep only rows with a ``yield_rate``."""
    require_future_maturity: bool = False
    """Keep only instruments maturing after the valuation date."""
    order_by: Literal["maturity", "identifier"] = "identifier"

    _clean = field_validator(
        "security_types",
        "issuers",
        "asset_identifiers",
        "detail_columns",
        "vector_columns",
        mode="before",
    )(_clean_strings)


class BondHistoryRequest(_Paged):
    """Asset details joined with the vector over a time range."""

    start_date: dt.datetime
    end_date: dt.datetime
    security_types: list[str] | None = None
    asset_identifiers: list[str] | None = Field(default=None, max_length=MAX_IDENTIFIERS)
    detail_columns: list[str] | None = None
    vector_columns: list[str] | None = None
    require_yield: bool = False

    _clean = field_validator(
        "security_types",
        "asset_identifiers",
        "detail_columns",
        "vector_columns",
        mode="before",
    )(_clean_strings)


class BondSearchRequest(_Request):
    """Server-side bond search, ordering and paging on one valuation date."""

    valuation_date: dt.date | None = None
    security_type: str | None = Field(default=None, max_length=64)
    issuer: str | None = Field(default=None, max_length=128)
    series: str | None = Field(default=None, max_length=128)
    query: str = Field(default="", max_length=120)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=25, ge=1, le=500)
    sort_by: BondSortField = "instrument"
    sort_direction: Literal["asc", "desc"] = "asc"


class BondComparisonUniverseRequest(_Request):
    """The Bond Comparison universe: details, vector and derived reference fields."""

    valuation_date: dt.date | None = None
    asset_identifiers: list[str] | None = None
    """A scope; ``None`` reads the market, an empty list matches nothing."""
    security_types: list[str] = Field(default_factory=list, max_length=40)
    issuers: list[str] = Field(default_factory=list, max_length=100)
    currencies: list[str] = Field(default_factory=list, max_length=30)
    sectors: list[str] = Field(default_factory=list, max_length=100)
    ratings: list[str] = Field(default_factory=list, max_length=100)
    rate_types: list[Literal["Fixed", "Floating"]] = Field(default_factory=list, max_length=2)
    reset_types: list[str] = Field(default_factory=list, max_length=100)
    min_days: int = Field(default=1, ge=0, le=36500)
    max_days: int = Field(default=12000, ge=1, le=36500)
    axis: Literal["yield", "spread"] = "yield"
    max_rows: int = Field(default=MAX_COMPARISON_ROWS, ge=1, le=MAX_COMPARISON_ROWS)

    @field_validator(
        "security_types",
        "issuers",
        "currencies",
        "sectors",
        "ratings",
        "rate_types",
        "reset_types",
        mode="before",
    )
    @classmethod
    def _normalize_options(cls, value: object) -> list[str]:
        return _clean_strings(value) or []

    _clean_scope = field_validator("asset_identifiers", mode="before")(_clean_strings)


class ValuationDatesResponse(BaseModel):
    """Vector valuation times, newest first."""

    valuation_dates: list[dt.datetime]


class BondFiltersResponse(BaseModel):
    """Cascading Bonds filter values for one valuation date."""

    valuation_dates: list[dt.date]
    selected_valuation_date: dt.date
    security_types: list[str]
    issuers: list[str]
    series: list[str]
    instrument_count: int


class BondInstrumentResponse(BaseModel):
    """Identity of one instrument, found by Valmer or Markets identifier."""

    asset_identifier: str
    security_type: str | None = None
    issuer: str | None = None
    series: str | None = None
    name: str | None = None
    found: bool


class BondComparisonFiltersResponse(BaseModel):
    """Bond Comparison dimensions for one valuation date."""

    valuation_dates: list[dt.date]
    selected_valuation_date: dt.date
    security_types: list[str]
    issuers: list[str]
    currencies: list[str]
    sectors: list[str]
    ratings: list[str]
    reset_types: list[str]
    instrument_count: int


class DatasetColumn(BaseModel):
    name: str
    dtype: str


class DatasetContract(BaseModel):
    """A published Valmer table that other pipelines may reference (ADR 0010, decision 4)."""

    key: str
    identifier: str
    uid: str | None = None
    """The registered MetaTable uid, or ``None`` when the table is not registered."""
    index_names: list[str]
    columns: list[DatasetColumn]


class DatasetsResponse(BaseModel):
    datasets: list[DatasetContract]


__all__ = [
    "MAX_COMPARISON_ROWS",
    "MAX_IDENTIFIERS",
    "MAX_PAGE_SIZE",
    "MAX_VALUATION_DATES",
    "AssetDetailsRequest",
    "AssetMaturityRequest",
    "BondComparisonFiltersResponse",
    "BondComparisonUniverseRequest",
    "BondFiltersResponse",
    "BondHistoryRequest",
    "BondInstrumentResponse",
    "BondSearchRequest",
    "BondSnapshotRequest",
    "BondSortField",
    "DatasetContract",
    "DatasetsResponse",
    "ValuationDatesResponse",
    "VectorHistoryRequest",
    "VectorLatestRequest",
    "VectorSnapshotRequest",
]
