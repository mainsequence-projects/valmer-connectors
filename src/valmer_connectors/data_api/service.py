"""Read models of the Valmer data API (ADR 0010).

Each public method answers one route. The SQL that joins Valmer's tables lives
here, next to their schema; consumers keep their analytics. Markets registries
(Asset rows) are read through the Markets API with set-based row calls.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd
from msm.api.http import TabularFrameResponse, build_tabular_frame
from msm.settings import ASSET_IDENTIFIER_DIMENSION
from sqlalchemy import (
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Uuid,
    and_,
    asc,
    case,
    cast,
    desc,
    false,
    func,
    literal,
    or_,
    select,
    union_all,
)
from sqlalchemy.dialects.postgresql import distinct_on

from valmer_connectors.data_api.models import (
    MAX_VALUATION_DATES,
    AssetDetailsRequest,
    AssetMaturityRequest,
    BondComparisonFiltersResponse,
    BondComparisonUniverseRequest,
    BondFiltersResponse,
    BondHistoryRequest,
    BondInstrumentResponse,
    BondSearchRequest,
    BondSnapshotRequest,
    DatasetColumn,
    DatasetContract,
    DatasetsResponse,
    ValuationDatesResponse,
    VectorHistoryRequest,
    VectorLatestRequest,
    VectorSnapshotRequest,
)
from valmer_connectors.data_api.tables import (
    VECTOR_TABLE_IDENTIFIER,
    DataApiBadRequest,
    DataApiError,
    DataApiNotFound,
    ValmerTables,
)
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.queries.asset_details import (
    _DETAIL_PROJECTION,
    expand_valmer_asset_detail_alias_frame,
)
from valmer_connectors.queries.vector_quotes import (
    DEFAULT_VALMER_VECTOR_COLUMNS,
    normalize_valmer_quote_frame,
)

LOGGER = logging.getLogger(__name__)

VECTOR_INDEX_COLUMNS = ("time_index", ASSET_IDENTIFIER_DIMENSION)
DEFAULT_DETAIL_COLUMNS = ("security_type", "issuer", "series", "full_name", "maturity_date")
DEFAULT_VALUATION_DATES = 400
CACHE_SECONDS = 5 * 60
BOND_COMPARISON_MIN_DAYS = 1
BOND_COMPARISON_MAX_DAYS = 12000

_VECTOR_COLUMNS = frozenset(
    str(column.name) for column in ValmerVectorPricesStorage.__table__.columns
) - set(VECTOR_INDEX_COLUMNS)
_DETAIL_COLUMNS = frozenset(
    str(column.name) for column in ValmerAssetDetailsTable.__table__.columns
) - {"asset_uid", "valmer_unique_identifier"}

AssetLookup = Callable[..., list[tuple[str, str]]]
"""``lookup(unique_identifiers=..., uids=...)`` returns ``(uid, unique_identifier)`` pairs."""


def markets_assets(
    *,
    unique_identifiers: Sequence[str] = (),
    uids: Sequence[str] = (),
) -> list[tuple[str, str]]:
    """Read Asset rows by identifier or uid through the Markets API, one batch each."""

    from msm.api.assets import Asset

    found: list[Any] = []
    if unique_identifiers:
        found.extend(
            Asset.filter(
                unique_identifier__in=list(unique_identifiers),
                limit=len(unique_identifiers),
            )
        )
    if uids:
        found.extend(Asset.filter(uid__in=list(uids), limit=len(uids)))
    return [(str(asset.uid), str(asset.unique_identifier)) for asset in found]


class DataApiService:
    """Answers the data API's routes from the registered Valmer tables."""

    def __init__(
        self,
        tables: ValmerTables | None = None,
        *,
        asset_lookup: AssetLookup | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.tables = tables or ValmerTables()
        self._asset_lookup = asset_lookup or markets_assets
        self._clock = clock
        self._cache: dict[tuple[Any, ...], tuple[float, Any]] = {}
        self._cache_lock = threading.Lock()

    # Datasets and valuation dates

    def datasets(self) -> DatasetsResponse:
        try:
            uid = getattr(self.tables.bound().vector_meta_table, "uid", None)
        except DataApiError:
            uid = None
        storage = ValmerVectorPricesStorage
        return DatasetsResponse(
            datasets=[
                DatasetContract(
                    key="vector",
                    identifier=VECTOR_TABLE_IDENTIFIER,
                    uid=str(uid) if uid else None,
                    index_names=list(storage.__index_names__),
                    columns=[
                        DatasetColumn(name=str(column.name), dtype=_dtype(column.type))
                        for column in storage.__table__.columns
                    ],
                )
            ]
        )

    def valuation_dates(self, limit: int = DEFAULT_VALUATION_DATES) -> ValuationDatesResponse:
        return ValuationDatesResponse(valuation_dates=self.valuation_times(limit))

    def valuation_times(self, limit: int = DEFAULT_VALUATION_DATES) -> list[dt.datetime]:
        """Distinct vector observation times, newest first."""

        limit = max(1, min(int(limit), MAX_VALUATION_DATES))

        def load() -> list[dt.datetime]:
            vector = self.tables.bound().vector
            statement = (
                select(vector.c.time_index.label("time_index"))
                .distinct()
                .order_by(desc(vector.c.time_index))
                .limit(limit)
            )
            return [_utc(row["time_index"]) for row in self.tables.rows(statement, max_rows=limit)]

        return list(self._cached(("valuation_times", limit), load))

    def resolve_valuation_time(self, value: dt.date | None) -> dt.datetime:
        """The vector's observation time on ``value`` (UTC), or the latest one."""

        if value is None:
            times = self.valuation_times(1)
            if not times:
                raise DataApiNotFound("No Valmer vector valuation dates are available.")
            return times[0]

        def load() -> dt.datetime | None:
            vector = self.tables.bound().vector
            start = dt.datetime.combine(value, dt.time.min, tzinfo=dt.UTC)
            statement = select(func.max(vector.c.time_index).label("time_index")).where(
                vector.c.time_index >= start,
                vector.c.time_index < start + dt.timedelta(days=1),
            )
            rows = self.tables.rows(statement, max_rows=1)
            found = rows[0].get("time_index") if rows else None
            return None if found is None else _utc(found)

        resolved = self._cached(("valuation_time", value), load)
        if resolved is None:
            raise DataApiNotFound(f"No Valmer vector snapshot is available for {value.isoformat()}.")
        return resolved

    # Vector reads

    def vector_snapshot(self, request: VectorSnapshotRequest) -> TabularFrameResponse:
        valuation_time = self.resolve_valuation_time(request.valuation_date)
        columns = _vector_columns(request.columns)
        frame_columns = [*VECTOR_INDEX_COLUMNS, *columns]
        meta = {"valuation_time": valuation_time.isoformat()}
        if request.asset_identifiers is not None and not request.asset_identifiers:
            return _frame([], frame_columns, meta=meta)
        vector = self.tables.bound().vector
        statement = select(
            vector.c.time_index,
            vector.c.asset_identifier,
            *(vector.c[column] for column in columns),
        ).where(vector.c.time_index == valuation_time)
        if request.asset_identifiers:
            statement = statement.where(vector.c.asset_identifier.in_(request.asset_identifiers))
        statement = (
            statement.order_by(vector.c.asset_identifier)
            .offset(request.offset)
            .limit(request.limit)
        )
        rows = self.tables.rows(statement, max_rows=request.limit)
        return _frame(rows, frame_columns, meta=meta)

    def vector_history(self, request: VectorHistoryRequest) -> TabularFrameResponse:
        columns = _vector_columns(request.columns)
        vector = self.tables.bound().vector
        filters = [vector.c.asset_identifier.in_(request.asset_identifiers)]
        if request.start_date is not None:
            filters.append(vector.c.time_index >= _utc(request.start_date))
        if request.end_date is not None:
            filters.append(vector.c.time_index <= _utc(request.end_date))
        statement = (
            select(
                vector.c.time_index,
                vector.c.asset_identifier,
                *(vector.c[column] for column in columns),
            )
            .where(*filters)
            .order_by(vector.c.time_index, vector.c.asset_identifier)
            .offset(request.offset)
            .limit(request.limit)
        )
        rows = self.tables.rows(statement, max_rows=request.limit)
        return _frame(rows, [*VECTOR_INDEX_COLUMNS, *columns])

    def vector_latest(self, request: VectorLatestRequest) -> TabularFrameResponse:
        """The latest row per identifier, with a bounded history fallback.

        The primary read is the vector's last observation per identifier. If it
        fails, the API reduces ``fallback_lookback_days`` of history to the latest
        row per identifier instead. Rows come back in request order.
        """

        columns = _vector_columns(request.columns)
        frame_columns = [*VECTOR_INDEX_COLUMNS, *columns]
        as_of = _utc(request.as_of) if request.as_of is not None else dt.datetime.now(dt.UTC)
        lookback_days = request.fallback_lookback_days
        try:
            records = self._last_observations(request.asset_identifiers, as_of, frame_columns)
            source = "latest"
        except Exception as primary_error:
            LOGGER.warning(
                "Latest Valmer observation read failed; using the bounded %s-day history.",
                lookback_days,
                exc_info=True,
            )
            try:
                records = self._bounded_latest(
                    request.asset_identifiers,
                    as_of,
                    columns,
                    lookback_days=lookback_days,
                )
            except Exception as fallback_error:
                raise DataApiError(
                    "The latest Valmer observation read failed and the bounded "
                    f"{lookback_days}-day history fallback failed. Primary error: "
                    f"{primary_error}. Fallback error: {fallback_error}."
                ) from fallback_error
            source = "history_fallback"
        position = {identifier: index for index, identifier in enumerate(request.asset_identifiers)}
        records = sorted(
            records,
            key=lambda row: position.get(str(row.get("asset_identifier")), len(position)),
        )
        return _frame(records, frame_columns, meta={"as_of": as_of.isoformat(), "source": source})

    def _last_observations(
        self,
        identifiers: Sequence[str],
        as_of: dt.datetime,
        frame_columns: Sequence[str],
    ) -> list[dict[str, Any]]:
        frame = self.tables.vector_ref().get_last_observation(
            dimension_range_map=[
                {"coordinate": {ASSET_IDENTIFIER_DIMENSION: identifier}, "end_date": as_of}
                for identifier in identifiers
            ]
        )
        frame = normalize_valmer_quote_frame(frame)
        if frame.empty:
            return []
        return _records(frame.loc[:, [column for column in frame_columns if column in frame.columns]])

    def _bounded_latest(
        self,
        identifiers: Sequence[str],
        as_of: dt.datetime,
        columns: Sequence[str],
        *,
        lookback_days: int,
    ) -> list[dict[str, Any]]:
        vector = self.tables.bound().vector
        statement = (
            select(
                vector.c.time_index,
                vector.c.asset_identifier,
                *(vector.c[column] for column in columns),
            )
            .where(
                vector.c.asset_identifier.in_(list(identifiers)),
                vector.c.time_index >= as_of - dt.timedelta(days=lookback_days),
                vector.c.time_index <= as_of,
            )
            .ext(distinct_on(vector.c.asset_identifier))
            .order_by(vector.c.asset_identifier, desc(vector.c.time_index))
        )
        return self.tables.rows(statement, max_rows=len(identifiers))

    # Asset details

    def asset_details(self, request: AssetDetailsRequest) -> TabularFrameResponse:
        columns = _detail_columns(request.columns, default=DEFAULT_DETAIL_COLUMNS)
        frame_columns = ["asset_identifier", "asset_uid", *columns]
        if request.asset_identifiers is not None and not request.asset_identifiers:
            return _frame([], frame_columns)
        details = self.tables.bound().details
        statement = select(
            details.c.valmer_unique_identifier.label("asset_identifier"),
            details.c.asset_uid.label("asset_uid"),
            *(details.c[column] for column in columns),
        )
        if request.asset_identifiers:
            statement = statement.where(
                details.c.valmer_unique_identifier.in_(request.asset_identifiers)
            )
        statement = (
            statement.order_by(details.c.valmer_unique_identifier)
            .offset(request.offset)
            .limit(request.limit)
        )
        return _frame(self.tables.rows(statement, max_rows=request.limit), frame_columns)

    def asset_maturity(self, request: AssetMaturityRequest) -> TabularFrameResponse:
        """Static maturity fields keyed by Valmer and Markets identifiers alike.

        Assets named by their Markets identifier, which may differ from the Valmer
        one, are found through the Markets API and merged here instead of joining
        ``AssetTable``.
        """

        identifiers = list(request.asset_identifiers)
        frame_columns = [
            "asset_uid",
            "asset_table_identifier",
            *dict.fromkeys(label for label, _column in _DETAIL_PROJECTION),
        ]
        assets = dict(self._asset_lookup(unique_identifiers=identifiers))
        details = self.tables.bound().details
        condition = details.c.valmer_unique_identifier.in_(identifiers)
        if assets:
            condition = or_(condition, details.c.asset_uid.in_([_uuid(uid) for uid in assets]))
        statement = (
            select(
                details.c.asset_uid.label("detail_asset_uid"),
                *(details.c[column].label(label) for label, column in _DETAIL_PROJECTION),
            )
            .where(condition)
            .order_by(details.c.valmer_unique_identifier)
            .limit(len(identifiers))
        )
        detail_rows = self.tables.rows(statement, max_rows=len(identifiers))
        missing = sorted(
            {str(row["detail_asset_uid"]) for row in detail_rows if row.get("detail_asset_uid")}
            - set(assets)
        )
        if missing:
            assets.update(dict(self._asset_lookup(uids=missing)))
        rows: list[dict[str, Any]] = []
        for row in detail_rows:
            asset_uid = str(row.pop("detail_asset_uid", None) or "")
            rows.append(
                {
                    "asset_uid": asset_uid if asset_uid in assets else None,
                    "asset_table_identifier": assets.get(asset_uid),
                    **row,
                }
            )
        frame = expand_valmer_asset_detail_alias_frame(rows)
        return _frame(_records(frame) if not frame.empty else [], frame_columns)

    # Bonds: details joined with the vector

    def bond_snapshot(self, request: BondSnapshotRequest) -> TabularFrameResponse:
        valuation_time = self.resolve_valuation_time(request.valuation_date)
        detail_columns = _detail_columns(request.detail_columns, default=DEFAULT_DETAIL_COLUMNS)
        vector_columns = _vector_columns(request.vector_columns)
        frame_columns = ["asset_uid", "asset_identifier", *detail_columns, *vector_columns, "time_index"]
        meta = {"valuation_time": valuation_time.isoformat()}
        if request.asset_identifiers is not None and not request.asset_identifiers:
            return _frame([], frame_columns, meta=meta)
        tables = self.tables.bound()
        details, vector = tables.details, tables.vector
        filters = [vector.c.time_index == valuation_time]
        filters.extend(
            _bond_scope_filters(
                details,
                security_types=request.security_types,
                issuers=request.issuers,
                asset_identifiers=request.asset_identifiers,
            )
        )
        if request.require_yield:
            filters.append(vector.c.yield_rate.is_not(None))
        if request.require_future_maturity:
            filters.append(details.c.maturity_date > valuation_time.date())
        order = (
            (asc(details.c.maturity_date), asc(details.c.valmer_unique_identifier))
            if request.order_by == "maturity"
            else (asc(details.c.valmer_unique_identifier),)
        )
        statement = (
            _bond_select(details, vector, detail_columns, vector_columns)
            .where(*filters)
            .order_by(*order)
            .offset(request.offset)
            .limit(request.limit)
        )
        return _frame(
            self.tables.rows(statement, max_rows=request.limit),
            frame_columns,
            meta=meta,
        )

    def bond_history(self, request: BondHistoryRequest) -> TabularFrameResponse:
        detail_columns = _detail_columns(request.detail_columns, default=DEFAULT_DETAIL_COLUMNS)
        vector_columns = _vector_columns(request.vector_columns)
        frame_columns = ["asset_uid", "asset_identifier", *detail_columns, *vector_columns, "time_index"]
        if request.asset_identifiers is not None and not request.asset_identifiers:
            return _frame([], frame_columns)
        tables = self.tables.bound()
        details, vector = tables.details, tables.vector
        filters = [
            vector.c.time_index >= _utc(request.start_date),
            vector.c.time_index <= _utc(request.end_date),
            *_bond_scope_filters(
                details,
                security_types=request.security_types,
                asset_identifiers=request.asset_identifiers,
            ),
        ]
        if request.require_yield:
            filters.append(vector.c.yield_rate.is_not(None))
        statement = (
            _bond_select(details, vector, detail_columns, vector_columns)
            .where(*filters)
            .order_by(asc(vector.c.time_index), asc(details.c.valmer_unique_identifier))
            .offset(request.offset)
            .limit(request.limit)
        )
        return _frame(self.tables.rows(statement, max_rows=request.limit), frame_columns)

    def bond_filters(
        self,
        *,
        valuation_date: dt.date | None,
        security_type: str | None,
        issuer: str | None,
    ) -> BondFiltersResponse:
        """Cascading values for the Bonds controls."""

        security_type = _strip(security_type)
        issuer = _strip(issuer)
        selected_time = self.resolve_valuation_time(valuation_date)
        details = self.tables.bound().details
        security_rows = self._grouped_option_counts(details.c.security_type)
        issuer_rows = (
            self._grouped_option_counts(details.c.issuer, security_type=security_type)
            if security_type
            else []
        )
        series_rows = (
            self._grouped_option_counts(
                details.c.series,
                security_type=security_type,
                issuer=issuer,
            )
            if security_type and issuer
            else []
        )
        if security_type and issuer:
            active_rows = series_rows
        elif security_type:
            active_rows = issuer_rows
        else:
            active_rows = security_rows
        return BondFiltersResponse(
            valuation_dates=[value.date() for value in self.valuation_times()],
            selected_valuation_date=selected_time.date(),
            security_types=_option_values(security_rows),
            issuers=_option_values(issuer_rows),
            series=_option_values(series_rows),
            instrument_count=sum(int(row.get("row_count") or 0) for row in active_rows),
        )

    def _grouped_option_counts(
        self,
        column: Any,
        *,
        security_type: str | None = None,
        issuer: str | None = None,
    ) -> list[dict[str, Any]]:
        def load() -> list[dict[str, Any]]:
            details = self.tables.bound().details
            statement = (
                select(column.label("value"), func.count().label("row_count"))
                .select_from(details)
                .where(*_equal_filters(details, security_type=security_type, issuer=issuer))
                .group_by(column)
                .order_by(column)
            )
            return self.tables.rows(statement, max_rows=50_000)

        return list(self._cached(("option_counts", str(column.name), security_type, issuer), load))

    def bond_search(self, request: BondSearchRequest) -> TabularFrameResponse:
        """Server-side bond search, sorting and paging on one valuation date."""

        selected_time = self.resolve_valuation_time(request.valuation_date)
        tables = self.tables.bound()
        details, vector = tables.details, tables.vector
        joined = _bond_join(details, vector, selected_time)
        filters = _equal_filters(
            details,
            security_type=_strip(request.security_type),
            issuer=_strip(request.issuer),
            series=_strip(request.series),
        )
        query = request.query.strip()
        if query:
            pattern = f"%{_escape_like(query)}%"
            filters.append(
                or_(
                    details.c.valmer_unique_identifier.ilike(pattern, escape="/"),
                    details.c.security_type.ilike(pattern, escape="/"),
                    details.c.issuer.ilike(pattern, escape="/"),
                    details.c.series.ilike(pattern, escape="/"),
                    details.c.full_name.ilike(pattern, escape="/"),
                )
            )
        sort_columns = {
            "instrument": details.c.valmer_unique_identifier,
            "issuer": details.c.issuer,
            "series": details.c.series,
            "name": details.c.full_name,
            "rating": _rating_expression(vector),
            "spread": vector.c.spread,
            "yield_rate": vector.c.yield_rate,
            "clean_price": vector.c.clean_price,
            "dirty_price": vector.c.dirty_price,
            "duration": vector.c.duration,
            "maturity_date": details.c.maturity_date,
            "liquidity": vector.c.liquidity,
        }
        direction = asc if request.sort_direction == "asc" else desc
        order = direction(sort_columns[request.sort_by]).nullslast()
        offset = (request.page - 1) * request.page_size
        statement = (
            select(
                details.c.asset_uid.label("asset_uid"),
                details.c.valmer_unique_identifier.label("asset_identifier"),
                details.c.security_type.label("security_type"),
                details.c.issuer.label("issuer"),
                details.c.series.label("series"),
                details.c.full_name.label("full_name"),
                vector.c.hr_rating.label("hr_rating"),
                vector.c.fitch_rating.label("fitch_rating"),
                vector.c.sp_rating.label("sp_rating"),
                vector.c.moodys_rating.label("moodys_rating"),
                vector.c.spread.label("spread"),
                vector.c.yield_rate.label("yield_rate"),
                vector.c.clean_price.label("clean_price"),
                vector.c.dirty_price.label("dirty_price"),
                vector.c.duration.label("duration"),
                details.c.maturity_date.label("maturity_date"),
                vector.c.liquidity.label("liquidity"),
                vector.c.time_index.label("valuation_date"),
                func.count().over().label("total_rows"),
            )
            .select_from(joined)
            .where(*filters)
            .order_by(order, asc(details.c.valmer_unique_identifier))
            .offset(offset)
            .limit(request.page_size)
        )
        rows = self.tables.rows(statement, max_rows=request.page_size)
        total_rows = int(rows[0].get("total_rows") or 0) if rows else 0
        if not rows and request.page > 1:
            counted = self.tables.rows(
                select(func.count().label("total_rows")).select_from(joined).where(*filters),
                max_rows=1,
            )
            total_rows = int(counted[0].get("total_rows") or 0) if counted else 0
        for row in rows:
            row.pop("total_rows", None)
        return _frame(
            rows,
            [
                "asset_uid",
                "asset_identifier",
                "security_type",
                "issuer",
                "series",
                "full_name",
                "hr_rating",
                "fitch_rating",
                "sp_rating",
                "moodys_rating",
                "spread",
                "yield_rate",
                "clean_price",
                "dirty_price",
                "duration",
                "maturity_date",
                "liquidity",
                "valuation_date",
            ],
            meta={"valuation_time": selected_time.isoformat(), "total_rows": total_rows},
        )

    def bond_instrument(self, asset_identifier: str) -> BondInstrumentResponse:
        """Identity of one instrument, found by Valmer or Markets identifier."""

        identifier = asset_identifier.strip()
        details = self.tables.bound().details
        projection = (
            details.c.valmer_unique_identifier.label("asset_identifier"),
            details.c.security_type.label("security_type"),
            details.c.issuer.label("issuer"),
            details.c.series.label("series"),
            details.c.full_name.label("name"),
        )
        rows = self.tables.rows(
            select(*projection).where(details.c.valmer_unique_identifier == identifier).limit(1),
            max_rows=1,
        )
        if not rows:
            asset_uids = [uid for uid, _unique in self._asset_lookup(unique_identifiers=[identifier])]
            if asset_uids:
                rows = self.tables.rows(
                    select(*projection)
                    .where(details.c.asset_uid.in_([_uuid(uid) for uid in asset_uids]))
                    .limit(1),
                    max_rows=1,
                )
        if not rows:
            return BondInstrumentResponse(asset_identifier=identifier, found=False)
        return BondInstrumentResponse(**rows[0], found=True)

    # Bond Comparison

    def bond_comparison_filters(
        self,
        *,
        valuation_date: dt.date | None,
        security_type: str | None,
    ) -> BondComparisonFiltersResponse:
        """Bond Comparison dimensions without transferring the catalog."""

        selected_time = self.resolve_valuation_time(valuation_date)
        security_type = _strip(security_type)

        def load() -> BondComparisonFiltersResponse:
            tables = self.tables.bound()
            details, vector = tables.details, tables.vector
            joined = _bond_join(details, vector, selected_time)
            scoped_filters: list[Any] = [vector.c.asset_identifier.is_not(None)]
            if security_type:
                scoped_filters.append(details.c.security_type == security_type)
            option_statements = [
                _option_statement(details, "security_type", details.c.security_type),
                _option_statement(
                    details, "issuer", details.c.issuer, from_clause=joined, filters=scoped_filters
                ),
                _option_statement(
                    details,
                    "currency",
                    details.c.issue_currency,
                    from_clause=joined,
                    filters=scoped_filters,
                ),
                _option_statement(
                    details, "sector", details.c.sector, from_clause=joined, filters=scoped_filters
                ),
                _option_statement(
                    details,
                    "rating",
                    _rating_expression(vector),
                    from_clause=joined,
                    filters=scoped_filters,
                ),
                _option_statement(
                    details,
                    "reset_type",
                    _reset_type_expression(details),
                    from_clause=joined,
                    filters=scoped_filters,
                ),
                select(
                    literal("instrument_count").label("dimension"),
                    cast(func.count(vector.c.asset_identifier), String).label("value"),
                )
                .select_from(joined)
                .where(*scoped_filters),
            ]
            rows = self.tables.rows(union_all(*option_statements), max_rows=50_000)
            return BondComparisonFiltersResponse(
                valuation_dates=[value.date() for value in self.valuation_times()],
                selected_valuation_date=selected_time.date(),
                security_types=_dimension_values(rows, "security_type"),
                issuers=_dimension_values(rows, "issuer"),
                currencies=_dimension_values(rows, "currency"),
                sectors=_dimension_values(rows, "sector"),
                ratings=_dimension_values(rows, "rating"),
                reset_types=_dimension_values(rows, "reset_type"),
                instrument_count=_dimension_integer(rows, "instrument_count"),
            )

        return self._cached(("comparison_filters", selected_time, security_type), load)

    def bond_comparison_universe(
        self,
        request: BondComparisonUniverseRequest,
    ) -> TabularFrameResponse:
        """One filtered Bond Comparison universe with its derived reference fields."""

        selected_time = self.resolve_valuation_time(request.valuation_date)
        tables = self.tables.bound()
        details, vector = tables.details, tables.vector
        reference = _reference_m_bond_lateral(details, vector, selected_time)
        previous_quote = _previous_quote_lateral(details, vector, selected_time)
        spread_history = _spread_history_lateral(details, vector, selected_time)
        joined = (
            _bond_join(details, vector, selected_time)
            .outerjoin(reference, literal(True))
            .outerjoin(previous_quote, literal(True))
            .outerjoin(spread_history, literal(True))
        )
        rating_expression = _rating_expression(vector)
        rate_type_expression = _rate_type_expression(details)
        reset_type_expression = _reset_type_expression(details)
        filters: list[Any] = [
            details.c.maturity_date >= selected_time.date() + dt.timedelta(days=request.min_days),
            details.c.maturity_date <= selected_time.date() + dt.timedelta(days=request.max_days),
        ]
        for column, values in (
            (details.c.security_type, request.security_types),
            (details.c.issuer, request.issuers),
            (details.c.issue_currency, request.currencies),
            (details.c.sector, request.sectors),
            (rating_expression, request.ratings),
            (rate_type_expression, request.rate_types),
            (reset_type_expression, request.reset_types),
        ):
            if values:
                filters.append(column.in_(values))
        if request.asset_identifiers is not None:
            filters.append(
                details.c.valmer_unique_identifier.in_(request.asset_identifiers)
                if request.asset_identifiers
                else false()
            )
        filters.append(vector.c.asset_identifier.is_not(None))
        metric_expression = vector.c.yield_rate if request.axis == "yield" else vector.c.spread
        projection = [
            details.c.asset_uid.label("asset_uid"),
            details.c.valmer_unique_identifier.label("asset_identifier"),
            *(
                details.c[column].label(column)
                for column in (
                    "security_type",
                    "issuer",
                    "series",
                    "full_name",
                    "sector",
                    "issued_amount",
                    "issue_date",
                    "issue_term",
                    "maturity_date",
                    "face_value",
                    "issue_currency",
                    "underlying",
                    "placement_yield",
                    "placement_spread",
                    "coupon_frequency",
                    "coupon_rate",
                    "coupon_rule",
                    "coupons_at_issue",
                )
            ),
            reference.c.reference_m_bond_identifier,
            reference.c.reference_m_bond_series,
            reference.c.reference_m_bond_yield,
            previous_quote.c.previous_valuation_date,
            previous_quote.c.previous_yield_rate,
            previous_quote.c.previous_spread,
            spread_history.c.spread_mean_1y,
            spread_history.c.spread_std_1y,
            spread_history.c.spread_observations_1y,
            *(vector.c[column].label(column) for column in _COMPARISON_VECTOR_COLUMNS),
            vector.c.time_index.label("valuation_date"),
            func.count().over().label("total_rows"),
        ]
        statement = (
            select(*projection)
            .select_from(joined)
            .where(*filters)
            .order_by(desc(metric_expression).nullslast(), details.c.valmer_unique_identifier)
            .limit(request.max_rows)
        )
        rows = self.tables.rows(statement, max_rows=request.max_rows)
        total_rows = int(rows[0].get("total_rows") or 0) if rows else 0
        for row in rows:
            row.pop("total_rows", None)
        columns = [str(column.name) for column in projection if str(column.name) != "total_rows"]
        return _frame(
            rows,
            columns,
            meta={"valuation_time": selected_time.isoformat(), "total_rows": total_rows},
        )

    # Cache

    def _cached(self, key: tuple[Any, ...], load: Callable[[], Any]) -> Any:
        now = self._clock()
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None and cached[0] > now:
                return cached[1]
        value = load()
        with self._cache_lock:
            self._cache[key] = (now + CACHE_SECONDS, value)
            for expired in [item for item, (expiry, _) in self._cache.items() if expiry <= now]:
                self._cache.pop(expired, None)
        return value


_COMPARISON_VECTOR_COLUMNS = (
    "hr_rating",
    "fitch_rating",
    "sp_rating",
    "moodys_rating",
    "yield_rate",
    "spread",
    "bid_yield",
    "ask_yield",
    "bid_spread",
    "ask_spread",
    "posted_bid",
    "posted_ask",
    "clean_price",
    "dirty_price",
    "theoretical_price",
    "accrued_interest",
    "current_coupon",
    "amount_outstanding",
    "days_since_coupon",
    "coupons_remaining",
    "daily_change_pct",
    "weekly_change_pct",
    "max_price_12m",
    "min_price_12m",
    "max_price_date",
    "min_price_date",
    "duration",
    "macaulay_duration",
    "monetary_duration",
    "convexity",
    "sensitivity",
    "value_at_risk",
    "standard_deviation",
    "volatility",
    "volatility_secondary",
    "liquidity",
    "marketability",
    "market_event",
    "suspension_status",
    "adjusted_face_value",
    "uh_date",
)


def _bond_select(
    details: Any,
    vector: Any,
    detail_columns: Sequence[str],
    vector_columns: Sequence[str],
) -> Any:
    return select(
        details.c.asset_uid.label("asset_uid"),
        details.c.valmer_unique_identifier.label("asset_identifier"),
        *(details.c[column] for column in detail_columns),
        *(vector.c[column] for column in vector_columns),
        vector.c.time_index.label("time_index"),
    ).select_from(
        details.join(vector, details.c.valmer_unique_identifier == vector.c.asset_identifier)
    )


def _bond_join(details: Any, vector: Any, selected_time: dt.datetime) -> Any:
    """Every detail row with its vector row on ``selected_time``, when it has one."""

    return details.outerjoin(
        vector,
        and_(
            details.c.valmer_unique_identifier == vector.c.asset_identifier,
            vector.c.time_index == selected_time,
        ),
    )


def _bond_scope_filters(
    details: Any,
    *,
    security_types: Sequence[str] | None = None,
    issuers: Sequence[str] | None = None,
    asset_identifiers: Sequence[str] | None = None,
) -> list[Any]:
    filters: list[Any] = []
    if security_types:
        filters.append(details.c.security_type.in_(list(security_types)))
    if issuers:
        filters.append(details.c.issuer.in_(list(issuers)))
    if asset_identifiers:
        filters.append(details.c.valmer_unique_identifier.in_(list(asset_identifiers)))
    return filters


def _equal_filters(details: Any, **values: str | None) -> list[Any]:
    return [details.c[column] == value for column, value in values.items() if value]


def _reference_m_bond_lateral(details: Any, vector: Any, selected_time: dt.datetime) -> Any:
    reference_details = details.alias("reference_details")
    reference_vector = vector.alias("reference_vector")
    return (
        select(
            reference_details.c.valmer_unique_identifier.label("reference_m_bond_identifier"),
            reference_details.c.series.label("reference_m_bond_series"),
            reference_vector.c.yield_rate.label("reference_m_bond_yield"),
        )
        .select_from(
            reference_details.join(
                reference_vector,
                and_(
                    reference_details.c.valmer_unique_identifier
                    == reference_vector.c.asset_identifier,
                    reference_vector.c.time_index == selected_time,
                ),
            )
        )
        .where(
            reference_details.c.security_type == "M",
            reference_details.c.issuer == "BONOS",
            reference_details.c.maturity_date >= selected_time.date(),
        )
        .order_by(func.abs(reference_details.c.maturity_date - details.c.maturity_date))
        .limit(1)
        .lateral("reference_m_bond")
    )


def _previous_quote_lateral(details: Any, vector: Any, selected_time: dt.datetime) -> Any:
    previous_vector = vector.alias("previous_vector")
    return (
        select(
            previous_vector.c.time_index.label("previous_valuation_date"),
            previous_vector.c.yield_rate.label("previous_yield_rate"),
            previous_vector.c.spread.label("previous_spread"),
        )
        .where(
            previous_vector.c.asset_identifier == details.c.valmer_unique_identifier,
            previous_vector.c.time_index < selected_time,
        )
        .order_by(previous_vector.c.time_index.desc())
        .limit(1)
        .lateral("previous_quote")
    )


def _spread_history_lateral(details: Any, vector: Any, selected_time: dt.datetime) -> Any:
    history_vector = vector.alias("spread_history_vector")
    history_start = selected_time - dt.timedelta(days=365)
    return (
        select(
            func.avg(history_vector.c.spread).label("spread_mean_1y"),
            func.stddev_pop(history_vector.c.spread).label("spread_std_1y"),
            func.count(history_vector.c.spread).label("spread_observations_1y"),
        )
        .where(
            history_vector.c.asset_identifier == details.c.valmer_unique_identifier,
            history_vector.c.time_index >= history_start,
            history_vector.c.time_index <= selected_time,
            history_vector.c.spread.is_not(None),
        )
        .lateral("spread_history")
    )


def _option_statement(
    details: Any,
    dimension: str,
    value_expression: Any,
    *,
    from_clause: Any | None = None,
    filters: list[Any] | None = None,
) -> Any:
    statement = select(
        literal(dimension).label("dimension"),
        cast(value_expression, String).label("value"),
    ).select_from(from_clause if from_clause is not None else details)
    conditions = list(filters or [])
    conditions.extend([value_expression.is_not(None), cast(value_expression, String) != ""])
    return statement.where(*conditions).group_by(value_expression)


def _rating_expression(vector: Any) -> Any:
    return func.coalesce(
        func.nullif(vector.c.hr_rating, ""),
        func.nullif(vector.c.fitch_rating, ""),
        func.nullif(vector.c.sp_rating, ""),
        func.nullif(vector.c.moodys_rating, ""),
    )


def _rate_type_expression(details: Any) -> Any:
    """Classify Mexican bonds as Fixed or Floating from Valmer's descriptors."""

    normalized_coupon_rule = func.upper(func.coalesce(details.c.coupon_rule, ""))
    normalized_underlying = func.upper(func.coalesce(details.c.underlying, ""))
    fixed_condition = or_(
        details.c.security_type.in_(["BI", "M"]),
        details.c.issuer.in_(["BONOS", "CETES"]),
        normalized_coupon_rule.like("%TASA FIJA%"),
    )
    floating_condition = or_(
        details.c.issuer.in_(["BONDESD", "BPAG28", "BPAG91", "BPA182"]),
        func.upper(func.coalesce(details.c.issuer, "")).like("BPAG%"),
        *[
            (normalized_coupon_rule + literal(" ") + normalized_underlying).like(f"%{marker}%")
            for marker in ("CETE", "FONDEO", "IRMXP", "TIIE")
        ],
    )
    return case(
        (fixed_condition, literal("Fixed")),
        (floating_condition, literal("Floating")),
        else_=literal("Fixed"),
    )


def _reset_type_expression(details: Any) -> Any:
    rate_type = _rate_type_expression(details)
    return case(
        (rate_type == "Fixed", literal("Fixed Rate")),
        else_=func.coalesce(
            func.nullif(details.c.underlying, ""),
            func.nullif(details.c.coupon_rule, ""),
            literal("Unspecified"),
        ),
    )


def _option_values(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    return [
        str(row["value"]).strip()
        for row in rows
        if row.get("value") is not None and str(row["value"]).strip()
    ]


def _dimension_values(rows: Iterable[Mapping[str, Any]], dimension: str) -> list[str]:
    return sorted(
        {
            str(row["value"]).strip()
            for row in rows
            if row.get("dimension") == dimension
            and row.get("value") is not None
            and str(row["value"]).strip()
        }
    )


def _dimension_integer(rows: Iterable[Mapping[str, Any]], dimension: str) -> int:
    value = next((row.get("value") for row in rows if row.get("dimension") == dimension), 0)
    return int(value or 0)


def _vector_columns(
    columns: Sequence[str] | None,
    *,
    default: Sequence[str] = DEFAULT_VALMER_VECTOR_COLUMNS,
) -> list[str]:
    return _known_columns(columns, default, allowed=_VECTOR_COLUMNS, kind="vector")


def _detail_columns(columns: Sequence[str] | None, *, default: Sequence[str]) -> list[str]:
    return _known_columns(columns, default, allowed=_DETAIL_COLUMNS, kind="asset-detail")


def _known_columns(
    columns: Sequence[str] | None,
    default: Sequence[str],
    *,
    allowed: frozenset[str],
    kind: str,
) -> list[str]:
    requested = [
        column
        for column in dict.fromkeys(columns if columns else default)
        if column not in {*VECTOR_INDEX_COLUMNS, "asset_uid", "valmer_unique_identifier"}
    ]
    unknown = sorted(set(requested) - allowed)
    if unknown:
        raise DataApiBadRequest(f"Unknown {kind} columns: {', '.join(unknown)}.")
    return requested


def _frame(
    rows: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
    *,
    meta: Mapping[str, Any] | None = None,
) -> TabularFrameResponse:
    return build_tabular_frame(
        rows=[{column: _json_value(row.get(column)) for column in columns} for row in rows],
        columns=list(columns),
        meta=dict(meta) if meta else None,
    )


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {str(key): _json_value(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.to_pydatetime()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and value != value:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _utc(value: Any) -> dt.datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def _uuid(value: Any) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _strip(value: str | None) -> str | None:
    text = value.strip() if value else ""
    return text or None


def _escape_like(value: str) -> str:
    return value.replace("/", "//").replace("%", "/%").replace("_", "/_")


def _dtype(column_type: Any) -> str:
    for sql_type, name in (
        (DateTime, "datetime"),
        (Date, "date"),
        (Float, "float"),
        (Integer, "integer"),
        (Uuid, "uuid"),
        (String, "string"),
    ):
        if isinstance(column_type, sql_type):
            return name
    return str(column_type).lower()


__all__ = ["DataApiService", "markets_assets"]
