"""Copy Valmer's curve and fixing history into Valmer's own instances (ms-markets ADR 0047).

Before ``VALMER_TIME_SERIES_STORAGE=valmer`` is set, Valmer's discount curves and
Banxico Index fixings live in the core Markets tables ``DiscountCurvesTS`` and
``IndexFixingsTS``. ``ValmerTimeSeriesCopyUpdater`` copies Valmer's rows (and
only Valmer's: the four Valmer curves, the seven Banxico fixings) from a core
table into the matching Valmer instance. It copies stored rows and never
recomputes them: the TIIE, SOFR and cross-currency curves were built from
same-day source files that cannot be replayed.

It copies every source row whose ``(time_index, identity)`` is missing in the
instance, so it can run again after the switch to fill the rows the core table
received in between. It writes through the normal time-index updater path, so
the instance's stored statistics are maintained and the production updaters
bound to the instance continue from its last observation.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

import pandas as pd
from msm.time_index.utils.stamped import StampedUpdater, StampedUpdaterConfiguration
from pydantic import Field

TimeSeriesName = Literal["discount_curves", "index_fixings"]
DEFAULT_COPY_PAGE_SIZE = 200
_READ_LIMITS = {"max_rows": 250_000, "statement_timeout_ms": 120_000}


@dataclass(frozen=True)
class TimeSeriesCopyPlan:
    """One Valmer time series: its core table, its instance and its identities."""

    series: TimeSeriesName
    source_table: type[Any]
    instance_table: type[Any]
    identity_column: str
    identities: tuple[str, ...]


def time_series_copy_plans(
    series: Iterable[str] | None = None,
) -> list[TimeSeriesCopyPlan]:
    """The copy plans for ``series`` (both by default), in a stable order."""

    from msm_pricing.time_index.curves.storage import DiscountCurvesStorage
    from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

    from valmer_connectors.banxico.settings import BANXICO_FIXING_INDEX_IDENTIFIERS
    from valmer_connectors.instruments.curve_bootstrap import VALMER_CURVE_DEFINITIONS
    from valmer_connectors.storage import (
        valmer_discount_curves_instance,
        valmer_index_fixings_instance,
    )

    plans = {
        "discount_curves": TimeSeriesCopyPlan(
            series="discount_curves",
            source_table=DiscountCurvesStorage,
            instance_table=valmer_discount_curves_instance(),
            identity_column="curve_identifier",
            identities=tuple(definition.unique_identifier for definition in VALMER_CURVE_DEFINITIONS),
        ),
        "index_fixings": TimeSeriesCopyPlan(
            series="index_fixings",
            source_table=IndexFixingsStorage,
            instance_table=valmer_index_fixings_instance(),
            identity_column="index_identifier",
            identities=tuple(BANXICO_FIXING_INDEX_IDENTIFIERS),
        ),
    }
    selected = list(plans) if series is None else list(dict.fromkeys(series))
    unknown = sorted(set(selected) - set(plans))
    if unknown:
        raise ValueError(f"Unknown Valmer time series {unknown!r}; choose from {sorted(plans)!r}.")
    return [plans[name] for name in selected]


class ValmerTimeSeriesCopyConfiguration(StampedUpdaterConfiguration):
    """Which Valmer series to copy and which of its identities."""

    series: TimeSeriesName = Field(..., description="The Valmer time series to copy.")
    identities: tuple[str, ...] = Field(
        ...,
        min_length=1,
        description="Valmer's identities in the core table (curves or fixing indexes).",
    )


class ValmerTimeSeriesCopyUpdater(StampedUpdater):
    """Copy the rows a Valmer instance is missing from the core Markets table."""

    configuration_class: ClassVar[type[ValmerTimeSeriesCopyConfiguration]] = (
        ValmerTimeSeriesCopyConfiguration
    )
    frame_label: ClassVar[str] = "Valmer time-series backfill"

    def __init__(
        self,
        config: ValmerTimeSeriesCopyConfiguration,
        output_table: type[Any],
    ) -> None:
        self.copy_config = config
        self.page_size = DEFAULT_COPY_PAGE_SIZE
        super().__init__(config=config, output_table=output_table)

    def set_page_size(self, page_size: int) -> ValmerTimeSeriesCopyUpdater:
        """Rows per source read; runtime state, not part of the updater's identity."""

        if page_size <= 0:
            raise ValueError("page_size must be positive.")
        self.page_size = page_size
        return self

    @property
    def plan(self) -> TimeSeriesCopyPlan:
        (plan,) = time_series_copy_plans([self.copy_config.series])
        return plan

    def update(self) -> pd.DataFrame:
        plan = self.plan
        frames: list[pd.DataFrame] = []
        for identity in self.copy_config.identities:
            missing = missing_time_indexes(
                source_table=plan.source_table,
                instance_table=self.output_table,
                identity_column=plan.identity_column,
                identity=identity,
            )
            for start in range(0, len(missing), self.page_size):
                rows = read_rows(
                    plan.source_table,
                    identity_column=plan.identity_column,
                    identity=identity,
                    time_indexes=missing[start : start + self.page_size],
                )
                if rows:
                    frames.append(pd.DataFrame(rows))
        if not frames:
            return pd.DataFrame()
        frame = pd.concat(frames, ignore_index=True)
        frame = _decoded_json_columns(frame, self.output_table)
        return self.validate_frame(frame, output_table=self.output_table)


def missing_time_indexes(
    *,
    source_table: type[Any],
    instance_table: type[Any],
    identity_column: str,
    identity: str,
) -> list[pd.Timestamp]:
    """The source table's time indexes for ``identity`` that the instance lacks."""

    source = set(_time_indexes(source_table, identity_column=identity_column, identity=identity))
    copied = set(_time_indexes(instance_table, identity_column=identity_column, identity=identity))
    return sorted(source - copied)


def copy_status(plan: TimeSeriesCopyPlan) -> dict[str, dict[str, Any]]:
    """Per identity: rows in the core table, rows in the instance, rows still missing."""

    status: dict[str, dict[str, Any]] = {}
    for identity in plan.identities:
        source = set(
            _time_indexes(plan.source_table, identity_column=plan.identity_column, identity=identity)
        )
        copied = set(
            _time_indexes(
                plan.instance_table, identity_column=plan.identity_column, identity=identity
            )
        )
        status[identity] = {
            "source_rows": len(source),
            "instance_rows": len(copied),
            "missing_rows": len(source - copied),
            "latest_source": _iso(max(source)) if source else None,
            "latest_instance": _iso(max(copied)) if copied else None,
        }
    return status


def read_rows(
    table: type[Any],
    *,
    identity_column: str,
    identity: str,
    time_indexes: Sequence[pd.Timestamp],
) -> list[dict[str, Any]]:
    """The full rows of ``table`` for ``identity`` at ``time_indexes``."""

    from sqlalchemy import select

    columns = table.__table__.c
    statement = (
        select(*columns)
        .where(columns[identity_column] == identity)
        .where(columns["time_index"].in_([value.to_pydatetime() for value in time_indexes]))
        .order_by(columns["time_index"])
    )
    return _select(statement)


def _time_indexes(
    table: type[Any],
    *,
    identity_column: str,
    identity: str,
) -> list[pd.Timestamp]:
    from sqlalchemy import select

    columns = table.__table__.c
    statement = select(columns["time_index"]).where(columns[identity_column] == identity)
    return [_utc_timestamp(row["time_index"]) for row in _select(statement)]


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _select(statement: Any) -> list[dict[str, Any]]:
    from msm.api.base import operation_result_rows
    from msm.repositories.base import (
        MarketsRepositoryContext,
        compile_markets_statement,
        execute_markets_operation,
    )

    context = MarketsRepositoryContext(limits=dict(_READ_LIMITS), timeout=180)
    operation = compile_markets_statement(statement, context=context, operation="select")
    result = execute_markets_operation(operation, context=context)
    if result.get("truncated"):
        raise RuntimeError(
            "A Valmer time-series backfill read exceeded "
            f"{_READ_LIMITS['max_rows']} rows; lower the page size."
        )
    return [dict(row) for row in operation_result_rows(result)]


def _decoded_json_columns(frame: pd.DataFrame, table: type[Any]) -> pd.DataFrame:
    """Turn JSON columns read back as text into Python values again."""

    from sqlalchemy import JSON

    working = frame.copy()
    for column in table.__table__.columns:
        if isinstance(column.type, JSON) and column.name in working.columns:
            working[column.name] = pd.Series(
                [_json_value(value) for value in working[column.name]],
                index=working.index,
                dtype=object,
            )
    return working


def _json_value(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _iso(value: pd.Timestamp) -> str:
    return value.isoformat()


__all__ = [
    "DEFAULT_COPY_PAGE_SIZE",
    "TimeSeriesCopyPlan",
    "ValmerTimeSeriesCopyConfiguration",
    "ValmerTimeSeriesCopyUpdater",
    "copy_status",
    "missing_time_indexes",
    "read_rows",
    "time_series_copy_plans",
]
