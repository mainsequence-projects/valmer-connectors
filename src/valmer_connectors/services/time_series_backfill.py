"""Move Valmer's curves and fixings into Valmer's own instances, and back.

The runbook is docs/time-series-instances.md. Valmer chooses which tables and
identities move (``time_series_copy_plans``); ms-markets' shared copy
(``msm.storage_copies``) does the moving: it reads both tables through
MetaTables, writes only the rows of those identities that the target lacks, with
empty update statistics so a second pass fills the gaps, and counts both sides
again. It never recomputes a row: the TIIE, SOFR and cross-currency curves were
built from same-day source files that cannot be replayed.

``backfill_valmer_time_series`` runs the moves (the ``time-series-backfill`` Job
and ``valmer-connectors time-series backfill``); with ``rollback=True`` it copies
the rows the instances gained back into the core tables. Which tables Valmer's
processes use stays ``VALMER_TIME_SERIES_STORAGE``, so the moves carry no
callbacks. ``valmer_series_moves`` is the ``module:callable`` for
``msm series move|rollback``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal

import structlog

LOGGER = structlog.get_logger(__name__)

TimeSeriesName = Literal["discount_curves", "index_fixings"]

__all__ = [
    "TimeSeriesCopyPlan",
    "backfill_valmer_time_series",
    "time_series_copy_plans",
    "valmer_series_moves",
]


@dataclass(frozen=True)
class TimeSeriesCopyPlan:
    """One Valmer time series: its core table, its instance and Valmer's identities in it."""

    series: TimeSeriesName
    source_table: type[Any]
    instance_table: type[Any]
    identity_column: str
    identities: tuple[str, ...]


def time_series_copy_plans(
    series: Iterable[str] | None = None,
) -> list[TimeSeriesCopyPlan]:
    """The plans for ``series`` (both by default), in a stable order.

    Only Valmer's identities move: the four Valmer curves and the seven Banxico
    fixings. Rows another producer wrote to the core tables stay there.
    """

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


def valmer_series_moves(series: Iterable[str] | None = None) -> list[Any]:
    """One ``TableSeriesMove`` per plan; ``msm series move|rollback`` takes this callable.

    ``msm series move valmer_connectors.services.time_series_backfill:valmer_series_moves``
    counts the rows to copy; ``--execute`` copies them.
    """

    from msm.storage_copies import TableSeriesMove

    return [
        TableSeriesMove(
            plan.source_table,
            plan.instance_table,
            plan.identity_column,
            plan.identities,
            label=plan.series,
        )
        for plan in time_series_copy_plans(series)
    ]


def backfill_valmer_time_series(
    *,
    series: Iterable[str] | None = None,
    dry_run: bool = False,
    rollback: bool = False,
    logger: Any = LOGGER,
) -> dict[str, Any]:
    """Copy the rows each target lacks (core into instance, or back) and report.

    Per series and identity it reports the rows in the source, in the target, in
    the instance, still missing and copied. ``dry_run`` only counts. Otherwise it
    raises when a series failed or left rows missing.
    """

    from msm.storage_copies import run_series_moves

    report = run_series_moves(valmer_series_moves(series), rollback=rollback, dry_run=dry_run)
    summary: dict[str, Any] = {
        "dry_run": dry_run,
        "rollback": rollback,
        "series": {},
    }
    problems: dict[str, str] = {}
    for result in report.results:
        entry: dict[str, Any] = {"status": result.status, "detail": result.detail}
        for copy in result.copies:
            entry.update(
                source_table=copy.source_table,
                target_table=copy.target_table,
                copied_rows=copy.copied_rows,
                identities={
                    key.key: {
                        "source_rows": key.source_rows,
                        "target_rows": key.target_rows,
                        "instance_rows": key.source_rows if rollback else key.target_rows,
                        "missing_rows": key.missing_rows,
                        "copied_rows": key.copied_rows,
                    }
                    for key in copy.keys
                },
            )
        summary["series"][result.label] = entry
        missing = [key.key for copy in result.copies for key in copy.keys if key.missing_rows]
        if result.status == "failed":
            problems[result.label] = result.detail
        elif missing:
            problems[result.label] = f"rows still missing for {missing!r}"
        logger.info(
            "Valmer time-series copy",
            series=result.label,
            rollback=rollback,
            dry_run=dry_run,
            status=result.status,
            missing_identities=missing,
        )
    summary["complete"] = not problems
    if problems and not dry_run:
        raise RuntimeError(f"Valmer time-series copy did not complete: {problems!r}.")
    return summary
