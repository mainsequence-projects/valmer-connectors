"""One-off copy of Valmer's curves and fixings into Valmer's own instances.

The runbook is docs/time-series-instances.md. The Job
(``scripts/backfill_valmer_time_series.py``, ``time-series-backfill``) and
``valmer-connectors time-series backfill`` run it after revision ``0002`` has
created the instances, before and right after ``VALMER_TIME_SERIES_STORAGE=valmer``
is set. It reads the core Markets tables directly, which is why it runs while
Valmer still holds its table roles on them, and it never recomputes a row.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import structlog

from valmer_connectors.data_nodes.time_series_backfill import (
    DEFAULT_COPY_PAGE_SIZE,
    ValmerTimeSeriesCopyConfiguration,
    ValmerTimeSeriesCopyUpdater,
    copy_status,
    time_series_copy_plans,
)

LOGGER = structlog.get_logger(__name__)

__all__ = ["backfill_valmer_time_series"]


def backfill_valmer_time_series(
    *,
    series: Iterable[str] | None = None,
    page_size: int = DEFAULT_COPY_PAGE_SIZE,
    dry_run: bool = False,
    logger: Any = LOGGER,
) -> dict[str, Any]:
    """Copy the core rows each Valmer instance lacks and report what remains.

    With ``dry_run`` it only reports, per identity, the rows in the core table,
    in the instance and still missing. Otherwise it copies and raises when a row
    is still missing afterwards.
    """

    import msm
    from metatables import UpdateStatistics

    plans = time_series_copy_plans(series)
    # One process, one runtime: the core tables it reads and the instances it fills.
    msm.start_engine(
        models=[
            *(plan.source_table for plan in plans),
            *(plan.instance_table for plan in plans),
        ]
    )
    report: dict[str, Any] = {"dry_run": dry_run, "series": {}}
    incomplete: dict[str, list[str]] = {}
    for plan in plans:
        if not dry_run:
            updater = ValmerTimeSeriesCopyUpdater(
                ValmerTimeSeriesCopyConfiguration(
                    series=plan.series,
                    identities=plan.identities,
                ),
                output_table=plan.instance_table,
            ).set_page_size(page_size)
            # Without stored statistics the runner keeps rows older than the
            # instance's latest observation (the gaps a second pass fills) and
            # inserts instead of overwriting; the copier only returns missing keys.
            result = updater.run(override_update_stats=UpdateStatistics.return_empty())
            error = result[0] if isinstance(result, tuple) else None
            if error:
                raise RuntimeError(f"Copying {plan.series} into {plan.instance_table.__table__.name} failed.")
        status = copy_status(plan)
        report["series"][plan.series] = {
            "source_table": plan.source_table.__table__.name,
            "instance_table": plan.instance_table.__table__.name,
            "identities": status,
        }
        missing = [identity for identity, counts in status.items() if counts["missing_rows"]]
        if missing:
            incomplete[plan.series] = missing
        logger.info(
            "Valmer time-series backfill status",
            series=plan.series,
            instance_table=plan.instance_table.__table__.name,
            missing_identities=missing,
        )
    report["complete"] = not incomplete
    if incomplete and not dry_run:
        raise RuntimeError(f"Valmer time-series backfill left rows missing: {incomplete!r}.")
    return report
