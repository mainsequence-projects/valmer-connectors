"""Main Sequence Job: copy Valmer's curves and fixings into Valmer's own instances.

A one-off step of docs/time-series-instances.md: it copies, never recomputes, the
Valmer rows of the core Markets discount-curve and Index-fixing tables into
``valmer_connectors__discountcurvests__valmer`` and
``valmer_connectors__indexfixingsts__valmer``, and fails when a row is still
missing. It makes no Markets row call, so it does not select the Markets API.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from valmer_connectors.data_nodes.time_series_backfill import DEFAULT_COPY_PAGE_SIZE
from valmer_connectors.services.time_series_backfill import backfill_valmer_time_series


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--series",
        action="append",
        choices=("discount_curves", "index_fixings"),
        default=None,
        help="Copy only this series; repeat for both (default: both).",
    )
    parser.add_argument("--page-size", type=int, default=DEFAULT_COPY_PAGE_SIZE)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report the rows each instance is missing.",
    )
    args = parser.parse_args(argv)
    report = backfill_valmer_time_series(
        series=args.series,
        page_size=args.page_size,
        dry_run=args.dry_run,
    )
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
