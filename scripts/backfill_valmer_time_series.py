"""Main Sequence Job: copy Valmer's curves, fixings and Index values into Valmer's tables, or back.

A step of docs/time-series-instances.md. It copies, never recomputes, the Valmer
rows of the Markets discount-curve, Index-fixing and daily Index-value tables
that ``valmer_connectors__discountcurvests__valmer``,
``valmer_connectors__indexfixingsts__valmer`` and
``valmer_connectors__index_values__t_1d`` lack (ms-markets'
``msm.storage_copies``) with ``--execute``, only counts them without it, and
fails while a row is still missing. ``--rollback``
copies the rows Valmer's tables gained back into the Markets tables. It makes no
Markets row call, so it does not select the Markets API.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from valmer_connectors.services.time_series_backfill import backfill_valmer_time_series


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--series",
        action="append",
        choices=("discount_curves", "index_fixings", "index_values"),
        default=None,
        help="Copy only this series; repeat for several (default: all three).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Copy the rows; without it only report the rows each target is missing.",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Copy the rows the instances gained back into the core tables.",
    )
    args = parser.parse_args(argv)
    report = backfill_valmer_time_series(
        series=args.series,
        dry_run=not args.execute,
        rollback=args.rollback,
    )
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
