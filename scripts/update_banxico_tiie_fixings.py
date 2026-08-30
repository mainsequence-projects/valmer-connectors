"""Refresh only the four TIIE fixing series used by the VS Code operation."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from banxico.fixings import run_banxico_fixings_update
from banxico.settings import (
    TIIE_28_INDEX_IDENTIFIER,
    TIIE_91_INDEX_IDENTIFIER,
    TIIE_182_INDEX_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_IDENTIFIER,
)

TIIE_INDEX_IDENTIFIERS = (
    TIIE_OVERNIGHT_INDEX_IDENTIFIER,
    TIIE_28_INDEX_IDENTIFIER,
    TIIE_91_INDEX_IDENTIFIER,
    TIIE_182_INDEX_IDENTIFIER,
)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh the four configured TIIE fixings.")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args(argv)
    run_banxico_fixings_update(
        index_identifiers=TIIE_INDEX_IDENTIFIERS,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
