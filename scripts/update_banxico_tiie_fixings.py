"""Refresh only the four TIIE fixing series used by the VS Code operation."""

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


def main() -> None:
    run_banxico_fixings_update(index_identifiers=TIIE_INDEX_IDENTIFIERS)


if __name__ == "__main__":
    main()
