"""Import Valmer vectors from the repository-declared MetaTable source."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from valmer_connectors.services.vector_update import run_vector_update
from valmer_connectors.settings import VALMER_METATABLE_SOURCE_CONFIG_RESOURCE


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Valmer vectors from a MetaTable.")
    parser.add_argument(
        "--force-pricing-details-patch",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument(
        "--bypass-vector-cursor-filter",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    run_vector_update(
        source_kind="metatable",
        source_metatables_config_path=VALMER_METATABLE_SOURCE_CONFIG_RESOURCE,
        force_pricing_details_patch=args.force_pricing_details_patch,
        bypass_vector_cursor_filter=args.bypass_vector_cursor_filter,
    )


if __name__ == "__main__":
    main()
