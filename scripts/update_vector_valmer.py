from __future__ import annotations

import argparse
from collections.abc import Sequence

from valmer_connectors.services.vector_update import run_vector_update


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Valmer vectors from Artifacts.")
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
        force_pricing_details_patch=args.force_pricing_details_patch,
        bypass_vector_cursor_filter=args.bypass_vector_cursor_filter,
    )


if __name__ == "__main__":
    main()
