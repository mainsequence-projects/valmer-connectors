from __future__ import annotations

import argparse
import json

from valmer_connectors.services.pricing_details_repair import (
    repair_valmer_asset_pricing_details,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Patch persisted Valmer asset pricing details that contain stale "
            "serialized instrument payloads. This script queries existing "
            "pricing-detail rows directly; it does not replay Valmer vectors."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write patched pricing-detail rows. Omit for dry-run mode.",
    )
    parser.add_argument(
        "--current-only",
        action="store_true",
        help="Only repair AssetCurrentPricingDetails rows; skip timestamped storage.",
    )
    parser.add_argument(
        "--asset-identifier",
        action="append",
        default=None,
        help="Limit repair to one AssetTable.unique_identifier. Repeat for many.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Rows to fetch per custom query page.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum rows to scan per pricing-detail table.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Bulk upsert batch size. Defaults to VALMER_PRICING_DETAILS_BATCH_SIZE.",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip the post-apply stale-signature verification query.",
    )
    parser.add_argument(
        "--allow-unresolved",
        action="store_true",
        help="Apply repairable rows even if some stale rows cannot be resolved.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = repair_valmer_asset_pricing_details(
        apply=args.apply,
        include_history=not args.current_only,
        asset_identifiers=args.asset_identifier,
        page_size=args.page_size,
        limit=args.limit,
        batch_size=args.batch_size,
        verify=not args.no_verify,
        allow_unresolved=args.allow_unresolved,
    )
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
