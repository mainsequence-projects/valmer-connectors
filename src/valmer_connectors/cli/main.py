from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Sequence
from importlib import resources
from importlib.metadata import PackageNotFoundError, version
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any

from banxico.settings import (
    BANXICO_FIXING_INDEX_IDENTIFIERS,
    BANXICO_POLICY_TARGET_INDEX_IDENTIFIER,
)
from fred.settings import FRED_REFERENCE_RATE_INDEX_IDENTIFIERS
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
)
from valmer_connectors.settings import (
    DEFAULT_VECTOR_FIRST_LOOP_COUNT,
    VALMER_FORCE_PRICING_DETAILS_PATCH_ENV,
    VALMER_VECTOR_BUCKET_NAME_ENV,
    VALMER_VECTOR_BYPASS_CURSOR_FILTER_ENV,
    VALMER_VECTOR_UPLOAD_DEBUG_PATH_ENV,
)

SOURCE_VALMER_SKILLS_PATH = (".agents", "skills", "valmer-connectors")
PACKAGE_VALMER_SKILLS_PATH = ("agent_skills", "valmer-connectors")


def _package_version() -> str:
    try:
        return version("valmer-connectors")
    except PackageNotFoundError:
        return "0+unknown"


def _dependency_version(distribution_name: str) -> str:
    try:
        return version(distribution_name)
    except PackageNotFoundError:
        return "not-installed"


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def _version_command(_args: argparse.Namespace) -> int:
    print(f"valmer-connectors {_package_version()}")
    print(f"mainsequence {_dependency_version('mainsequence')}")
    print(f"ms-markets {_dependency_version('ms-markets')}")
    return 0


def _runtime_validate_command(_args: argparse.Namespace) -> int:
    from valmer_connectors.services.runtime_validation import validate_runtime

    print(json.dumps(validate_runtime(), indent=2, sort_keys=True))
    return 0


def _vector_update_command(args: argparse.Namespace) -> int:
    from valmer_connectors.services.vector_update import run_vector_update

    run_vector_update(
        bucket_name=args.bucket_name,
        first_loop_count=args.first_loop_count,
        debug_artifact_path=args.debug_artifact_path,
        local_bucket_path=args.local_bucket_path,
        local_bucket_path_env_var=args.local_bucket_path_env_var,
        source_kind=args.source,
        source_metatables_config_path=args.source_metatables_config_path,
        onedrive_drive_id=args.onedrive_drive_id,
        onedrive_folder_path=args.onedrive_folder_path,
        onedrive_cache_path=args.onedrive_cache_path,
        onedrive_tenant_id_secret_name=args.onedrive_tenant_id_secret_name,
        onedrive_client_id_secret_name=args.onedrive_client_id_secret_name,
        onedrive_client_secret_secret_name=args.onedrive_client_secret_secret_name,
        force_pricing_details_patch=args.force_pricing_details_patch,
        bypass_vector_cursor_filter=args.bypass_vector_cursor_filter,
    )
    return 0


def _curves_update_tiie_irs_mxn_command(args: argparse.Namespace) -> int:
    from valmer_connectors.services.curve_update import run_tiie_irs_mxn_curve_update

    run_tiie_irs_mxn_curve_update(curve_identifier=args.curve_identifier)
    return 0


def _curves_update_usd_sofr_command(args: argparse.Namespace) -> int:
    from valmer_connectors.services.curve_update import run_usd_sofr_curve_update

    run_usd_sofr_curve_update(curve_identifier=args.curve_identifier)
    return 0


def _curves_update_usd_mxn_xccy_command(args: argparse.Namespace) -> int:
    from valmer_connectors.services.curve_update import run_usd_mxn_xccy_curve_update

    run_usd_mxn_xccy_curve_update(
        curve_identifier=args.curve_identifier,
        hash_namespace=args.hash_namespace,
        rebuild_current=args.rebuild_current,
    )
    return 0


def _curves_update_mxn_government_command(args: argparse.Namespace) -> int:
    from valmer_connectors.services.curve_update import run_mxn_government_curve_update

    run_mxn_government_curve_update(
        curve_identifier=args.curve_identifier,
        bucket_name=args.bucket_name,
        debug_artifact_path=args.debug_artifact_path,
    )
    return 0


def _fixings_update_banxico_command(args: argparse.Namespace) -> int:
    from banxico.fixings import run_banxico_fixings_update

    run_banxico_fixings_update(
        index_identifiers=args.index_identifier or None,
        token_secret_name=args.token_secret_name,
        validate_metadata=not args.skip_metadata_validation,
        end_date=args.end_date,
        hash_namespace=args.hash_namespace,
    )
    return 0


def _reference_rates_update_fred_command(args: argparse.Namespace) -> int:
    from fred.reference_rates import run_fred_reference_rates_update

    run_fred_reference_rates_update(
        index_identifiers=args.index_identifier or None,
        api_key_secret_name=args.api_key_secret_name,
        validate_metadata=not args.skip_metadata_validation,
        bootstrap_lookback_days=args.bootstrap_lookback_days,
        backfill_start=args.backfill_start,
        backfill_end=args.backfill_end,
        runtime_end=args.end_date,
        hash_namespace=args.hash_namespace,
        require_hash_namespace=args.smoke,
    )
    return 0


def _reference_rates_update_banxico_command(args: argparse.Namespace) -> int:
    from banxico.policy_rates import run_banxico_policy_rates_update

    run_banxico_policy_rates_update(
        index_identifiers=args.index_identifier or None,
        token_secret_name=args.token_secret_name,
        validate_metadata=not args.skip_metadata_validation,
        bootstrap_lookback_days=args.bootstrap_lookback_days,
        backfill_start=args.backfill_start,
        backfill_end=args.backfill_end,
        runtime_end=args.end_date,
        hash_namespace=args.hash_namespace,
        require_hash_namespace=args.smoke,
    )
    return 0


def _migrations_commands_command(_args: argparse.Namespace) -> int:
    from valmer_connectors.services.migrations import migration_command_lines

    print("\n".join(migration_command_lines()))
    return 0


def _copy_valmer_skills_command(args: argparse.Namespace) -> int:
    return copy_valmer_skills_command(
        path=Path(args.path),
        dry_run=args.dry_run,
        emit_json=args.emit_json,
    )


def copy_valmer_skills_command(
    *,
    path: Path,
    dry_run: bool = False,
    emit_json: bool = False,
) -> int:
    source_root = bundled_valmer_skills_root()
    if not _traversable_exists(source_root) or not source_root.is_dir():
        raise SystemExit(f"Packaged valmer-connectors skill bundle is missing: {source_root}")

    project_dir = path.expanduser().resolve()
    destination_root = project_dir / ".agents" / "skills" / "valmer-connectors"
    source_label = _source_root_label(source_root)
    block_reason = _copy_valmer_skills_block_reason(
        project_dir=project_dir,
        destination_root=destination_root,
        source_root=source_root,
    )
    if block_reason is not None:
        payload = {
            "blocked": True,
            "destination_root": str(destination_root),
            "dry_run": dry_run,
            "project": str(project_dir),
            "reason": block_reason,
            "source": source_label,
            "updated": [],
            "updated_count": 0,
        }
        if emit_json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(block_reason, file=sys.stderr)
        return 2

    skill_sources = _iter_skill_roots(source_root)
    copied = [
        {
            "name": source.name,
            "source": f"{source_label}/{source.name}",
            "destination": str(destination_root / source.name),
        }
        for source in skill_sources
    ]
    payload = {
        "project": str(project_dir),
        "source": source_label,
        "destination_root": str(destination_root),
        "dry_run": dry_run,
        "updated_count": len(copied),
        "updated": copied,
    }

    if not dry_run:
        for source in skill_sources:
            destination = destination_root / source.name
            _copy_traversable_tree(source, destination)

    if emit_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    action = "Would update" if dry_run else "Updated"
    print(f"{action} .agents/skills/valmer-connectors from packaged Valmer skills.")
    _print_table(
        "Valmer Connectors Skills",
        ["Skill Folder", "Destination"],
        [[item["name"], item["destination"]] for item in copied],
    )
    return 0


def bundled_valmer_skills_root() -> Traversable:
    source_root = source_tree_valmer_skills_root()
    if source_root.is_dir():
        return source_root
    return package_tree_valmer_skills_root()


def source_tree_valmer_skills_root() -> Path:
    return _valmer_connectors_source_checkout_root().joinpath(*SOURCE_VALMER_SKILLS_PATH)


def package_tree_valmer_skills_root() -> Traversable:
    return resources.files("valmer_connectors").joinpath(*PACKAGE_VALMER_SKILLS_PATH)


def _valmer_connectors_source_checkout_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _copy_valmer_skills_block_reason(
    *,
    project_dir: Path,
    destination_root: Path,
    source_root: Traversable,
) -> str | None:
    if _same_resolved_path(
        project_dir, _valmer_connectors_source_checkout_root()
    ) or _is_valmer_connectors_source_checkout(project_dir):
        return (
            "Blocked: valmer-connectors copy-valmer-skills cannot run inside "
            "the valmer-connectors source checkout. Use this command only from "
            "a separate host project."
        )

    source_path = _traversable_path(source_root)
    if source_path is not None and _same_resolved_path(destination_root, source_path):
        return (
            "Blocked: destination .agents/skills/valmer-connectors is the packaged "
            "Valmer skill source. Copying here would delete source skills."
        )

    return None


def _is_valmer_connectors_source_checkout(path: Path) -> bool:
    pyproject = path / "pyproject.toml"
    if not pyproject.is_file():
        return False
    try:
        project_config = pyproject.read_text(encoding="utf-8")
    except OSError:
        return False
    return (
        'name = "valmer-connectors"' in project_config
        and (path / "src" / "valmer_connectors").is_dir()
        and (path / ".agents" / "skills" / "valmer-connectors").is_dir()
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="valmer-connectors",
        description="Operational CLI for the Valmer connectors project.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=_package_version(),
    )

    subcommands = parser.add_subparsers(dest="command", required=True)

    version_parser = subcommands.add_parser("version", help="Print package version.")
    version_parser.set_defaults(func=_version_command)

    copy_skills_parser = subcommands.add_parser(
        "copy-valmer-skills",
        help="Copy packaged Valmer connector agent skills into a host project.",
    )
    copy_skills_parser.add_argument(
        "--path",
        default=".",
        help="Host project directory. Defaults to the current directory.",
    )
    copy_skills_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without writing files.",
    )
    copy_skills_parser.add_argument(
        "--json",
        dest="emit_json",
        action="store_true",
        help="Emit machine-readable JSON.",
    )
    copy_skills_parser.set_defaults(func=_copy_valmer_skills_command)

    runtime_parser = subcommands.add_parser("runtime", help="Runtime commands.")
    runtime_subcommands = runtime_parser.add_subparsers(
        dest="runtime_command",
        required=True,
    )
    runtime_validate_parser = runtime_subcommands.add_parser(
        "validate",
        help="Bootstrap runtime tables and print a JSON validation payload.",
    )
    runtime_validate_parser.set_defaults(func=_runtime_validate_command)

    vector_parser = subcommands.add_parser("vector", help="Valmer vector commands.")
    vector_subcommands = vector_parser.add_subparsers(
        dest="vector_command",
        required=True,
    )
    vector_update_parser = vector_subcommands.add_parser(
        "update",
        help="Run the Valmer vector import, asset/detail sync, pricing hydration, and DataNode update.",
    )
    vector_update_parser.add_argument(
        "--bucket-name",
        default=None,
        help=(
            "Main Sequence Artifact bucket name for Valmer vector source files. "
            f"If omitted, reads {VALMER_VECTOR_BUCKET_NAME_ENV} from the environment."
        ),
    )
    vector_update_parser.add_argument(
        "--debug-artifact-path",
        default=None,
        help="Local Valmer Excel file or folder. Overrides bucket import for this command.",
    )
    vector_update_parser.add_argument(
        "--local-bucket-path",
        default=None,
        help=(
            "Local folder of Valmer Excel files. The folder is treated like a "
            "local Artifact bucket for this command."
        ),
    )
    vector_update_parser.add_argument(
        "--local-bucket-path-env-var",
        default=None,
        help=(
            "Environment variable containing a local Valmer folder path. "
            f"For VS Code debug configs, use {VALMER_VECTOR_UPLOAD_DEBUG_PATH_ENV}."
        ),
    )
    vector_update_parser.add_argument(
        "--source",
        choices=["artifact", "metatable", "onedrive-graph"],
        default="artifact",
        help="Source adapter for Valmer vector rows.",
    )
    vector_update_parser.add_argument(
        "--source-metatables-config-path",
        default=None,
        help="JSON config file with MetaTableValmerSource entries for --source metatable.",
    )
    vector_update_parser.add_argument(
        "--onedrive-drive-id",
        default=None,
        help=(
            "Microsoft Graph drive id for --source onedrive-graph. If omitted, "
            "reads VALMER_ONEDRIVE_DRIVE_ID env or Main Sequence Constant "
            "VALMER_ONEDRIVE_DRIVE_ID."
        ),
    )
    vector_update_parser.add_argument(
        "--onedrive-folder-path",
        default=None,
        help=(
            "OneDrive folder path for --source onedrive-graph. Defaults to the "
            "configured package folder path."
        ),
    )
    vector_update_parser.add_argument(
        "--onedrive-cache-path",
        default=None,
        help=(
            "Local cache directory for downloaded Graph files. Defaults to "
            "/tmp/valmer-vector-cache."
        ),
    )
    vector_update_parser.add_argument(
        "--onedrive-tenant-id-secret-name",
        default=None,
        help=(
            "Credential key for the Azure tenant id. The value is resolved from "
            "an environment variable with this name first, then Main Sequence Secret."
        ),
    )
    vector_update_parser.add_argument(
        "--onedrive-client-id-secret-name",
        default=None,
        help=(
            "Credential key for the Azure app client id. The value is resolved "
            "from an environment variable with this name first, then Main Sequence Secret."
        ),
    )
    vector_update_parser.add_argument(
        "--onedrive-client-secret-secret-name",
        default=None,
        help=(
            "Credential key for the Azure app client secret. The value is resolved "
            "from an environment variable with this name first, then Main Sequence Secret."
        ),
    )
    vector_update_parser.add_argument(
        "--first-loop-count",
        type=_positive_int,
        default=DEFAULT_VECTOR_FIRST_LOOP_COUNT,
        help="Number of compatibility loop runs when update statistics are missing.",
    )
    vector_update_parser.add_argument(
        "--force-pricing-details-patch",
        action="store_true",
        default=None,
        help=(
            "Force current pricing detail rehydration for selected target bonds. "
            f"Equivalent to setting {VALMER_FORCE_PRICING_DETAILS_PATCH_ENV}=1."
        ),
    )
    vector_update_parser.add_argument(
        "--bypass-vector-cursor-filter",
        action="store_true",
        default=None,
        help=(
            "Keep source rows even when vector storage already has equal or newer "
            "observations. "
            f"Equivalent to setting {VALMER_VECTOR_BYPASS_CURSOR_FILTER_ENV}=1."
        ),
    )
    vector_update_parser.set_defaults(func=_vector_update_command)

    curves_parser = subcommands.add_parser("curves", help="Valmer curve commands.")
    curves_subcommands = curves_parser.add_subparsers(
        dest="curves_command",
        required=True,
    )
    curves_update_parser = curves_subcommands.add_parser(
        "update-tiie-irs-mxn",
        help="Run the Valmer TIIE overnight OIS curve update from IRS_MXN_CURVE.",
    )
    curves_update_parser.add_argument(
        "--curve-identifier",
        default=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        help="Curve.unique_identifier to publish with the Valmer TIIE IRS MXN builder.",
    )
    curves_update_parser.set_defaults(func=_curves_update_tiie_irs_mxn_command)

    curves_usd_sofr_parser = curves_subcommands.add_parser(
        "update-usd-sofr",
        help="Run the Valmer USD SOFR overnight OIS curve update from IRS_USD_CURVE.",
    )
    curves_usd_sofr_parser.add_argument(
        "--curve-identifier",
        default=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        help="Curve.unique_identifier to publish with the Valmer USD SOFR builder.",
    )
    curves_usd_sofr_parser.set_defaults(func=_curves_update_usd_sofr_command)

    curves_usd_mxn_xccy_parser = curves_subcommands.add_parser(
        "update-usd-mxn-xccy",
        help="Run the Valmer USD/MXN F-TIIE/SOFR cross-currency curve update.",
    )
    curves_usd_mxn_xccy_parser.add_argument(
        "--curve-identifier",
        default=VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
        help="Curve.unique_identifier to publish with the Valmer USD/MXN XCCY builder.",
    )
    curves_usd_mxn_xccy_parser.add_argument(
        "--hash-namespace",
        default=None,
        help="Optional DataNode hash namespace for isolated USD/MXN XCCY curve runs.",
    )
    curves_usd_mxn_xccy_parser.add_argument(
        "--rebuild-current",
        action="store_true",
        help=(
            "Rebuild the current Valmer source date instead of filtering it out "
            "when the same curve date is already published."
        ),
    )
    curves_usd_mxn_xccy_parser.set_defaults(func=_curves_update_usd_mxn_xccy_command)

    curves_mxn_government_parser = curves_subcommands.add_parser(
        "update-mxn-government",
        help="Run the Valmer MXN government bond discount-curve update.",
    )
    curves_mxn_government_parser.add_argument(
        "--curve-identifier",
        default=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
        help="Curve.unique_identifier to publish with the Valmer MXN government curve builder.",
    )
    curves_mxn_government_parser.add_argument(
        "--bucket-name",
        default=None,
        help=(
            "Deprecated; ignored. The MXN government curve reads "
            "ValmerVectorPricesStorage joined to ValmerAssetDetailsTable."
        ),
    )
    curves_mxn_government_parser.add_argument(
        "--debug-artifact-path",
        default=None,
        help=(
            "Deprecated; ignored. Run the vector DataNode with a debug artifact "
            "first, then build this curve from persisted vector storage."
        ),
    )
    curves_mxn_government_parser.set_defaults(func=_curves_update_mxn_government_command)

    fixings_parser = subcommands.add_parser("fixings", help="Reference-rate fixing commands.")
    fixings_subcommands = fixings_parser.add_subparsers(
        dest="fixings_command",
        required=True,
    )
    banxico_fixings_parser = fixings_subcommands.add_parser(
        "update-banxico",
        help="Run the Banxico TIIE/CETE fixing update.",
    )
    banxico_fixings_parser.add_argument(
        "--index-identifier",
        choices=BANXICO_FIXING_INDEX_IDENTIFIERS,
        action="append",
        default=[],
        help=(
            "Pricing index identifier to update. Repeat to select multiple. "
            "Defaults to all supported Banxico TIIE/CETE fixing indexes."
        ),
    )
    banxico_fixings_parser.add_argument(
        "--token-secret-name",
        default="BANXICO_TOKEN",
        help="Main Sequence Secret name used to resolve the Banxico SIE API token.",
    )
    banxico_fixings_parser.add_argument(
        "--skip-metadata-validation",
        action="store_true",
        help="Skip token-backed Banxico series metadata validation for this run.",
    )
    banxico_fixings_parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive Banxico request end date in YYYY-MM-DD form. Defaults to yesterday UTC.",
    )
    banxico_fixings_parser.add_argument(
        "--hash-namespace",
        default=None,
        help="Optional DataNode hash namespace for isolated validation runs.",
    )
    banxico_fixings_parser.set_defaults(func=_fixings_update_banxico_command)

    reference_rates_parser = subcommands.add_parser(
        "reference-rates",
        help="External analytical reference-rate observation commands.",
    )
    reference_rates_subcommands = reference_rates_parser.add_subparsers(
        dest="reference_rates_command",
        required=True,
    )

    fred_reference_rates_parser = reference_rates_subcommands.add_parser(
        "update-fred",
        help="Publish FRED Treasury yields and the Fed target upper limit.",
    )
    fred_reference_rates_parser.add_argument(
        "--index-identifier",
        choices=FRED_REFERENCE_RATE_INDEX_IDENTIFIERS,
        action="append",
        default=[],
        help="Reference-rate Index identifier to update. Repeat to select multiple.",
    )
    fred_reference_rates_parser.add_argument(
        "--api-key-secret-name",
        default="FRED_API_KEY",
        help="Environment variable or Main Sequence Secret name for the FRED API key.",
    )
    _add_reference_rate_window_arguments(fred_reference_rates_parser)
    fred_reference_rates_parser.set_defaults(func=_reference_rates_update_fred_command)

    banxico_policy_parser = reference_rates_subcommands.add_parser(
        "update-banxico-policy",
        help="Publish the Banco de Mexico policy target from Banxico SIE.",
    )
    banxico_policy_parser.add_argument(
        "--index-identifier",
        choices=(BANXICO_POLICY_TARGET_INDEX_IDENTIFIER,),
        action="append",
        default=[],
        help="Policy-rate Index identifier to update.",
    )
    banxico_policy_parser.add_argument(
        "--token-secret-name",
        default="BANXICO_TOKEN",
        help="Environment variable or Main Sequence Secret name for the Banxico token.",
    )
    _add_reference_rate_window_arguments(banxico_policy_parser)
    banxico_policy_parser.set_defaults(func=_reference_rates_update_banxico_command)

    migrations_parser = subcommands.add_parser(
        "migrations",
        help="Migration helper commands.",
    )
    migrations_subcommands = migrations_parser.add_subparsers(
        dest="migrations_command",
        required=True,
    )
    migrations_commands_parser = migrations_subcommands.add_parser(
        "commands",
        help="Print the canonical migration commands for this project.",
    )
    migrations_commands_parser.set_defaults(func=_migrations_commands_command)

    return parser


def _add_reference_rate_window_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bootstrap-lookback-days",
        type=_positive_int,
        default=90,
        help="First-run calendar-day lookback. Defaults to 90 days.",
    )
    parser.add_argument(
        "--backfill-start",
        default=None,
        help="Inclusive timezone-aware bounded-backfill start timestamp.",
    )
    parser.add_argument(
        "--backfill-end",
        default=None,
        help="Inclusive timezone-aware bounded-backfill end timestamp.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional runtime request end date. Normal jobs default to yesterday UTC.",
    )
    parser.add_argument(
        "--hash-namespace",
        default=None,
        help="DataNode hash namespace for an isolated shared-backend validation run.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Require an explicit hash namespace for the initial 90-day smoke run.",
    )
    parser.add_argument(
        "--skip-metadata-validation",
        action="store_true",
        help="Skip authenticated provider metadata validation for this run.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


def _source_root_label(source_root: Traversable) -> str:
    source_path = _traversable_path(source_root)
    if source_path is not None and _same_resolved_path(
        source_path,
        source_tree_valmer_skills_root(),
    ):
        return "/".join(SOURCE_VALMER_SKILLS_PATH)
    return "/".join(("valmer_connectors", *PACKAGE_VALMER_SKILLS_PATH))


def _iter_skill_roots(source_root: Traversable) -> list[Traversable]:
    return [
        item
        for item in sorted(source_root.iterdir(), key=lambda child: child.name)
        if item.is_dir() and not item.name.startswith(".") and not item.name.startswith("__")
    ]


def _copy_traversable_tree(source: Traversable, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)

    for child in source.iterdir():
        child_destination = destination / child.name
        if child.is_dir():
            _copy_traversable_tree(child, child_destination)
            continue
        if child.is_file():
            child_destination.parent.mkdir(parents=True, exist_ok=True)
            child_destination.write_bytes(child.read_bytes())


def _traversable_exists(item: Traversable) -> bool:
    try:
        return item.exists()
    except FileNotFoundError:
        return False


def _traversable_path(item: Traversable) -> Path | None:
    return item if isinstance(item, Path) else None


def _same_resolved_path(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except FileNotFoundError:
        return left.expanduser().resolve(strict=False) == right.expanduser().resolve(strict=False)


def _print_table(title: str, headers: list[str], rows: list[list[Any]]) -> None:
    print(title)
    if not rows:
        print("  (no rows)")
        return

    widths = [
        max(len(str(value)) for value in [header, *(row[index] for row in rows)])
        for index, header in enumerate(headers)
    ]
    header_line = "  " + "  ".join(
        str(header).ljust(widths[index]) for index, header in enumerate(headers)
    )
    separator = "  " + "  ".join("-" * width for width in widths)
    print(header_line)
    print(separator)
    for row in rows:
        print("  " + "  ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))


if __name__ == "__main__":
    raise SystemExit(main())
