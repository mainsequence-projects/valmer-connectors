from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version

from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
)
from valmer_connectors.settings import DEFAULT_VECTOR_FIRST_LOOP_COUNT
from valmer_connectors.settings import VALMER_VECTOR_BUCKET_NAME_ENV


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
    )
    return 0


def _curves_update_tiie_zero_command(args: argparse.Namespace) -> int:
    from valmer_connectors.services.curve_update import run_tiie_zero_curve_update

    run_tiie_zero_curve_update(curve_identifier=args.curve_identifier)
    return 0


def _migrations_commands_command(_args: argparse.Namespace) -> int:
    from valmer_connectors.services.migrations import migration_command_lines

    print("\n".join(migration_command_lines()))
    return 0


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
        "--first-loop-count",
        type=_positive_int,
        default=DEFAULT_VECTOR_FIRST_LOOP_COUNT,
        help="Number of compatibility loop runs when update statistics are missing.",
    )
    vector_update_parser.set_defaults(func=_vector_update_command)

    curves_parser = subcommands.add_parser("curves", help="Valmer curve commands.")
    curves_subcommands = curves_parser.add_subparsers(
        dest="curves_command",
        required=True,
    )
    curves_update_parser = curves_subcommands.add_parser(
        "update-tiie-zero",
        help="Run the Valmer TIIE 28 discount-curve update.",
    )
    curves_update_parser.add_argument(
        "--curve-identifier",
        default=VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
        help="Curve.unique_identifier to publish with the Valmer TIIE curve builder.",
    )
    curves_update_parser.set_defaults(func=_curves_update_tiie_zero_command)

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


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
