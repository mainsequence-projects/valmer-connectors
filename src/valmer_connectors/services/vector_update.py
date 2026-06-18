from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from valmer_connectors.data_nodes.nodes import (
    ImportValmer,
    ImportValmerConfig,
    MetaTableValmerSourceConfig,
)
from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.settings import DEFAULT_VECTOR_FIRST_LOOP_COUNT
from valmer_connectors.settings import resolve_valmer_vector_bucket_name

DEFAULT_FIRST_LOOP_COUNT = DEFAULT_VECTOR_FIRST_LOOP_COUNT
DEBUG_ARTIFACT_PATH_ENV = "DEBUG_ARTIFACT_PATH"


@contextmanager
def _debug_artifact_path(path: str | None) -> Iterator[None]:
    if path is None:
        yield
        return

    previous = os.environ.get(DEBUG_ARTIFACT_PATH_ENV)
    os.environ[DEBUG_ARTIFACT_PATH_ENV] = path
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(DEBUG_ARTIFACT_PATH_ENV, None)
        else:
            os.environ[DEBUG_ARTIFACT_PATH_ENV] = previous


def build_import_valmer(
    *,
    bucket_name: str | None = None,
    source_kind: str = "artifact",
    source_metatables: list[MetaTableValmerSourceConfig] | None = None,
) -> ImportValmer:
    return ImportValmer(
        config=ImportValmerConfig(
            bucket_name=resolve_valmer_vector_bucket_name(bucket_name),
            source_kind=source_kind,
            source_metatables=source_metatables,
        ),
    )


def prepare_import_valmer(
    *,
    bucket_name: str | None = None,
    force_pricing_update: bool = False,
    source_kind: str = "artifact",
    source_metatables: list[MetaTableValmerSourceConfig] | None = None,
) -> ImportValmer:
    updater = build_import_valmer(
        bucket_name=bucket_name,
        source_kind=source_kind,
        source_metatables=source_metatables,
    )
    updater.prepare_for_update(
        force_pricing_update=force_pricing_update,
    )
    return updater


def _is_first_time_update(updater: ImportValmer) -> bool:
    try:
        updater.get_update_statistics()
    except AttributeError:
        return True
    return False


def load_metatable_sources_config(path: str | Path) -> list[MetaTableValmerSourceConfig]:
    config_path = Path(path).expanduser()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    raw_sources = payload.get("sources") if isinstance(payload, dict) else payload
    if not isinstance(raw_sources, list):
        raise ValueError(
            "MetaTable source config must be a list or an object with a 'sources' list."
        )
    return [MetaTableValmerSourceConfig.model_validate(source) for source in raw_sources]


def run_vector_update(
    *,
    bucket_name: str | None = None,
    first_loop_count: int = DEFAULT_FIRST_LOOP_COUNT,
    debug_artifact_path: str | None = None,
    source_kind: str = "artifact",
    source_metatables_config_path: str | None = None,
) -> None:
    """Run the Valmer vector update through the package service boundary."""

    bootstrap_runtime()
    source_metatables = None
    if source_kind == "metatable":
        if debug_artifact_path is not None:
            raise ValueError("--debug-artifact-path cannot be used with --source metatable.")
        if source_metatables_config_path is None:
            raise ValueError("--source-metatables-config-path is required with --source metatable.")
        source_metatables = load_metatable_sources_config(source_metatables_config_path)

    with _debug_artifact_path(debug_artifact_path):
        updater = build_import_valmer(
            bucket_name=bucket_name,
            source_kind=source_kind,
            source_metatables=source_metatables,
        )
        first_time_update = _is_first_time_update(updater)

        if first_time_update:
            for _ in range(first_loop_count):
                loop_updater = build_import_valmer(
                    bucket_name=bucket_name,
                    source_kind=source_kind,
                    source_metatables=source_metatables,
                )
                loop_updater.prepare_for_update(force_pricing_update=True)
                loop_updater.run(force_update=True)
            return

        updater.prepare_for_update(force_pricing_update=True)
        updater.run(force_update=True)
