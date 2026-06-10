from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from valmer_connectors.data_nodes.nodes import ImportValmer, ImportValmerConfig
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
) -> ImportValmer:
    return ImportValmer(
        config=ImportValmerConfig(
            bucket_name=resolve_valmer_vector_bucket_name(bucket_name),
        ),
    )


def prepare_import_valmer(
    *,
    bucket_name: str | None = None,
    force_pricing_update: bool = False,
) -> ImportValmer:
    updater = build_import_valmer(bucket_name=bucket_name)
    updater.prepare_for_update(force_pricing_update=force_pricing_update)
    return updater


def _is_first_time_update(updater: ImportValmer) -> bool:
    try:
        updater.get_update_statistics()
    except AttributeError:
        return True
    return False


def run_vector_update(
    *,
    bucket_name: str | None = None,
    first_loop_count: int = DEFAULT_FIRST_LOOP_COUNT,
    debug_artifact_path: str | None = None,
) -> None:
    """Run the Valmer vector update through the package service boundary."""

    bootstrap_runtime()

    with _debug_artifact_path(debug_artifact_path):
        first_time_update = _is_first_time_update(
            build_import_valmer(bucket_name=bucket_name),
        )

        if first_time_update:
            for _ in range(first_loop_count):
                updater = prepare_import_valmer(
                    bucket_name=bucket_name,
                    force_pricing_update=True,
                )
                updater.run(force_update=True)
            return

        updater = prepare_import_valmer(
            bucket_name=bucket_name,
            force_pricing_update=True,
        )
        updater.run(force_update=True)
