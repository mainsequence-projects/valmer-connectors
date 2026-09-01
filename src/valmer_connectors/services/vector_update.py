from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.resources.abc import Traversable
from pathlib import Path

import structlog
from msm.api.base import operation_result_rows
from msm.repositories.base import compile_markets_statement, execute_markets_operation
from sqlalchemy import func, select

from valmer_connectors.data_nodes.nodes import (
    ImportValmer,
    ImportValmerConfig,
    MetaTableValmerSourceConfig,
)
from valmer_connectors.data_nodes.valmer_vector_storage import (
    ValmerVectorPricesStorage,
    ensure_valmer_vector_runtime,
)
from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.settings import (
    DEFAULT_VECTOR_FIRST_LOOP_COUNT,
    resolve_valmer_force_pricing_details_patch,
    resolve_valmer_vector_bucket_name,
    resolve_valmer_vector_bypass_cursor_filter,
    resolve_valmer_vector_file_batch_size,
)

LOGGER = structlog.get_logger(__name__)

DEFAULT_FIRST_LOOP_COUNT = DEFAULT_VECTOR_FIRST_LOOP_COUNT
DEBUG_ARTIFACT_PATH_ENV = "DEBUG_ARTIFACT_PATH"
DEBUG_ARTIFACT_FILES_ENV = "DEBUG_ARTIFACT_FILES"
_LOCAL_VECTOR_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{8})")
_LOCAL_FILE_MATERIALIZE_READ_BYTES = 1024
_LOCAL_FILE_MATERIALIZE_ATTEMPTS = 3
_LOCAL_FILE_MATERIALIZE_RETRY_SECONDS = 5.0


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
    force_pricing_update: bool | None = None,
    bypass_vector_cursor_filter: bool | None = None,
    source_kind: str = "artifact",
    source_metatables: list[MetaTableValmerSourceConfig] | None = None,
) -> ImportValmer:
    updater = build_import_valmer(
        bucket_name=bucket_name,
        source_kind=source_kind,
        source_metatables=source_metatables,
    )
    updater.prepare_for_update(
        force_pricing_update=resolve_valmer_force_pricing_details_patch(
            force_pricing_update,
        ),
        bypass_vector_cursor_filter=resolve_valmer_vector_bypass_cursor_filter(
            bypass_vector_cursor_filter,
        ),
    )
    return updater


def _is_first_time_update(updater: ImportValmer) -> bool:
    try:
        updater.get_update_statistics()
    except AttributeError:
        return True
    return False


def load_metatable_sources_config(
    path: str | os.PathLike[str] | Traversable,
) -> list[MetaTableValmerSourceConfig]:
    if isinstance(path, (str, os.PathLike)):
        config_text = Path(path).expanduser().read_text(encoding="utf-8")
    else:
        config_text = path.read_text(encoding="utf-8")
    payload = json.loads(config_text)
    raw_sources = payload.get("sources") if isinstance(payload, dict) else payload
    if not isinstance(raw_sources, list):
        raise ValueError(
            "MetaTable source config must be a list or an object with a 'sources' list."
        )
    return [MetaTableValmerSourceConfig.model_validate(source) for source in raw_sources]


def preflight_metatable_source(
    path: str | os.PathLike[str] | Traversable,
) -> dict[str, object]:
    """Validate the repository-configured MetaTable and perform a one-row read probe."""

    sources = load_metatable_sources_config(path)
    if len(sources) != 1:
        raise ValueError("The MetaTable Job requires exactly one configured source.")
    source = sources[0]
    meta_table = ImportValmer._resolve_source_metatable(source)
    metatable_uid = str(meta_table.uid)
    if str(getattr(meta_table, "provisioning_status", "")).lower() != "active":
        raise ValueError(
            f"MetaTable {metatable_uid} is not active "
            f"(status={getattr(meta_table, 'provisioning_status', None)!r})."
        )
    physical_table_name = ImportValmer._physical_metatable_name(meta_table)

    contract_columns = ImportValmer._metatable_contract_columns(meta_table)
    if not contract_columns:
        raise ValueError(
            f"MetaTable {metatable_uid} does not expose a column contract."
        )
    missing_contract_columns = sorted(set(source.column_map) - contract_columns)
    if missing_contract_columns:
        raise ValueError(
            f"MetaTable {metatable_uid} is missing required Valmer source columns: "
            f"{missing_contract_columns}."
        )

    probe_source = source.model_copy(update={"max_rows": 1})
    sql = ImportValmer._build_source_select_sql(
        probe_source,
        physical_table_name=physical_table_name,
        minimum_valuation_date=None,
    )
    result = ImportValmer._run_metatable_query(meta_table, sql, timeout=30)
    frame = ImportValmer._frame_from_metatable_query_result(
        result,
        source_name=source.source_name,
    )
    frame = ImportValmer._align_metatable_source_columns(
        frame,
        expected_columns=tuple(source.column_map),
    )
    missing_result_columns = sorted(set(source.column_map) - set(frame.columns))
    if missing_result_columns:
        raise ValueError(
            f"MetaTable {metatable_uid} read probe did not return required columns: "
            f"{missing_result_columns}."
        )
    return {
        "uid": str(meta_table.uid),
        "identifier": str(meta_table.identifier or ""),
        "physical_table_name": physical_table_name,
        "source_name": source.source_name,
        "source_kind": "metatable",
        "contract_column_count": len(contract_columns),
        "sample_row_count": len(frame.index),
    }


def _resolve_local_bucket_path(
    *,
    local_bucket_path: str | None,
    local_bucket_path_env_var: str | None,
) -> str | None:
    if local_bucket_path and local_bucket_path_env_var:
        raise ValueError(
            "--local-bucket-path and --local-bucket-path-env-var are mutually exclusive."
        )
    if local_bucket_path:
        return local_bucket_path
    if local_bucket_path_env_var is None:
        return None

    resolved = os.environ.get(local_bucket_path_env_var)
    if not resolved:
        raise ValueError(
            f"{local_bucket_path_env_var} must be set when "
            "--local-bucket-path-env-var is used."
        )
    return resolved


@contextmanager
def _debug_artifact_files(paths: str | None) -> Iterator[None]:
    if paths is None:
        yield
        return

    previous = os.environ.get(DEBUG_ARTIFACT_FILES_ENV)
    os.environ[DEBUG_ARTIFACT_FILES_ENV] = paths
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(DEBUG_ARTIFACT_FILES_ENV, None)
        else:
            os.environ[DEBUG_ARTIFACT_FILES_ENV] = previous


def _expand_local_files(source_artifact_path: str | None) -> list[Path] | None:
    """Sorted local source files (dir → its ``.xls*``; single file → ``[it]``), or
    ``None`` for the bucket path."""
    if source_artifact_path is None:
        return None
    base = Path(source_artifact_path)
    if base.is_dir():
        return sorted(base.rglob("*.xls*"))
    return [base]


def _as_utc_datetime(value: object) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            value = dt.datetime.fromisoformat(text)
        except ValueError:
            return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _rows_from_query_result(result: object) -> list[dict]:
    if not isinstance(result, dict):
        return []
    data = result.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    rows = result.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    results = result.get("results")
    if isinstance(results, list):
        return [row for row in results if isinstance(row, dict)]
    result_rows = result.get("result")
    if isinstance(result_rows, list):
        return [row for row in result_rows if isinstance(row, dict)]
    if isinstance(data, dict):
        nested_rows = (
            data.get("rows")
            or data.get("data")
            or data.get("results")
            or data.get("result")
        )
        if isinstance(nested_rows, list):
            return [row for row in nested_rows if isinstance(row, dict)]
    return []


def _latest_vector_storage_time_index() -> dt.datetime | None:
    context = ensure_valmer_vector_runtime(timeout=120)
    table = ValmerVectorPricesStorage.__table__
    statement = select(func.max(table.c.time_index).label("latest_time_index"))
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[ValmerVectorPricesStorage],
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    rows = list(operation_result_rows(result))
    if not rows:
        return None
    value = rows[0].get("latest_time_index")
    return _as_utc_datetime(value)


def _local_vector_file_time_index(path: Path) -> dt.datetime | None:
    match = _LOCAL_VECTOR_FILE_DATE_RE.search(path.name)
    if match is None:
        return None
    raw_date = match.group(1)
    if "-" in raw_date:
        valuation_date = dt.date.fromisoformat(raw_date)
    else:
        valuation_date = dt.datetime.strptime(raw_date, "%Y%m%d").date()
    return dt.datetime.combine(valuation_date, dt.time(23, 59, 59, tzinfo=dt.UTC))


def _select_local_vector_files_for_update(
    paths: list[Path],
    latest_time_index: dt.datetime | None,
) -> list[Path]:
    if latest_time_index is None:
        LOGGER.info(
            "No persisted Valmer vector cursor found; all local vector files are candidates.",
            file_count=len(paths),
        )
        return paths

    selected: list[Path] = []
    undated: list[Path] = []
    skipped_count = 0
    latest_time_index = latest_time_index.astimezone(dt.UTC)
    for path in paths:
        file_time_index = _local_vector_file_time_index(path)
        if file_time_index is None:
            selected.append(path)
            undated.append(path)
            continue
        if file_time_index > latest_time_index:
            selected.append(path)
        else:
            skipped_count += 1

    LOGGER.info(
        "Filtered local Valmer vector files by latest persisted vector date.",
        latest_vector_time_index=latest_time_index.isoformat(),
        input_file_count=len(paths),
        selected_file_count=len(selected),
        skipped_file_count=skipped_count,
        undated_file_count=len(undated),
    )
    if undated:
        LOGGER.warning(
            "Local Valmer vector files without a parseable date remain selected.",
            files=[path.name for path in undated],
        )
    return selected


def _is_materialized_local_file(path: Path) -> bool:
    try:
        stat_result = path.stat()
    except OSError:
        return False
    blocks = getattr(stat_result, "st_blocks", None)
    return not (stat_result.st_size > 0 and blocks == 0)


def _request_local_file_materialization(path: Path) -> None:
    with path.open("rb") as handle:
        handle.read(_LOCAL_FILE_MATERIALIZE_READ_BYTES)


def _ensure_local_vector_files_materialized(paths: list[Path]) -> None:
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise RuntimeError(
            "Local Valmer vector source files are missing:\n"
            + "\n".join(f"  - {path}" for path in missing)
        )

    placeholders = [
        path
        for path in paths
        if not _is_materialized_local_file(path)
    ]
    if not placeholders:
        return

    LOGGER.info(
        "Materializing local Valmer vector cloud placeholders",
        file_count=len(placeholders),
        files=[path.name for path in placeholders],
    )
    remaining: list[Path] = []
    for path in placeholders:
        materialized = False
        last_error: BaseException | None = None
        for attempt in range(1, _LOCAL_FILE_MATERIALIZE_ATTEMPTS + 1):
            try:
                LOGGER.info(
                    "Requesting local vector file materialization",
                    file=path.name,
                    attempt=attempt,
                    attempts=_LOCAL_FILE_MATERIALIZE_ATTEMPTS,
                )
                _request_local_file_materialization(path)
                materialized = _is_materialized_local_file(path)
                if materialized:
                    break
            except OSError as exc:
                last_error = exc
            if attempt < _LOCAL_FILE_MATERIALIZE_ATTEMPTS:
                time.sleep(_LOCAL_FILE_MATERIALIZE_RETRY_SECONDS)
        if not materialized:
            if last_error is not None:
                LOGGER.warning(
                    "Local vector file materialization failed",
                    file=str(path),
                    error=str(last_error),
                )
            remaining.append(path)

    if not remaining:
        LOGGER.info(
            "Materialized local Valmer vector cloud placeholders",
            file_count=len(placeholders),
        )
        return

    raise RuntimeError(
        "Local Valmer vector source files could not be materialized by reading them. "
        "OneDrive did not provide the listed files:\n"
        + "\n".join(f"  - {path}" for path in remaining)
    )


def run_vector_update(
    *,
    bucket_name: str | None = None,
    first_loop_count: int = DEFAULT_FIRST_LOOP_COUNT,
    debug_artifact_path: str | None = None,
    local_bucket_path: str | None = None,
    local_bucket_path_env_var: str | None = None,
    source_kind: str = "artifact",
    source_metatables_config_path: str | os.PathLike[str] | Traversable | None = None,
    onedrive_drive_id: str | None = None,
    onedrive_folder_path: str | None = None,
    onedrive_cache_path: str | None = None,
    onedrive_tenant_id_secret_name: str | None = None,
    onedrive_client_id_secret_name: str | None = None,
    onedrive_client_secret_secret_name: str | None = None,
    force_pricing_details_patch: bool | None = None,
    bypass_vector_cursor_filter: bool | None = None,
) -> None:
    """Run the Valmer vector update through the package service boundary."""

    resolved_force_pricing_details_patch = resolve_valmer_force_pricing_details_patch(
        force_pricing_details_patch,
        default=True,
    )
    resolved_bypass_vector_cursor_filter = resolve_valmer_vector_bypass_cursor_filter(
        bypass_vector_cursor_filter,
    )
    resolved_local_bucket_path = _resolve_local_bucket_path(
        local_bucket_path=local_bucket_path,
        local_bucket_path_env_var=local_bucket_path_env_var,
    )
    if debug_artifact_path is not None and resolved_local_bucket_path is not None:
        raise ValueError("--debug-artifact-path and --local-bucket-path are mutually exclusive.")
    source_artifact_path = debug_artifact_path or resolved_local_bucket_path

    source_metatables = None
    if source_kind == "metatable":
        if source_artifact_path is not None:
            raise ValueError(
                "Local artifact paths cannot be used with --source metatable."
            )
        if source_metatables_config_path is None:
            raise ValueError("--source-metatables-config-path is required with --source metatable.")
        source_metatables = load_metatable_sources_config(source_metatables_config_path)
    elif source_kind == "onedrive-graph":
        if source_artifact_path is not None:
            raise ValueError(
                "Local artifact paths cannot be used with --source onedrive-graph."
            )
        if source_metatables_config_path is not None:
            raise ValueError(
                "--source-metatables-config-path cannot be used with --source onedrive-graph."
            )

    LOGGER.info(
        "Starting Valmer vector update",
        source_kind=source_kind,
        bucket_name=resolve_valmer_vector_bucket_name(bucket_name),
        local_source=source_artifact_path,
        force_pricing_details_patch=resolved_force_pricing_details_patch,
        bypass_vector_cursor_filter=resolved_bypass_vector_cursor_filter,
    )
    bootstrap_runtime(seed_static_rows=False)

    # Local source → process files in batches of N per run, so peak memory stays
    # bounded to one batch (~N × 29k rows) instead of concatenating (and re-reading)
    # the whole folder. Each run reads its batch, persists, and frees it;
    # filter_df_by_latest_value keeps it idempotent.
    local_files = _expand_local_files(resolved_local_bucket_path)
    data_node_source_kind = source_kind
    if source_kind == "onedrive-graph":
        from valmer_connectors.services.onedrive_graph import stage_onedrive_vector_files

        latest_time_index = (
            None
            if resolved_bypass_vector_cursor_filter
            else _latest_vector_storage_time_index()
        )
        local_files = stage_onedrive_vector_files(
            latest_time_index=latest_time_index,
            drive_id=onedrive_drive_id,
            folder_path=onedrive_folder_path,
            cache_path=onedrive_cache_path,
            tenant_id_secret_name=onedrive_tenant_id_secret_name,
            client_id_secret_name=onedrive_client_id_secret_name,
            client_secret_secret_name=onedrive_client_secret_secret_name,
        )
        data_node_source_kind = "artifact"
    if local_files is not None:
        if source_kind != "onedrive-graph" and not resolved_bypass_vector_cursor_filter:
            latest_time_index = _latest_vector_storage_time_index()
            local_files = _select_local_vector_files_for_update(
                local_files,
                latest_time_index,
            )
        elif resolved_bypass_vector_cursor_filter:
            LOGGER.info(
                "Bypassing Valmer vector local-file cursor filter for repair run.",
                file_count=len(local_files),
            )
        batch_size = resolve_valmer_vector_file_batch_size()
        total = len(local_files)
        batch_count = (total + batch_size - 1) // batch_size
        if total == 0:
            LOGGER.info("No local Valmer vector files are newer than current storage.")
            return
        LOGGER.info(
            "Processing local vector files in batches",
            file_count=total,
            batch_size=batch_size,
            batches=batch_count,
        )
        run_started = time.monotonic()
        for batch_index, start in enumerate(range(0, total, batch_size), start=1):
            batch = local_files[start : start + batch_size]
            _ensure_local_vector_files_materialized(batch)
            LOGGER.info(
                "Vector batch starting",
                batch=batch_index,
                batches=batch_count,
                files=[path.name for path in batch],
            )
            started = time.monotonic()
            with _debug_artifact_files(os.pathsep.join(str(path) for path in batch)):
                batch_updater = build_import_valmer(
                    bucket_name=bucket_name,
                    source_kind=data_node_source_kind,
                    source_metatables=source_metatables,
                )
                batch_updater.prepare_for_update(
                    force_pricing_update=resolved_force_pricing_details_patch,
                    bypass_vector_cursor_filter=resolved_bypass_vector_cursor_filter,
                )
                batch_updater.run(force_update=True)
            LOGGER.info(
                "Vector batch complete",
                batch=batch_index,
                batches=batch_count,
                elapsed_seconds=round(time.monotonic() - started, 1),
            )
        LOGGER.info(
            "Local vector backfill complete",
            file_count=total,
            batches=batch_count,
            elapsed_seconds=round(time.monotonic() - run_started, 1),
        )
        return

    with _debug_artifact_path(debug_artifact_path):
        updater = build_import_valmer(
            bucket_name=bucket_name,
            source_kind=source_kind,
            source_metatables=source_metatables,
        )
        first_time_update = _is_first_time_update(updater)

        if first_time_update:
            LOGGER.info(
                "No existing vector in backend — running first-time backfill "
                "(each iteration re-reads the source vector, so this can take several minutes)",
                iterations=first_loop_count,
            )
            run_started = time.monotonic()
            for iteration in range(1, first_loop_count + 1):
                LOGGER.info(
                    "Vector backfill iteration starting",
                    iteration=iteration,
                    total=first_loop_count,
                )
                started = time.monotonic()
                loop_updater = build_import_valmer(
                    bucket_name=bucket_name,
                    source_kind=source_kind,
                    source_metatables=source_metatables,
                )
                loop_updater.prepare_for_update(
                    force_pricing_update=resolved_force_pricing_details_patch,
                    bypass_vector_cursor_filter=resolved_bypass_vector_cursor_filter,
                )
                loop_updater.run(force_update=True)
                LOGGER.info(
                    "Vector backfill iteration complete",
                    iteration=iteration,
                    total=first_loop_count,
                    elapsed_seconds=round(time.monotonic() - started, 1),
                )
            LOGGER.info(
                "First-time vector backfill complete",
                iterations=first_loop_count,
                elapsed_seconds=round(time.monotonic() - run_started, 1),
            )
            return

        LOGGER.info("Existing vector found — running incremental update")
        started = time.monotonic()
        updater.prepare_for_update(
            force_pricing_update=resolved_force_pricing_details_patch,
            bypass_vector_cursor_filter=resolved_bypass_vector_cursor_filter,
        )
        updater.run(force_update=True)
        LOGGER.info(
            "Incremental vector update complete",
            elapsed_seconds=round(time.monotonic() - started, 1),
        )
