from __future__ import annotations

import datetime as dt
import logging
import os
import re
import threading
import uuid
from collections.abc import Callable, Iterable, Sequence
from typing import Any

import pandas as pd
from msm.api.base import operation_result_rows
from msm.models import AssetTable
from msm.repositories.base import (
    MarketsRepositoryContext,
    compile_markets_statement,
    execute_markets_operation,
)
from msm_pricing.bootstrap import resolve_pricing_runtime
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable
from sqlalchemy import func, select

from mainsequence.client.metatables import MetaTable, TimeIndexMetaTable
from mainsequence.client.models_helpers import Job, JobRun
from mainsequence.meta_tables import TimeIndexTableRef
from valmer_connectors.control_plane.catalog import (
    DATA_PRODUCTS,
    JOB_ACTIONS,
    JOB_ACTIONS_BY_KEY,
    JOB_ACTIONS_BY_NAME,
    PIPELINE_STAGES,
    VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
    DataProductDefinition,
    JobActionDefinition,
)
from valmer_connectors.control_plane.models import (
    BulkActionExecution,
    BulkActionPreflight,
    CurrentUserResponse,
    LaunchResponse,
    Metric,
    OverviewResponse,
    PageInfo,
    PipelineResponse,
    ResourceCollection,
)

logger = logging.getLogger(__name__)

RUNNING_JOB_STATUSES = frozenset({"PENDING", "QUEUED", "RUNNING"})
FAILURE_JOB_STATUSES = frozenset({"FAILED", "ABORTED"})
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 250
MAX_ASSET_DETAIL_ROWS = 100_000
SAFE_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ControlPlaneError(RuntimeError):
    """Base error for user-safe control-plane failures."""


class ControlPlaneForbidden(ControlPlaneError):
    """Raised when a human caller is not allowed to perform an operation."""


class ControlPlaneConflict(ControlPlaneError):
    """Raised when the requested operation conflicts with active platform state."""


class _TimedCache:
    def __init__(self, ttl_seconds: int = 90) -> None:
        self.ttl = dt.timedelta(seconds=ttl_seconds)
        self._lock = threading.RLock()
        self._values: dict[str, tuple[dt.datetime, Any]] = {}

    def get_or_load(self, key: str, loader: Callable[[], Any]) -> Any:
        now = dt.datetime.now(dt.UTC)
        with self._lock:
            cached = self._values.get(key)
            if cached is not None and now - cached[0] < self.ttl:
                return cached[1]
        value = loader()
        with self._lock:
            self._values[key] = (now, value)
        return value

    def clear(self) -> None:
        with self._lock:
            self._values.clear()


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _operational_error_message(exc: Exception) -> str:
    message = " ".join(str(exc).split())
    lower_message = message.lower()
    if "<!doctype html" in lower_message or "<html" in lower_message:
        status_match = re.match(r"(?P<status>\d{3})\b", message)
        if status_match is not None:
            return f"upstream service returned HTTP {status_match.group('status')}"
        return "upstream service returned an HTML error response"
    if len(message) > 500:
        return f"{message[:497]}..."
    return message


def _flat_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if isinstance(frame.index, pd.MultiIndex) or frame.index.name is not None:
        return frame.reset_index()
    return frame.copy()


def _iso(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, dt.date | dt.datetime | pd.Timestamp):
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC").isoformat()
    return str(value)


def _json_value(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, dt.date | dt.datetime | pd.Timestamp):
        return _iso(value)
    if hasattr(value, "item"):
        return value.item()
    return value


def _canonical_job_run_status(value: object) -> str:
    status = str(value or "").strip().upper()
    if not status:
        raise ControlPlaneError("The platform returned a Job run without a status.")
    if status in {"COMPLETE", "COMPLETED", "SUCCESS", "SUCCESSFUL"}:
        return "SUCCEEDED"
    if status in {"ERROR", "ERRORED"}:
        return "FAILED"
    return status


def _approved_job_definition(job: Job) -> JobActionDefinition | None:
    definition = JOB_ACTIONS_BY_NAME.get(job.name)
    if definition is None or job.execution_path != definition.execution_path:
        return None
    return definition


def _optional_registered_meta_table(identifier: str) -> MetaTable | None:
    matches = list(MetaTable.filter(identifier=identifier, timeout=60))
    if not matches:
        return None
    if len(matches) != 1:
        raise ControlPlaneError(
            f"Expected at most one registered MetaTable for {identifier!r}; "
            f"found {len(matches)}."
        )
    table = matches[0]
    if not table.uid:
        raise ControlPlaneError(f"Registered MetaTable {identifier!r} has no public UID.")
    return table


def _qualified_table_name(table: MetaTable) -> str:
    names = [name for name in (table.physical_schema, table.physical_table_name) if name]
    if not names or any(SAFE_SQL_IDENTIFIER.fullmatch(name) is None for name in names):
        raise ControlPlaneError(
            f"MetaTable {table.identifier!r} returned an unsafe physical table identity."
        )
    return ".".join(f'"{name}"' for name in names)


def _select_meta_table_rows(
    table: MetaTable,
    sql: str,
    *,
    max_rows: int,
) -> list[dict[str, Any]]:
    result = MetaTable.execute_operation(
        {
            "operation": "select",
            "statement": {"sql": sql, "parameters": {}},
            "scope": {
                "tables": [
                    {
                        "meta_table_uid": table.uid,
                        "access": "read",
                    }
                ]
            },
            "limits": {
                "max_rows": max_rows,
                "statement_timeout_ms": 60_000,
            },
        },
        timeout=90,
    )
    if result.get("truncated"):
        raise ControlPlaneError(
            f"MetaTable query for {table.identifier!r} exceeded {max_rows} rows."
        )
    rows = result.get("rows")
    if not isinstance(rows, list):
        raise ControlPlaneError(
            f"MetaTable query for {table.identifier!r} returned no row collection."
        )
    return [dict(row) for row in rows]


def _page(items: list[dict[str, Any]], page_index: int, page_size: int) -> ResourceCollection:
    page_index = max(0, page_index)
    page_size = max(1, min(page_size, MAX_PAGE_SIZE))
    total = len(items)
    start = page_index * page_size
    end = start + page_size
    return ResourceCollection(
        items=items[start:end],
        pageInfo=PageInfo(
            pageIndex=page_index,
            pageSize=page_size,
            totalItems=total,
            hasNextPage=end < total,
            hasPreviousPage=page_index > 0,
        ),
    )


def _search(items: Iterable[dict[str, Any]], query: str | None, fields: Sequence[str]) -> list[dict[str, Any]]:
    if not query:
        return list(items)
    needle = query.casefold().strip()
    return [
        item
        for item in items
        if any(needle in str(item.get(field) or "").casefold() for field in fields)
    ]


def _sort(items: list[dict[str, Any]], ordering: str | None, allowed: set[str]) -> list[dict[str, Any]]:
    if not ordering:
        return items
    descending = ordering.startswith("-")
    key = ordering.removeprefix("-")
    if key not in allowed:
        raise ValueError(f"Unsupported ordering field {ordering!r}.")
    return sorted(items, key=lambda item: (item.get(key) is None, item.get(key)), reverse=descending)


class PlatformControlPlaneGateway:
    """Read and operate on the branch-owned Main Sequence resources."""

    def __init__(self) -> None:
        self._cache = _TimedCache()

    def _latest_observation(self, definition: DataProductDefinition) -> pd.DataFrame:
        def load() -> pd.DataFrame:
            table = self._time_index_tables().get(definition.table_identifier)
            if table is None:
                raise LookupError(
                    "No TimeIndexMetaTable found matching "
                    f"identifier={definition.table_identifier!r}."
                )
            table_ref = TimeIndexTableRef.from_meta_table(table)
            filters = None
            if definition.identity_column and definition.identity_value:
                filters = {definition.identity_column: [definition.identity_value]}
            frame = table_ref.get_last_observation(dimension_filters=filters)
            return _flat_frame(frame)

        return self._cache.get_or_load(f"latest:{definition.key}", load)

    def _time_index_tables(self) -> dict[str, TimeIndexMetaTable]:
        def load() -> dict[str, TimeIndexMetaTable]:
            identifiers = sorted(
                {
                    definition.table_identifier
                    for definition in DATA_PRODUCTS
                    if definition.time_indexed
                }
            )
            tables = TimeIndexMetaTable.filter(
                identifier__in=identifiers,
                timeout=60,
            )
            return {str(table.identifier): table for table in tables}

        return self._cache.get_or_load("time-index-tables", load)

    def environment_name(self) -> str:
        names = {
            str(table.organization_environment_name).strip()
            for table in self._time_index_tables().values()
            if table.organization_environment_name
        }
        if not names:
            raise ControlPlaneError(
                "Registered control-plane TimeIndexMetaTables do not report an "
                "Organization Environment name."
            )
        if len(names) != 1:
            raise ControlPlaneError(
                "Control-plane TimeIndexMetaTables span multiple Organization "
                f"Environments: {', '.join(sorted(names))}."
            )
        return names.pop()

    def _registered_valmer_asset_count(self) -> int:
        def load() -> int:
            table = _optional_registered_meta_table(
                VALMER_ASSET_DETAILS_TABLE_IDENTIFIER
            )
            if table is None:
                raise ControlPlaneError(
                    "The registered Valmer asset-details MetaTable is unavailable."
                )
            rows = _select_meta_table_rows(
                table,
                (
                    "SELECT COUNT(*) AS asset_count "
                    f"FROM {_qualified_table_name(table)}"
                ),
                max_rows=1,
            )
            if len(rows) != 1:
                raise ControlPlaneError(
                    "Valmer asset-details count query returned an unexpected row shape."
                )
            return int(rows[0]["asset_count"])

        return self._cache.get_or_load("registered-assets", load)

    def _current_pricing_details_count(self) -> int:
        def load() -> int:
            runtime = resolve_pricing_runtime(
                models=[AssetCurrentPricingDetailsTable],
                row_model_name="AssetCurrentPricingDetails",
            )
            statement = (
                select(func.count().label("pricing_detail_count"))
                .select_from(AssetCurrentPricingDetailsTable)
            )
            operation = compile_markets_statement(
                statement,
                context=runtime.context,
                operation="select",
                models=[AssetCurrentPricingDetailsTable],
                access="read",
            )
            rows = operation_result_rows(
                execute_markets_operation(operation, context=runtime.context)
            )
            if len(rows) != 1:
                raise RuntimeError(
                    "Current pricing-details count query returned an unexpected row shape."
                )
            return int(rows[0]["pricing_detail_count"])

        return self._cache.get_or_load("current-pricing-details", load)

    def _pricing_target_identifiers(self) -> set[str]:
        def load() -> set[str]:
            runtime = resolve_pricing_runtime(
                models=[AssetTable, AssetCurrentPricingDetailsTable],
                row_model_name="AssetCurrentPricingDetails",
            )
            query_context = MarketsRepositoryContext(
                limits={
                    "max_rows": MAX_ASSET_DETAIL_ROWS,
                    "statement_timeout_ms": 60_000,
                },
                data_source_uid=runtime.context.data_source_uid,
                timeout=90,
                namespace=runtime.context.namespace,
                reserved_policy=runtime.context.reserved_policy,
            )
            statement = (
                select(AssetTable.unique_identifier)
                .select_from(AssetCurrentPricingDetailsTable)
                .join(
                    AssetTable,
                    AssetTable.uid == AssetCurrentPricingDetailsTable.asset_uid,
                )
            )
            operation = compile_markets_statement(
                statement,
                context=query_context,
                operation="select",
                models=[AssetTable, AssetCurrentPricingDetailsTable],
                access="read",
            )
            result = execute_markets_operation(operation, context=query_context)
            if result.get("truncated"):
                raise ControlPlaneError(
                    "Current pricing-details identifier query exceeded "
                    f"{MAX_ASSET_DETAIL_ROWS} rows."
                )
            rows = operation_result_rows(result)
            return {
                str(row["unique_identifier"])
                for row in rows
                if row.get("unique_identifier") not in (None, "")
            }

        return self._cache.get_or_load("pricing-target-identifiers", load)

    def _vector_snapshot(self) -> pd.DataFrame:
        definition = next(product for product in DATA_PRODUCTS if product.key == "valmer-vector")
        frame = self._latest_observation(definition)
        if "asset_identifier" in frame.columns and "unique_identifier" not in frame.columns:
            frame = frame.rename(columns={"asset_identifier": "unique_identifier"})
        return frame

    def _asset_details(self, identifiers: Sequence[str]) -> dict[str, dict[str, Any]]:
        def load() -> dict[str, dict[str, Any]]:
            table = _optional_registered_meta_table(
                VALMER_ASSET_DETAILS_TABLE_IDENTIFIER
            )
            if table is None:
                raise ControlPlaneError(
                    "The registered Valmer asset-details MetaTable is unavailable."
                )
            rows = _select_meta_table_rows(
                table,
                (
                    "SELECT valmer_unique_identifier AS asset_identifier, "
                    "security_type AS valmer_security_type, issuer AS valmer_issuer, "
                    "series AS valmer_series, full_name AS valmer_full_name, "
                    "issue_currency AS valmer_issue_currency, "
                    "sector AS valmer_sector "
                    f"FROM {_qualified_table_name(table)}"
                ),
                max_rows=MAX_ASSET_DETAIL_ROWS,
            )
            allowed = set(identifiers)
            return {
                str(row["asset_identifier"]): {
                    key: _json_value(value) for key, value in row.items()
                }
                for row in rows
                if str(row.get("asset_identifier") or "") in allowed
            }

        cache_key = "asset-details:" + str(hash(tuple(identifiers)))
        return self._cache.get_or_load(cache_key, load)

    def data_products(self) -> list[dict[str, Any]]:
        now = _utc_now()
        return [self._data_product(definition, now=now) for definition in DATA_PRODUCTS]

    def _data_product(
        self,
        definition: DataProductDefinition,
        *,
        now: dt.datetime,
    ) -> dict[str, Any]:
        if not definition.time_indexed:
            if definition.key not in {"valmer-assets", "pricing-details"}:
                return self._failed_product(
                    definition,
                    RuntimeError("Unsupported non-time-indexed data product."),
                )
            try:
                count = (
                    self._registered_valmer_asset_count()
                    if definition.key == "valmer-assets"
                    else self._current_pricing_details_count()
                )
                return {
                    "uid": definition.key,
                    "name": definition.name,
                    "category": definition.category,
                    "status": "healthy",
                    "table_identifier": definition.table_identifier,
                    "cadence": None,
                    "latest_observation": None,
                    "lag_hours": None,
                    "latest_rows": count,
                    "description": definition.description,
                    "error": None,
                }
            except Exception as exc:
                return self._failed_product(definition, exc)

        try:
            table = self._time_index_tables().get(definition.table_identifier)
            if table is None:
                raise LookupError(
                    "No TimeIndexMetaTable found matching "
                    f"identifier={definition.table_identifier!r}."
                )
            frame = self._latest_observation(definition)
            latest_value = frame["time_index"].max() if "time_index" in frame.columns else None
            latest_timestamp = pd.Timestamp(latest_value) if latest_value is not None else None
            if latest_timestamp is not None and latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.tz_localize("UTC")
            lag_hours = (
                (now - latest_timestamp.to_pydatetime()).total_seconds() / 3600
                if latest_timestamp is not None
                else None
            )
            if frame.empty or lag_hours is None:
                status = "warning"
            elif lag_hours > definition.stale_after_hours:
                status = "stale"
            else:
                status = "healthy"
            return {
                "uid": definition.key,
                "name": definition.name,
                "category": definition.category,
                "status": status,
                "table_identifier": definition.table_identifier,
                "cadence": str(table.cadence) if table.cadence is not None else None,
                "latest_observation": _iso(latest_timestamp),
                "lag_hours": round(lag_hours, 1) if lag_hours is not None else None,
                "latest_rows": int(len(frame.index)),
                "description": definition.description,
                "error": None,
            }
        except Exception as exc:
            return self._failed_product(definition, exc)

    @staticmethod
    def _failed_product(definition: DataProductDefinition, exc: Exception) -> dict[str, Any]:
        return {
            "uid": definition.key,
            "name": definition.name,
            "category": definition.category,
            "status": "failed",
            "table_identifier": definition.table_identifier,
            "cadence": None,
            "latest_observation": None,
            "lag_hours": None,
            "latest_rows": None,
            "description": definition.description,
            "error": _operational_error_message(exc),
        }

    def assets(self) -> list[dict[str, Any]]:
        frame = self._vector_snapshot()
        if frame.empty or "unique_identifier" not in frame.columns:
            return []
        identifiers = frame["unique_identifier"].dropna().astype(str).unique().tolist()
        details = self._asset_details(identifiers)
        target_identifiers = self._pricing_target_identifiers()
        latest = frame.sort_values("time_index").drop_duplicates("unique_identifier", keep="last")
        vector_definition = next(
            product for product in DATA_PRODUCTS if product.key == "valmer-vector"
        )
        now = _utc_now()
        result: list[dict[str, Any]] = []
        for row in latest.to_dict(orient="records"):
            identifier = str(row["unique_identifier"])
            detail = details.get(identifier, {})
            observation = pd.Timestamp(row.get("time_index"))
            if observation.tzinfo is None:
                observation = observation.tz_localize("UTC")
            lag_hours = (now - observation.to_pydatetime()).total_seconds() / 3600
            if not detail:
                status = "warning"
            elif lag_hours > vector_definition.stale_after_hours:
                status = "stale"
            else:
                status = "healthy"
            result.append(
                {
                    "uid": identifier,
                    "name": detail.get("valmer_full_name") or identifier,
                    "security_type": detail.get("valmer_security_type"),
                    "issuer": detail.get("valmer_issuer"),
                    "series": detail.get("valmer_series"),
                    "currency": detail.get("valmer_issue_currency"),
                    "sector": detail.get("valmer_sector"),
                    "dirty_price": _json_value(row.get("dirty_price")),
                    "yield_rate": _json_value(row.get("yield_rate")),
                    "duration": _json_value(row.get("duration")),
                    "latest_observation": _iso(observation),
                    "pricing_target": identifier in target_identifiers,
                    "status": status,
                }
            )
        return result

    def jobs(self) -> list[dict[str, Any]]:
        jobs = self.required_jobs()
        job_uids = [str(job.uid) for job in jobs if job.uid]
        runs_by_job_uid = self.job_runs_for_uids(job_uids)
        result: list[dict[str, Any]] = []
        for job in jobs:
            if not job.uid:
                continue
            definition = _approved_job_definition(job)
            runs = runs_by_job_uid.get(str(job.uid), [])
            latest = runs[0] if runs else None
            latest_status = (
                _canonical_job_run_status(getattr(latest, "status", None))
                if latest
                else None
            )
            status = (
                "not-run"
                if latest_status is None
                else "running"
                if latest_status in RUNNING_JOB_STATUSES
                else latest_status.lower()
            )
            schedule = job.task_schedule.model_dump(mode="json") if job.task_schedule else None
            result.append(
                {
                    "uid": str(job.uid),
                    "key": definition.key if definition else None,
                    "name": job.name,
                    "description": definition.description if definition else None,
                    "status": status,
                    "last_run_status": latest_status,
                    "last_run_at": _iso(getattr(latest, "execution_start", None)) if latest else None,
                    "execution_path": job.execution_path,
                    "schedule": schedule,
                    "image_status": job.image_status,
                    "automatic_deployment": job.automatic_deployment,
                    "dependencies": list(definition.dependencies) if definition else [],
                    "approved_action": definition is not None,
                }
            )
        return result

    @staticmethod
    def required_jobs() -> list[Job]:
        try:
            return list(
                Job.filter(
                    name__in=[definition.job_name for definition in JOB_ACTIONS],
                    timeout=60,
                )
            )
        except Exception as exc:
            raise ControlPlaneError(
                "The required platform Jobs query failed: "
                f"{_operational_error_message(exc)}"
            ) from exc

    @staticmethod
    def job_runs_for(job_uid: str) -> list[JobRun]:
        return PlatformControlPlaneGateway.job_runs_for_uids([job_uid]).get(job_uid, [])

    @staticmethod
    def job_runs_for_uids(job_uids: Sequence[str]) -> dict[str, list[JobRun]]:
        normalized_job_uids = list(dict.fromkeys(str(uid) for uid in job_uids if uid))
        if not normalized_job_uids:
            return {}
        try:
            runs = list(JobRun.filter(job__uid__in=normalized_job_uids, timeout=60))
        except Exception as exc:
            raise ControlPlaneError(
                "The platform JobRun query failed: "
                f"{_operational_error_message(exc)}"
            ) from exc
        runs_by_job_uid: dict[str, list[JobRun]] = {
            job_uid: [] for job_uid in normalized_job_uids
        }
        for run in runs:
            run_job_uid = str(run.job_uid or "")
            if run_job_uid in runs_by_job_uid:
                runs_by_job_uid[run_job_uid].append(run)
        for job_runs in runs_by_job_uid.values():
            job_runs.sort(
                key=lambda run: getattr(run, "execution_start", None)
                or dt.datetime.min.replace(tzinfo=dt.UTC),
                reverse=True,
            )
        return runs_by_job_uid

    def job_runs(self) -> list[dict[str, Any]]:
        jobs = self.required_jobs()
        runs_by_job_uid = self.job_runs_for_uids(
            [str(job.uid) for job in jobs if job.uid]
        )
        result: list[dict[str, Any]] = []
        for job in jobs:
            if not job.uid:
                continue
            for run in runs_by_job_uid.get(str(job.uid), []):
                if not run.uid:
                    raise ControlPlaneError(
                        f"Job {job.name!r} returned a run without a public UID."
                    )
                result.append(
                    {
                        "uid": str(run.uid),
                        "job_uid": str(run.job_uid or job.uid),
                        "job_name": run.job_name or job.name,
                        "status": _canonical_job_run_status(getattr(run, "status", None)),
                        "execution_start": _iso(run.execution_start),
                        "execution_end": _iso(getattr(run, "execution_end", None)),
                        "commit_hash": getattr(run, "commit_hash", None),
                        "triggered_by": getattr(run, "triggered_by", None),
                        "logs_url": getattr(getattr(run, "observability", None), "application_logs_url", None),
                        "resource_usage_url": getattr(
                            getattr(run, "observability", None), "resource_usage_url", None
                        ),
                    }
                )
        return sorted(result, key=lambda item: item.get("execution_start") or "", reverse=True)

    def run_job(self, job_uid: str) -> tuple[Job, dict[str, Any]]:
        matches = list(Job.filter(uid=job_uid, timeout=60))
        if len(matches) != 1:
            raise ControlPlaneConflict("The selected Job is no longer available in this branch.")
        job = matches[0]
        if _approved_job_definition(job) is None:
            raise ControlPlaneConflict(
                "The selected Job no longer matches the approved control-plane catalog."
            )
        runs = self.job_runs_for(job_uid)
        if runs and _canonical_job_run_status(runs[0].status) in RUNNING_JOB_STATUSES:
            raise ControlPlaneConflict(
                "The selected Job already has a pending or running execution."
            )
        if job.image_status.lower() != "ready":
            raise ControlPlaneConflict("The selected Job image is not ready.")
        result = job.run_job(timeout=60)
        self._cache.clear()
        return job, result


class ControlPlaneService:
    def __init__(
        self,
        gateway: PlatformControlPlaneGateway | None = None,
        *,
        operator_uids: Iterable[str] | None = None,
        now: Callable[[], dt.datetime] = _utc_now,
    ) -> None:
        self.gateway = gateway or PlatformControlPlaneGateway()
        configured = operator_uids
        if configured is None:
            configured = os.getenv("VALMER_CONTROL_PLANE_OPERATOR_UIDS", "").split(",")
        self.operator_uids = frozenset(uid.strip() for uid in configured if uid.strip())
        self.now = now

    def current_user(self, user_uid: str) -> CurrentUserResponse:
        return CurrentUserResponse(
            uid=user_uid,
            role="operator" if self.is_operator(user_uid) else "viewer",
        )

    def is_operator(self, user_uid: str) -> bool:
        return user_uid in self.operator_uids

    def overview(self) -> OverviewResponse:
        products = self.gateway.data_products()
        failures = [
            f"{item['name']}: {item['error']}"
            for item in products
            if item["status"] == "failed"
        ]
        vector = next((item for item in products if item["uid"] == "valmer-vector"), None)
        assets = next((item for item in products if item["uid"] == "valmer-assets"), None)
        pricing_details = next(
            (item for item in products if item["uid"] == "pricing-details"),
            None,
        )
        registered_asset_count = assets.get("latest_rows") if assets else None
        pricing_detail_count = pricing_details.get("latest_rows") if pricing_details else None
        latest_vector_observation = vector.get("latest_observation") if vector else None
        latest_vector_display = (
            pd.Timestamp(latest_vector_observation).strftime("%Y-%m-%d")
            if latest_vector_observation
            else "No persisted observation"
        )
        curves = [item for item in products if item["category"] == "curve"]
        try:
            environment = self.gateway.environment_name()
        except Exception as exc:
            environment = None
            failures.append(f"Environment: {_operational_error_message(exc)}")
        try:
            jobs = self.gateway.jobs()
            jobs_loaded = True
        except Exception as exc:
            jobs = []
            jobs_loaded = False
            failures.append(f"Jobs: {_operational_error_message(exc)}")
        approved_jobs = [item for item in jobs if item["approved_action"]]
        available_action_keys = {
            item["key"] for item in approved_jobs if item.get("key") is not None
        }
        missing_actions = [
            action for action in JOB_ACTIONS if action.key not in available_action_keys
        ]
        if jobs_loaded and missing_actions:
            failures.append(
                "Jobs: "
                f"{len(approved_jobs)} of {len(JOB_ACTIONS)} approved control-plane "
                "Jobs are available in this branch."
            )
        running = sum(item["status"] == "running" for item in approved_jobs)
        failed = sum(
            item["last_run_status"] in FAILURE_JOB_STATUSES
            for item in approved_jobs
            if item["last_run_status"] is not None
        )
        unhealthy_products = sum(
            item["status"] in {"stale", "failed", "warning"} for item in products
        )
        failed_products = any(item["status"] == "failed" for item in products)
        status = (
            "failed"
            if failures or failed
            else "warning"
            if unhealthy_products
            else "healthy"
        )
        curve_status = (
            "failed"
            if not curves or any(item["status"] == "failed" for item in curves)
            else "healthy"
            if all(item["status"] == "healthy" for item in curves)
            else "warning"
        )
        jobs_available_status = (
            "healthy" if jobs_loaded and not missing_actions else "failed"
        )
        metrics = [
            Metric(
                id="registered-assets",
                label="Registered Valmer assets",
                value=registered_asset_count,
                display=(
                    str(registered_asset_count)
                    if registered_asset_count is not None
                    else "Unavailable"
                ),
                detail=(
                    "No Valmer assets are registered yet."
                    if registered_asset_count == 0
                    else (
                        "Valmer asset-detail rows linked one-to-one to canonical "
                        "Asset identities."
                    )
                ),
                status=assets["status"] if assets else "failed",
            ),
            Metric(
                id="current-pricing-details",
                label="Current pricing details",
                value=pricing_detail_count,
                display=(
                    str(pricing_detail_count)
                    if pricing_detail_count is not None
                    else "Unavailable"
                ),
                detail="Rows in the current persisted Asset pricing-details relation.",
                status=pricing_details["status"] if pricing_details else "failed",
            ),
            Metric(
                id="latest-vector-observation",
                label="Latest vector observation",
                value=latest_vector_observation,
                display=latest_vector_display,
                detail="Most recent persisted Valmer vector time index.",
                status=vector["status"] if vector else "failed",
            ),
            Metric(
                id="healthy-curves",
                label="Healthy curves",
                value=sum(item["status"] == "healthy" for item in curves),
                display=f"{sum(item['status'] == 'healthy' for item in curves)}/{len(curves)}",
                detail="Current TIIE, SOFR, XCCY and government curve products.",
                status=curve_status,
            ),
            Metric(
                id="available-jobs",
                label="Available Jobs",
                value=len(approved_jobs) if jobs_loaded else None,
                display=(
                    f"{len(approved_jobs)}/{len(JOB_ACTIONS)}"
                    if jobs_loaded
                    else "Unavailable"
                ),
                detail="Approved control-plane Jobs currently registered in this branch.",
                status=jobs_available_status,
            ),
            Metric(
                id="running-jobs",
                label="Running jobs",
                value=running if jobs_loaded else None,
                display=str(running) if jobs_loaded else "Unavailable",
                detail="Branch Jobs currently pending or running.",
                status=(
                    "running"
                    if running
                    else "healthy"
                    if jobs_loaded and not missing_actions
                    else "failed"
                ),
            ),
            Metric(
                id="failed-jobs",
                label="Failed jobs",
                value=failed if jobs_loaded else None,
                display=str(failed) if jobs_loaded else "Unavailable",
                detail="Branch Jobs whose latest run failed or was aborted.",
                status="failed" if failed or missing_actions else "healthy",
            ),
            Metric(
                id="unhealthy-products",
                label="Unhealthy data products",
                value=unhealthy_products,
                display=str(unhealthy_products),
                detail="Products that are stale, unavailable, or have no persisted observation.",
                status=(
                    "failed"
                    if failed_products
                    else "warning"
                    if unhealthy_products
                    else "healthy"
                ),
            ),
        ]
        return OverviewResponse(
            generated_at=self.now(),
            status=status,
            environment=environment,
            metrics=metrics,
            failures=failures,
        )

    def data_products(
        self,
        *,
        page_index: int,
        page_size: int,
        search: str | None,
        status: str | None,
        category: str | None,
        ordering: str | None,
    ) -> ResourceCollection:
        items = self.gateway.data_products()
        if status:
            items = [item for item in items if item["status"] == status]
        if category:
            items = [item for item in items if item["category"] == category]
        items = _search(items, search, ("uid", "name", "table_identifier", "description"))
        items = _sort(items, ordering, {"name", "status", "latest_observation", "lag_hours"})
        return _page(items, page_index, page_size)

    def assets(
        self,
        *,
        page_index: int,
        page_size: int,
        search: str | None,
        pricing_target: bool | None,
        ordering: str | None,
    ) -> ResourceCollection:
        items = self.gateway.assets()
        if pricing_target is not None:
            items = [item for item in items if item["pricing_target"] is pricing_target]
        items = _search(items, search, ("uid", "name", "issuer", "series", "sector"))
        items = _sort(items, ordering, {"name", "issuer", "dirty_price", "yield_rate", "latest_observation"})
        return _page(items, page_index, page_size)

    def jobs(
        self,
        *,
        page_index: int,
        page_size: int,
        search: str | None,
        status: str | None,
        ordering: str | None,
    ) -> ResourceCollection:
        items = self.gateway.jobs()
        if status:
            items = [item for item in items if item["status"] == status]
        items = _search(items, search, ("uid", "key", "name", "description"))
        items = _sort(items, ordering, {"name", "status", "last_run_at"})
        return _page(items, page_index, page_size)

    def job_runs(
        self,
        *,
        page_index: int,
        page_size: int,
        search: str | None,
        status: str | None,
        ordering: str | None,
    ) -> ResourceCollection:
        items = self.gateway.job_runs()
        if status:
            items = [item for item in items if item["status"] == status]
        items = _search(items, search, ("uid", "job_uid", "job_name", "commit_hash"))
        items = _sort(items, ordering, {"job_name", "status", "execution_start", "execution_end"})
        return _page(items, page_index, page_size)

    def pipeline(self) -> PipelineResponse:
        jobs_by_key = {
            item["key"]: item
            for item in self.gateway.jobs()
            if item["approved_action"] and item.get("key") is not None
        }
        stages: list[dict[str, Any]] = []
        for stage in PIPELINE_STAGES:
            actions: list[dict[str, Any]] = []
            for action_key in stage["actions"]:
                definition = JOB_ACTIONS_BY_KEY[str(action_key)]
                job = jobs_by_key.get(definition.key)
                actions.append(
                    {
                        "key": definition.key,
                        "name": definition.job_name,
                        "description": definition.description,
                        "execution_path": definition.execution_path,
                        "dependencies": list(definition.dependencies),
                        "available": job is not None,
                        "job_uid": job["uid"] if job else None,
                        "status": job["status"] if job else "missing",
                        "last_run_status": job["last_run_status"] if job else None,
                        "last_run_at": job["last_run_at"] if job else None,
                        "image_status": job["image_status"] if job else None,
                        "automatic_deployment": (
                            job["automatic_deployment"] if job else None
                        ),
                    }
                )
            stages.append(
                {
                    "id": stage["id"],
                    "label": stage["label"],
                    "description": stage["description"],
                    "actions": actions,
                }
            )
        return PipelineResponse(
            stages=stages,
            action_dependencies={action.key: list(action.dependencies) for action in JOB_ACTIONS},
        )

    def preflight(self, user_uid: str, request: BulkActionExecution) -> BulkActionPreflight:
        blockers: list[str] = []
        warnings: list[str] = []
        if not self.is_operator(user_uid):
            blockers.append("Your account has viewer access and cannot launch production Jobs.")
        if request.selection.mode != "explicit":
            blockers.append("Job launch supports explicit selection only.")
            selected_uids: list[str] = []
        else:
            selected_uids = request.selection.uids
        if len(selected_uids) != 1:
            blockers.append("Select exactly one Job for each launch request.")
        selected = None
        if len(selected_uids) == 1:
            jobs = self.gateway.jobs()
            selected = next((job for job in jobs if job["uid"] == selected_uids[0]), None)
            if selected is None:
                blockers.append("The selected Job is not available in this branch.")
            elif not selected["approved_action"]:
                blockers.append("The selected Job is not in the approved control-plane catalog.")
            elif selected["status"] == "running":
                blockers.append("The selected Job already has a pending or running execution.")
            elif selected["image_status"] not in {"ready", "READY"}:
                blockers.append("The selected Job image is not ready.")
            if selected and selected["dependencies"]:
                warnings.append(
                    "This Job depends on: " + ", ".join(selected["dependencies"]) + "."
                )
        detail = "The Job can be launched." if not blockers else "The Job launch is blocked."
        return BulkActionPreflight(
            allowed=not blockers,
            detail=detail,
            matched_count=len(selected_uids),
            blockers=blockers,
            warnings=warnings,
        )

    def launch(self, user_uid: str, request: BulkActionExecution) -> LaunchResponse:
        if not self.is_operator(user_uid):
            raise ControlPlaneForbidden(
                "Your account has viewer access and cannot launch production Jobs."
            )
        preflight = self.preflight(user_uid, request)
        if not preflight.allowed:
            raise ControlPlaneConflict(preflight.detail or "The Job launch is blocked.")
        if request.selection.mode != "explicit":
            raise ControlPlaneConflict("Job launch supports explicit selection only.")
        job_uid = request.selection.uids[0]
        request_uid = str(uuid.uuid4())
        requested_at = self.now()
        job, result = self.gateway.run_job(job_uid)
        run_uid = result.get("uid") or result.get("job_run_uid")
        if not run_uid:
            raise ControlPlaneError(
                "The platform accepted the Job launch without returning a Job run UID."
            )
        launch_status = result.get("status") or "ACCEPTED"
        logger.info(
            "control_plane_job_launch",
            extra={
                "control_plane_request_uid": request_uid,
                "requested_by_user_uid": user_uid,
                "job_uid": job_uid,
                "job_name": job.name,
                "job_run_uid": run_uid,
            },
        )
        return LaunchResponse(
            request_uid=request_uid,
            requested_by_user_uid=user_uid,
            job_uid=job_uid,
            job_run_uid=str(run_uid),
            status=_canonical_job_run_status(launch_status),
            requested_at=requested_at,
        )


def data_product_discovery() -> dict[str, Any]:
    return {
        "contract": "command-center.resource_discovery@v1",
        "resource": {
            "id": "data-products",
            "label": "Data products",
            "item_label": "data product",
            "identity": {"fields": ["uid"]},
        },
        "list": {
            "controls": {
                "search": {
                    "placeholder": "Search data products",
                    "fields": ["uid", "name", "table_identifier", "description"],
                },
                "filters": [
                    {
                        "key": "status",
                        "label": "Status",
                        "type": "select",
                        "options": [
                            {"value": value, "label": value.title()}
                            for value in ("healthy", "warning", "stale", "failed")
                        ],
                    },
                    {
                        "key": "category",
                        "label": "Category",
                        "type": "select",
                        "options": [
                            {"value": value, "label": value.replace("-", " ").title()}
                            for value in ("registry", "source", "market-data", "curve")
                        ],
                    },
                ],
                "ordering": ["name", "status", "latest_observation", "lag_hours"],
            },
            "columns": [
                {"id": "name", "header": "Data product", "default_visible": True, "hideable": False, "sortable_key": "name", "importance": "primary"},
                {"id": "status", "header": "Status", "default_visible": True, "hideable": False, "sortable_key": "status", "filter_key": "status", "importance": "primary"},
                {"id": "category", "header": "Category", "value_path": "category", "data_type": "badge", "default_visible": True, "hideable": True, "filter_key": "category", "importance": "secondary"},
                {"id": "latest-observation", "header": "Latest observation", "value_path": "latest_observation", "data_type": "datetime", "default_visible": True, "hideable": True, "sortable_key": "latest_observation", "importance": "secondary"},
                {"id": "latest-rows", "header": "Current rows", "value_path": "latest_rows", "data_type": "number", "default_visible": True, "hideable": True, "importance": "secondary", "align": "end"},
                {"id": "table", "header": "Table", "value_path": "table_identifier", "data_type": "text", "default_visible": False, "hideable": True, "importance": "tertiary"},
            ],
        },
        "bulk_actions": [],
    }


def asset_discovery() -> dict[str, Any]:
    return {
        "contract": "command-center.resource_discovery@v1",
        "resource": {"id": "assets", "label": "Assets", "item_label": "asset", "identity": {"fields": ["uid"]}},
        "list": {
            "controls": {
                "search": {"placeholder": "Search assets", "fields": ["uid", "name", "issuer", "series", "sector"]},
                "filters": [{"key": "pricing_target", "label": "Pricing target", "type": "boolean"}],
                "ordering": ["name", "issuer", "dirty_price", "yield_rate", "latest_observation"],
            },
            "columns": [
                {"id": "name", "header": "Asset", "default_visible": True, "hideable": False, "sortable_key": "name", "importance": "primary"},
                {"id": "status", "header": "Status", "default_visible": True, "hideable": False, "importance": "primary"},
                {"id": "issuer", "header": "Issuer", "value_path": "issuer", "data_type": "text", "default_visible": True, "hideable": True, "sortable_key": "issuer", "importance": "secondary"},
                {"id": "dirty-price", "header": "Dirty price", "value_path": "dirty_price", "data_type": "number", "default_visible": True, "hideable": True, "sortable_key": "dirty_price", "importance": "secondary", "align": "end"},
                {"id": "yield", "header": "Yield", "value_path": "yield_rate", "data_type": "number", "default_visible": True, "hideable": True, "sortable_key": "yield_rate", "importance": "secondary", "align": "end"},
                {"id": "latest-observation", "header": "Latest observation", "value_path": "latest_observation", "data_type": "datetime", "default_visible": True, "hideable": True, "sortable_key": "latest_observation", "importance": "secondary"},
            ],
        },
        "bulk_actions": [],
    }


def job_discovery(*, can_operate: bool) -> dict[str, Any]:
    actions = []
    if can_operate:
        actions.append(
            {
                "id": "run",
                "label": "Run Job",
                "endpoint": "/api/v1/control-plane/jobs/actions/run",
                "preflight_endpoint": "/api/v1/control-plane/jobs/actions/run/preflight",
                "method": "POST",
                "tone": "primary",
                "selection_modes": ["explicit"],
                "confirmation": {
                    "title": "Run production Job",
                    "word": "RUN JOB",
                    "button_label": "Run Job",
                    "warning": "This starts a production workload using the Job's pinned image and configuration.",
                },
                "options": [],
            }
        )
    return {
        "contract": "command-center.resource_discovery@v1",
        "resource": {"id": "jobs", "label": "Jobs", "item_label": "job", "identity": {"fields": ["uid"]}},
        "list": {
            "controls": {
                "search": {"placeholder": "Search Jobs", "fields": ["uid", "key", "name", "description"]},
                "filters": [{"key": "status", "label": "Status", "type": "select", "options": [{"value": value, "label": value.title()} for value in ("not-run", "running", "succeeded", "failed", "aborted")]}],
                "ordering": ["name", "status", "last_run_at"],
            },
            "columns": [
                {"id": "name", "header": "Job", "default_visible": True, "hideable": False, "sortable_key": "name", "importance": "primary"},
                {"id": "status", "header": "Status", "default_visible": True, "hideable": False, "sortable_key": "status", "filter_key": "status", "importance": "primary"},
                {"id": "last-run", "header": "Last run", "value_path": "last_run_at", "data_type": "datetime", "default_visible": True, "hideable": True, "sortable_key": "last_run_at", "importance": "secondary"},
                {"id": "execution-path", "header": "Execution path", "value_path": "execution_path", "data_type": "text", "default_visible": True, "hideable": True, "importance": "secondary"},
                {"id": "dependencies", "header": "Dependencies", "value_path": "dependencies", "data_type": "list", "default_visible": False, "hideable": True, "importance": "tertiary"},
            ],
        },
        "bulk_actions": actions,
    }


def job_run_discovery() -> dict[str, Any]:
    return {
        "contract": "command-center.resource_discovery@v1",
        "resource": {"id": "job-runs", "label": "Job runs", "item_label": "job run", "identity": {"fields": ["uid"]}},
        "list": {
            "controls": {
                "search": {"placeholder": "Search Job runs", "fields": ["uid", "job_uid", "job_name", "commit_hash"]},
                "filters": [{"key": "status", "label": "Status", "type": "select", "options": [{"value": value, "label": value.title()} for value in ("PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "ABORTED")]}],
                "ordering": ["job_name", "status", "execution_start", "execution_end"],
            },
            "columns": [
                {"id": "job", "header": "Job", "value_path": "job_name", "data_type": "text", "default_visible": True, "hideable": False, "sortable_key": "job_name", "importance": "primary"},
                {"id": "status", "header": "Status", "default_visible": True, "hideable": False, "sortable_key": "status", "filter_key": "status", "importance": "primary"},
                {"id": "started", "header": "Started", "value_path": "execution_start", "data_type": "datetime", "default_visible": True, "hideable": True, "sortable_key": "execution_start", "importance": "secondary"},
                {"id": "finished", "header": "Finished", "value_path": "execution_end", "data_type": "datetime", "default_visible": True, "hideable": True, "sortable_key": "execution_end", "importance": "secondary"},
                {"id": "commit", "header": "Commit", "value_path": "commit_hash", "data_type": "text", "default_visible": False, "hideable": True, "importance": "tertiary"},
            ],
        },
        "bulk_actions": [],
    }


__all__ = [
    "ControlPlaneConflict",
    "ControlPlaneError",
    "ControlPlaneForbidden",
    "ControlPlaneService",
    "PlatformControlPlaneGateway",
    "asset_discovery",
    "data_product_discovery",
    "job_discovery",
    "job_run_discovery",
]
