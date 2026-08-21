from __future__ import annotations

import datetime as dt
import json
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import structlog
from msm.api.base import operation_result_rows
from msm.models import AssetTable, IndexTable
from msm.repositories.base import compile_markets_statement, execute_markets_operation
from msm.repositories.crud import bulk_upsert_model
from msm_pricing.api.pricing_details import (
    AssetCurrentPricingDetails,
    AssetPricingDetails,
)
from msm_pricing.data_nodes.pricing_details.storage import AssetPricingDetailsStorage
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable
from sqlalchemy import String, and_, cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB

from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.instruments.curve_bootstrap import (
    CETE_28_INDEX_UNIQUE_IDENTIFIER,
    CETE_91_INDEX_UNIQUE_IDENTIFIER,
    CETE_182_INDEX_UNIQUE_IDENTIFIER,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    TIIE_91_INDEX_UNIQUE_IDENTIFIER,
    TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
)
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.settings import (
    SUBYACENTE_TO_INDEX_MAP,
    resolve_valmer_pricing_details_batch_size,
)

LOGGER = structlog.get_logger(__name__)

RepairScope = Literal["current", "timestamped"]

SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER = "MXN_GOVERNMENT_BOND"
MEXICO_CALENDAR_JSON = {"name": "Mexican stock exchange"}
BAD_CALENDAR_TOKENS = frozenset(
    {
        "Mexico-BMV",
        "Mexico/BMV",
        "Mexico_BMV",
        "MEXICO-BMV",
        "MEXICO/BMV",
    }
)
STALE_CALENDAR_OBJECT_TOKENS = frozenset({*BAD_CALENDAR_TOKENS, "Mexico"})
CALENDAR_TOKEN_KEYS = ("name", "class", "calendar_class", "calendar_name", "calendar_code")
UID_FIELD_NAMES = frozenset(
    {
        "benchmark_rate_index_uid",
        "floating_rate_index_uid",
        "float_leg_index_uid",
    }
)
STALE_INDEX_NAME_FIELD_TARGETS = {
    "benchmark_rate_index_name": "benchmark_rate_index_uid",
    "floating_rate_index_name": "floating_rate_index_uid",
    "float_leg_index_name": "float_leg_index_uid",
}
LEGACY_INDEX_RELATION_FIELD_TARGETS = {
    "benchmark_pricing_details": "benchmark_rate_index_uid",
    "benchmark_rate_index": "benchmark_rate_index_uid",
    "floating_rate_index": "floating_rate_index_uid",
    "float_leg_index": "float_leg_index_uid",
}
REFERENCE_INDEX_IDENTIFIERS = frozenset(
    {
        CETE_28_INDEX_UNIQUE_IDENTIFIER,
        CETE_91_INDEX_UNIQUE_IDENTIFIER,
        CETE_182_INDEX_UNIQUE_IDENTIFIER,
        TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    }
)


@dataclass(frozen=True)
class PersistedPricingDetailsRow:
    scope: RepairScope
    asset_uid: uuid.UUID
    asset_identifier: str
    time_index: dt.datetime
    instrument_type: str
    instrument_dump: dict[str, Any]
    serialization_format: str
    pricing_package_version: str | None
    source: str | None
    metadata_json: dict[str, Any] | None
    valmer_underlying: str | None
    valmer_security_type: str | None
    valmer_issuer: str | None


@dataclass(frozen=True)
class PricingDetailsRepairPlan:
    row: PersistedPricingDetailsRow
    patched_instrument_dump: dict[str, Any]
    changes: tuple[str, ...]

    def upsert_values(self) -> dict[str, Any]:
        common = {
            "instrument_type": self.row.instrument_type,
            "instrument_dump": self.patched_instrument_dump,
            "serialization_format": self.row.serialization_format,
            "pricing_package_version": self.row.pricing_package_version,
            "source": self.row.source,
            "metadata_json": self.row.metadata_json,
        }
        if self.row.scope == "current":
            return {
                "asset_uid": self.row.asset_uid,
                "pricing_details_date": self.row.time_index,
                **common,
            }
        return {
            "time_index": self.row.time_index,
            "asset_identifier": self.row.asset_identifier,
            **common,
        }


@dataclass(frozen=True)
class PricingDetailsRepairFailure:
    scope: RepairScope
    asset_identifier: str
    time_index: str
    reason: str

    def as_dict(self) -> dict[str, str]:
        return {
            "scope": self.scope,
            "asset_identifier": self.asset_identifier,
            "time_index": self.time_index,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PricingDetailsRepairSummary:
    applied: bool
    scanned_rows: int
    candidate_rows: int
    patched_current_rows: int
    patched_timestamped_rows: int
    failures: tuple[PricingDetailsRepairFailure, ...] = ()
    changes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "scanned_rows": self.scanned_rows,
            "candidate_rows": self.candidate_rows,
            "patched_current_rows": self.patched_current_rows,
            "patched_timestamped_rows": self.patched_timestamped_rows,
            "failures": [failure.as_dict() for failure in self.failures],
            "changes": dict(sorted(self.changes.items())),
        }


class PricingDetailsRepairError(RuntimeError):
    """Raised when persisted Valmer pricing details cannot be repaired safely."""


def repair_valmer_asset_pricing_details(
    *,
    apply: bool = False,
    include_history: bool = True,
    asset_identifiers: Sequence[str] | None = None,
    page_size: int = 1000,
    limit: int | None = None,
    batch_size: int | None = None,
    verify: bool = True,
    allow_unresolved: bool = False,
    logger: Any = LOGGER,
) -> PricingDetailsRepairSummary:
    """Repair persisted Valmer asset pricing-detail payloads in place.

    The repair is targeted: rows are discovered from the pricing-detail tables
    with a bad-signature query and then written back with the same persistence
    keys. No Valmer vector files are replayed.
    """

    if page_size < 1:
        raise ValueError("page_size must be positive.")
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive when provided.")

    bootstrap_runtime(seed_static_rows=False)
    resolved_batch_size = resolve_valmer_pricing_details_batch_size(batch_size)
    index_uid_by_identifier = _load_index_uid_by_identifier()

    scopes: tuple[RepairScope, ...] = (
        ("current", "timestamped") if include_history else ("current",)
    )
    rows: list[PersistedPricingDetailsRow] = []
    for scope in scopes:
        rows.extend(
            _query_repair_rows(
                scope=scope,
                index_uid_by_identifier=index_uid_by_identifier,
                asset_identifiers=asset_identifiers,
                page_size=page_size,
                limit=limit,
            )
        )

    plans: list[PricingDetailsRepairPlan] = []
    failures: list[PricingDetailsRepairFailure] = []
    change_counter: Counter[str] = Counter()
    for row in rows:
        try:
            patched_dump, changes = patch_instrument_dump(
                row,
                index_uid_by_identifier=index_uid_by_identifier,
            )
        except Exception as exc:
            failures.append(_failure_from_row(row, str(exc)))
            continue
        if not changes:
            continue
        plans.append(
            PricingDetailsRepairPlan(
                row=row,
                patched_instrument_dump=patched_dump,
                changes=tuple(changes),
            )
        )
        change_counter.update(changes)

    if failures and apply and not allow_unresolved:
        raise PricingDetailsRepairError(_format_failure_message(failures))

    current_plans = [plan for plan in plans if plan.row.scope == "current"]
    timestamped_plans = [plan for plan in plans if plan.row.scope == "timestamped"]

    logger.info(
        "Valmer pricing-details repair prepared",
        apply=apply,
        scanned_rows=len(rows),
        candidate_rows=len(plans),
        current_rows=len(current_plans),
        timestamped_rows=len(timestamped_plans),
        failures=len(failures),
    )

    if apply and plans:
        _bulk_upsert_repair_plans(
            current_plans=current_plans,
            timestamped_plans=timestamped_plans,
            batch_size=resolved_batch_size,
        )
        if verify:
            remaining = []
            for scope in scopes:
                remaining.extend(
                    _query_repair_rows(
                        scope=scope,
                        index_uid_by_identifier=index_uid_by_identifier,
                        asset_identifiers=asset_identifiers,
                        page_size=page_size,
                        limit=limit,
                    )
                )
            remaining_bad = _remaining_repair_required(
                remaining,
                index_uid_by_identifier=index_uid_by_identifier,
            )
            if remaining_bad:
                sample = ", ".join(
                    f"{row.scope}:{row.asset_identifier}@{row.time_index.isoformat()}"
                    for row in remaining_bad[:10]
                )
                raise PricingDetailsRepairError(
                    "Valmer pricing-details repair left stale rows after apply: "
                    f"{sample}"
                )

    return PricingDetailsRepairSummary(
        applied=apply,
        scanned_rows=len(rows),
        candidate_rows=len(plans),
        patched_current_rows=len(current_plans) if apply else 0,
        patched_timestamped_rows=len(timestamped_plans) if apply else 0,
        failures=tuple(failures),
        changes=dict(change_counter),
    )


def patch_instrument_dump(
    row: PersistedPricingDetailsRow,
    *,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
) -> tuple[dict[str, Any], list[str]]:
    """Return a repaired copy of one serialized instrument payload."""

    patched = json.loads(json.dumps(row.instrument_dump, default=str))
    changes: list[str] = []
    _patch_mapping(
        patched,
        row=row,
        index_uid_by_identifier=index_uid_by_identifier,
        path=(),
        changes=changes,
    )
    return patched, changes


def _remaining_repair_required(
    rows: Sequence[PersistedPricingDetailsRow],
    *,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
) -> list[PersistedPricingDetailsRow]:
    remaining: list[PersistedPricingDetailsRow] = []
    for row in rows:
        try:
            _, changes = patch_instrument_dump(
                row,
                index_uid_by_identifier=index_uid_by_identifier,
            )
        except Exception:
            remaining.append(row)
            continue
        if changes:
            remaining.append(row)
    return remaining


def _patch_mapping(
    payload: dict[str, Any],
    *,
    row: PersistedPricingDetailsRow,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
    path: tuple[str, ...],
    changes: list[str],
) -> None:
    for legacy_field, uid_field in {
        **STALE_INDEX_NAME_FIELD_TARGETS,
        **LEGACY_INDEX_RELATION_FIELD_TARGETS,
    }.items():
        if legacy_field not in payload:
            continue
        raw_identifier = payload.pop(legacy_field)
        replacement_uid = _resolve_replacement_index_uid(
            raw_identifier,
            row=row,
            target_uid_field=uid_field,
            index_uid_by_identifier=index_uid_by_identifier,
        )
        current_value = payload.get(uid_field)
        if not _is_uuid_string(current_value) or _is_synthetic_government_uid(
            current_value,
            index_uid_by_identifier=index_uid_by_identifier,
        ):
            payload[uid_field] = str(replacement_uid)
            changes.append(f"{_json_path((*path, uid_field))}:set_index_uid")
        changes.append(f"{_json_path((*path, legacy_field))}:remove_legacy_field")

    for key, value in list(payload.items()):
        current_path = (*path, str(key))
        if key in UID_FIELD_NAMES:
            repaired_uid = _repair_uid_field(
                value,
                row=row,
                target_uid_field=key,
                index_uid_by_identifier=index_uid_by_identifier,
            )
            if repaired_uid is not None:
                payload[key] = str(repaired_uid)
                changes.append(f"{_json_path(current_path)}:set_index_uid")
            continue

        if _is_calendar_field(key):
            repaired_calendar = _repair_calendar_value(value, code_field=key.endswith("_code"))
            if repaired_calendar is not _NO_CHANGE:
                payload[key] = repaired_calendar
                changes.append(f"{_json_path(current_path)}:set_calendar")
                continue

        if isinstance(value, dict):
            _patch_mapping(
                value,
                row=row,
                index_uid_by_identifier=index_uid_by_identifier,
                path=current_path,
                changes=changes,
            )
        elif isinstance(value, list):
            _patch_sequence(
                value,
                row=row,
                index_uid_by_identifier=index_uid_by_identifier,
                path=current_path,
                changes=changes,
            )


def _patch_sequence(
    payload: list[Any],
    *,
    row: PersistedPricingDetailsRow,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
    path: tuple[str, ...],
    changes: list[str],
) -> None:
    for index, item in enumerate(payload):
        item_path = (*path, str(index))
        if isinstance(item, dict):
            _patch_mapping(
                item,
                row=row,
                index_uid_by_identifier=index_uid_by_identifier,
                path=item_path,
                changes=changes,
            )
        elif isinstance(item, list):
            _patch_sequence(
                item,
                row=row,
                index_uid_by_identifier=index_uid_by_identifier,
                path=item_path,
                changes=changes,
            )


def _repair_uid_field(
    value: Any,
    *,
    row: PersistedPricingDetailsRow,
    target_uid_field: str,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
) -> uuid.UUID | None:
    if _is_synthetic_government_uid(
        value,
        index_uid_by_identifier=index_uid_by_identifier,
    ):
        return _resolve_replacement_index_uid(
            SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER,
            row=row,
            target_uid_field=target_uid_field,
            index_uid_by_identifier=index_uid_by_identifier,
        )
    if _is_uuid_string(value):
        return None

    identifier = _index_identifier_from_value(value, row=row, target_uid_field=target_uid_field)
    if identifier is None:
        return None
    try:
        return index_uid_by_identifier[identifier]
    except KeyError as exc:
        raise PricingDetailsRepairError(
            f"Missing IndexTable row for {identifier!r} while repairing "
            f"{row.asset_identifier}."
        ) from exc


def _resolve_replacement_index_uid(
    value: Any,
    *,
    row: PersistedPricingDetailsRow,
    target_uid_field: str,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
) -> uuid.UUID:
    identifier = _index_identifier_from_value(
        value,
        row=row,
        target_uid_field=target_uid_field,
    )
    if identifier is None and target_uid_field == "benchmark_rate_index_uid":
        identifier = _benchmark_index_identifier_from_asset(row)
    if identifier is None:
        raise PricingDetailsRepairError(
            f"Cannot resolve {target_uid_field} from {value!r} for "
            f"{row.asset_identifier}."
        )
    try:
        return index_uid_by_identifier[identifier]
    except KeyError as exc:
        raise PricingDetailsRepairError(
            f"Missing IndexTable row for {identifier!r} while repairing "
            f"{row.asset_identifier}."
        ) from exc


def _index_identifier_from_value(
    value: Any,
    *,
    row: PersistedPricingDetailsRow,
    target_uid_field: str,
) -> str | None:
    raw_value = value
    if isinstance(raw_value, Mapping):
        for key in (
            "unique_identifier",
            "index_unique_identifier",
            "identifier",
            "name",
        ):
            candidate = raw_value.get(key)
            if candidate not in (None, ""):
                raw_value = candidate
                break
    if raw_value in (None, ""):
        return None
    text = str(raw_value).strip()
    if not text or _is_uuid_string(text):
        return None
    if text == SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER:
        return (
            _benchmark_index_identifier_from_asset(row)
            if target_uid_field == "benchmark_rate_index_uid"
            else None
        )
    if text in REFERENCE_INDEX_IDENTIFIERS:
        return text
    if text in SUBYACENTE_TO_INDEX_MAP:
        return SUBYACENTE_TO_INDEX_MAP[text]
    return None


def _benchmark_index_identifier_from_asset(row: PersistedPricingDetailsRow) -> str | None:
    underlying = _clean_text(row.valmer_underlying)
    if underlying and underlying in SUBYACENTE_TO_INDEX_MAP:
        return SUBYACENTE_TO_INDEX_MAP[underlying]
    issuer = (_clean_text(row.valmer_issuer) or "").upper()
    security_type = (_clean_text(row.valmer_security_type) or "").upper()
    if issuer == "CETES":
        return CETE_28_INDEX_UNIQUE_IDENTIFIER
    if security_type in {"MC", "MP"}:
        return CETE_182_INDEX_UNIQUE_IDENTIFIER
    return None


_NO_CHANGE = object()


def _repair_calendar_value(value: Any, *, code_field: bool) -> Any:
    if isinstance(value, str):
        if code_field:
            return "Mexico" if value in BAD_CALENDAR_TOKENS else _NO_CHANGE
        return (
            dict(MEXICO_CALENDAR_JSON)
            if value in STALE_CALENDAR_OBJECT_TOKENS
            else _NO_CHANGE
        )
    if isinstance(value, Mapping):
        for key in CALENDAR_TOKEN_KEYS:
            token = value.get(key)
            if not isinstance(token, str):
                continue
            if code_field:
                if token in BAD_CALENDAR_TOKENS:
                    return "Mexico"
                continue
            if token in STALE_CALENDAR_OBJECT_TOKENS:
                return dict(MEXICO_CALENDAR_JSON)
    return _NO_CHANGE


def _query_repair_rows(
    *,
    scope: RepairScope,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
    asset_identifiers: Sequence[str] | None,
    page_size: int,
    limit: int | None,
) -> list[PersistedPricingDetailsRow]:
    rows: list[PersistedPricingDetailsRow] = []
    offset = 0
    remaining = limit
    while True:
        page_limit = page_size if remaining is None else min(page_size, remaining)
        if page_limit <= 0:
            break
        result = _execute_pricing_details_select(
            scope=scope,
            index_uid_by_identifier=index_uid_by_identifier,
            asset_identifiers=asset_identifiers,
            limit=page_limit,
            offset=offset,
        )
        page_rows = [
            _row_from_operation_result(scope, row)
            for row in operation_result_rows(result)
        ]
        rows.extend(page_rows)
        if len(page_rows) < page_limit:
            break
        offset += page_limit
        if remaining is not None:
            remaining -= page_limit
    return rows


def _execute_pricing_details_select(
    *,
    scope: RepairScope,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
    asset_identifiers: Sequence[str] | None,
    limit: int,
    offset: int,
) -> Mapping[str, Any]:
    context = AssetPricingDetails._active_context()
    asset_table = AssetTable
    detail_table = ValmerAssetDetailsTable
    if scope == "current":
        pricing_table = AssetCurrentPricingDetailsTable
        time_column = pricing_table.pricing_details_date
        join_clause = pricing_table.__table__.join(
            asset_table.__table__,
            pricing_table.asset_uid == asset_table.uid,
        ).outerjoin(
            detail_table.__table__,
            detail_table.asset_uid == asset_table.uid,
        )
        select_columns = [
            pricing_table.asset_uid.label("asset_uid"),
            asset_table.unique_identifier.label("asset_identifier"),
        ]
    else:
        pricing_table = AssetPricingDetailsStorage
        time_column = pricing_table.time_index
        join_clause = pricing_table.__table__.join(
            asset_table.__table__,
            pricing_table.asset_identifier == asset_table.unique_identifier,
        ).outerjoin(
            detail_table.__table__,
            detail_table.asset_uid == asset_table.uid,
        )
        select_columns = [
            asset_table.uid.label("asset_uid"),
            pricing_table.asset_identifier.label("asset_identifier"),
        ]

    source_filter = or_(
        pricing_table.source == "valmer",
        cast(pricing_table.metadata_json, String).ilike("%valmer_unique_identifier%"),
    )
    bad_filter = _bad_payload_filter(
        pricing_table.instrument_dump,
        index_uid_by_identifier=index_uid_by_identifier,
    )
    filters = [source_filter, bad_filter]
    if asset_identifiers:
        filters.append(asset_table.unique_identifier.in_(list(asset_identifiers)))

    statement = (
        select(
            *select_columns,
            time_column.label("time_index"),
            pricing_table.instrument_type.label("instrument_type"),
            pricing_table.instrument_dump.label("instrument_dump"),
            pricing_table.serialization_format.label("serialization_format"),
            pricing_table.pricing_package_version.label("pricing_package_version"),
            pricing_table.source.label("source"),
            pricing_table.metadata_json.label("metadata_json"),
            detail_table.underlying.label("valmer_underlying"),
            detail_table.security_type.label("valmer_security_type"),
            detail_table.issuer.label("valmer_issuer"),
        )
        .select_from(join_clause)
        .where(and_(*filters))
        .order_by(time_column, asset_table.unique_identifier)
        .limit(limit)
    )
    if offset:
        statement = statement.offset(offset)

    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[pricing_table, asset_table, detail_table],
        access="read",
    )
    return execute_markets_operation(operation, context=context)


def _row_from_operation_result(
    scope: RepairScope,
    row: Mapping[str, Any],
) -> PersistedPricingDetailsRow:
    return PersistedPricingDetailsRow(
        scope=scope,
        asset_uid=uuid.UUID(str(row["asset_uid"])),
        asset_identifier=str(row["asset_identifier"]),
        time_index=_as_aware_datetime(row["time_index"]),
        instrument_type=str(row["instrument_type"]),
        instrument_dump=_as_dict(row["instrument_dump"], field_name="instrument_dump"),
        serialization_format=str(row["serialization_format"]),
        pricing_package_version=_optional_str(row.get("pricing_package_version")),
        source=_optional_str(row.get("source")),
        metadata_json=_optional_dict(row.get("metadata_json")),
        valmer_underlying=_optional_str(row.get("valmer_underlying")),
        valmer_security_type=_optional_str(row.get("valmer_security_type")),
        valmer_issuer=_optional_str(row.get("valmer_issuer")),
    )


def _load_index_uid_by_identifier() -> dict[str, uuid.UUID]:
    bootstrap_runtime(seed_static_rows=False)
    identifiers = sorted(
        {
            *REFERENCE_INDEX_IDENTIFIERS,
            SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER,
        }
    )
    context = AssetPricingDetails._active_context()
    statement = select(
        IndexTable.unique_identifier.label("unique_identifier"),
        IndexTable.uid.label("uid"),
    ).where(IndexTable.unique_identifier.in_(identifiers))
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[IndexTable],
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    return {
        str(row["unique_identifier"]): uuid.UUID(str(row["uid"]))
        for row in operation_result_rows(result)
    }


def _bulk_upsert_repair_plans(
    *,
    current_plans: Sequence[PricingDetailsRepairPlan],
    timestamped_plans: Sequence[PricingDetailsRepairPlan],
    batch_size: int,
) -> None:
    context = AssetPricingDetails._active_context()
    _bulk_upsert_values(
        context=context,
        model=AssetCurrentPricingDetailsTable,
        conflict_columns=AssetCurrentPricingDetails.__upsert_keys__,
        values=[plan.upsert_values() for plan in current_plans],
        batch_size=batch_size,
    )
    _bulk_upsert_values(
        context=context,
        model=AssetPricingDetailsStorage,
        conflict_columns=AssetPricingDetails.__upsert_keys__,
        values=[plan.upsert_values() for plan in timestamped_plans],
        batch_size=batch_size,
    )


def _bulk_upsert_values(
    *,
    context: Any,
    model: Any,
    conflict_columns: Sequence[str],
    values: Sequence[Mapping[str, Any]],
    batch_size: int,
) -> None:
    for start in range(0, len(values), batch_size):
        chunk = values[start : start + batch_size]
        if not chunk:
            continue
        bulk_upsert_model(
            context,
            model=model,
            values=chunk,
            conflict_columns=conflict_columns,
        )


def _bad_payload_filter(
    instrument_dump_column: Any,
    *,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
) -> Any:
    dump = cast(instrument_dump_column, JSONB)
    filters: list[Any] = []

    calendar_paths = (
        ("calendar",),
        ("calendar", "name"),
        ("calendar", "class"),
        ("calendar", "calendar_class"),
        ("calendar", "calendar_name"),
        ("calendar", "calendar_code"),
        ("calendar_code",),
        ("calendar_code", "name"),
        ("calendar_code", "class"),
        ("calendar_code", "calendar_class"),
        ("calendar_code", "calendar_name"),
        ("fixing_calendar_code",),
        ("fixing_calendar_code", "name"),
        ("fixing_calendar_code", "class"),
        ("fixing_calendar_code", "calendar_class"),
        ("fixing_calendar_code", "calendar_name"),
        ("schedule", "calendar"),
        ("schedule", "calendar", "name"),
        ("schedule", "calendar", "class"),
        ("schedule", "calendar", "calendar_class"),
        ("schedule", "calendar", "calendar_name"),
        ("schedule", "calendar", "calendar_code"),
    )
    filters.extend(
        _jsonb_text_path(dump, *path).in_(tuple(BAD_CALENDAR_TOKENS))
        for path in calendar_paths
    )
    stale_instrument_calendar_paths = (
        ("calendar",),
        ("calendar", "name"),
        ("calendar", "class"),
        ("calendar", "calendar_class"),
        ("calendar", "calendar_name"),
        ("calendar", "calendar_code"),
        ("schedule", "calendar"),
        ("schedule", "calendar", "name"),
        ("schedule", "calendar", "class"),
        ("schedule", "calendar", "calendar_class"),
        ("schedule", "calendar", "calendar_name"),
        ("schedule", "calendar", "calendar_code"),
    )
    filters.extend(
        _jsonb_text_path(dump, *path) == "Mexico"
        for path in stale_instrument_calendar_paths
    )
    filters.extend(
        dump.op("?")(field)
        for field in (
            *STALE_INDEX_NAME_FIELD_TARGETS,
            *LEGACY_INDEX_RELATION_FIELD_TARGETS,
        )
    )

    synthetic_uid = index_uid_by_identifier.get(SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER)
    if synthetic_uid is not None:
        filters.extend(
            _jsonb_text_path(dump, field) == str(synthetic_uid)
            for field in UID_FIELD_NAMES
        )

    legacy_identifier_values = tuple(
        sorted(
            {
                *REFERENCE_INDEX_IDENTIFIERS,
                SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER,
                *SUBYACENTE_TO_INDEX_MAP,
            }
        )
    )
    filters.extend(
        _jsonb_text_path(dump, field).in_(legacy_identifier_values)
        for field in UID_FIELD_NAMES
    )
    return or_(*filters)


def _jsonb_text_path(dump: Any, *path: str) -> Any:
    return func.jsonb_extract_path_text(dump, *path)


def _is_calendar_field(key: str) -> bool:
    return key in {"calendar", "fixing_calendar_code", "calendar_code"}


def _is_synthetic_government_uid(
    value: Any,
    *,
    index_uid_by_identifier: Mapping[str, uuid.UUID],
) -> bool:
    synthetic_uid = index_uid_by_identifier.get(SYNTHETIC_GOVERNMENT_INDEX_IDENTIFIER)
    if synthetic_uid is None:
        return False
    try:
        return uuid.UUID(str(value)) == synthetic_uid
    except (TypeError, ValueError, AttributeError):
        return False


def _is_uuid_string(value: Any) -> bool:
    try:
        uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return False
    return True


def _json_path(path: Sequence[str]) -> str:
    return ".".join(path)


def _clean_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _optional_str(value: Any) -> str | None:
    return _clean_text(value)


def _optional_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return _as_dict(value, field_name="metadata_json")


def _as_dict(value: Any, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise TypeError(f"{field_name} must be a JSON object.")


def _as_aware_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = dt.datetime.fromisoformat(text)
    else:
        to_pydatetime = getattr(value, "to_pydatetime", None)
        if callable(to_pydatetime):
            parsed = to_pydatetime()
        else:
            raise TypeError(f"Cannot parse datetime value {value!r}.")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _failure_from_row(
    row: PersistedPricingDetailsRow,
    reason: str,
) -> PricingDetailsRepairFailure:
    return PricingDetailsRepairFailure(
        scope=row.scope,
        asset_identifier=row.asset_identifier,
        time_index=row.time_index.isoformat(),
        reason=reason,
    )


def _format_failure_message(failures: Sequence[PricingDetailsRepairFailure]) -> str:
    sample = "; ".join(
        f"{failure.scope}:{failure.asset_identifier}@{failure.time_index}: "
        f"{failure.reason}"
        for failure in failures[:10]
    )
    suffix = "" if len(failures) <= 10 else f"; ... (+{len(failures) - 10} more)"
    return f"Cannot repair {len(failures)} Valmer pricing-detail rows: {sample}{suffix}"


__all__ = [
    "PersistedPricingDetailsRow",
    "PricingDetailsRepairError",
    "PricingDetailsRepairFailure",
    "PricingDetailsRepairPlan",
    "PricingDetailsRepairSummary",
    "patch_instrument_dump",
    "repair_valmer_asset_pricing_details",
]
