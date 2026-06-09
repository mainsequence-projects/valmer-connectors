from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING
import uuid

import pandas as pd
from msm.constants import ASSET_TYPE_BOND
from sqlalchemy import select
from valmer_connectors.settings import resolve_valmer_asset_upsert_batch_size
from valmer_connectors.settings import resolve_valmer_meta_operation_batch_size

if TYPE_CHECKING:
    from msm.api.assets import Asset

VALMER_ASSET_IDENTITY_COLUMNS = ("tipovalor", "emisora", "serie")

_RUNTIME_READY = False


@dataclass(frozen=True)
class ValmerAssetRef:
    unique_identifier: str
    uid: uuid.UUID
    asset_type: str | None

    def as_asset(self) -> Asset:
        AssetModel = _asset_model()
        return AssetModel.model_validate(
            {
                "uid": self.uid,
                "unique_identifier": self.unique_identifier,
                "asset_type": self.asset_type,
            }
        )


def build_valmer_unique_identifier(row: Mapping[str, object]) -> str:
    """Build the stable Valmer asset key from one normalized source row."""

    parts = [_required_identity_part(row, field) for field in VALMER_ASSET_IDENTITY_COLUMNS]
    return "_".join(parts)


def add_valmer_unique_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of a Valmer frame with the canonical unique identifier column."""

    missing = sorted(set(VALMER_ASSET_IDENTITY_COLUMNS) - set(df.columns))
    if missing:
        raise KeyError(f"Missing Valmer asset identity columns: {missing}")

    out = df.copy()
    out["unique_identifier"] = (
        out["tipovalor"]
        .astype("string")
        .str.cat(out["emisora"].astype("string"), sep="_")
        .str.cat(out["serie"].astype("string"), sep="_")
    )
    return out


def normalize_valmer_unique_identifiers(unique_identifiers: Sequence[object]) -> list[str]:
    """Normalize a UID sequence while preserving order and removing nulls."""

    seen: set[str] = set()
    normalized: list[str] = []
    for raw_identifier in unique_identifiers:
        if pd.isna(raw_identifier):
            continue
        identifier = str(raw_identifier)
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        normalized.append(identifier)
    return normalized


def ensure_valmer_asset_runtime() -> None:
    """Verify that the markets runtime is attached for Asset row operations."""

    global _RUNTIME_READY
    if _RUNTIME_READY:
        return

    from msm.bootstrap import get_runtime
    from msm.models import AssetTable

    try:
        get_runtime().table(AssetTable)
    except Exception as exc:
        raise RuntimeError(
            "Valmer asset row operations require the project bootstrap entry point. "
            "Run valmer_connectors.instruments.bootstrap.bootstrap_runtime() once during "
            "application initialization before resolving or upserting Valmer assets."
        ) from exc
    _RUNTIME_READY = True


def ensure_valmer_asset_schemas() -> None:
    """Compatibility wrapper for the old schema-oriented helper name."""

    ensure_valmer_asset_runtime()


def resolve_valmer_assets(
    unique_identifiers: Sequence[object],
    *,
    batch_size: int | None = None,
    ensure_schemas: bool = True,
    logger=None,
) -> dict[str, Asset]:
    """Resolve existing typed markets assets by Valmer unique identifier."""

    refs = resolve_valmer_asset_refs(
        unique_identifiers,
        batch_size=batch_size,
        ensure_schemas=ensure_schemas,
        logger=logger,
    )
    return {unique_identifier: ref.as_asset() for unique_identifier, ref in refs.items()}


def resolve_valmer_asset_refs(
    unique_identifiers: Sequence[object],
    *,
    batch_size: int | None = None,
    ensure_schemas: bool = True,
    logger=None,
) -> dict[str, ValmerAssetRef]:
    """Resolve thin Valmer asset references without materializing full rows."""

    identifiers = normalize_valmer_unique_identifiers(unique_identifiers)
    if not identifiers:
        return {}
    if ensure_schemas:
        ensure_valmer_asset_runtime()

    from msm.api.base import operation_result_rows
    from msm.repositories.base import compile_markets_statement
    from msm.repositories.base import execute_markets_operation

    Asset = _asset_model()
    context = Asset._active_context()
    refs: dict[str, ValmerAssetRef] = {}
    resolved_batch_size = resolve_valmer_meta_operation_batch_size(batch_size)
    batches = batched_values(identifiers, resolved_batch_size)
    if logger is not None:
        logger.info(
            f"Resolving {len(identifiers)} Valmer asset refs in {len(batches)} batches "
            f"of up to {resolved_batch_size}."
        )
    progress = _ProgressMilestones(logger, "Resolving Valmer asset refs", len(identifiers))
    for batch in batches:
        statement = (
            select(
                Asset.__table__.unique_identifier.label("unique_identifier"),
                Asset.__table__.uid.label("uid"),
                Asset.__table__.asset_type.label("asset_type"),
            )
            .where(Asset.__table__.unique_identifier.in_(batch))
            .limit(len(batch))
        )
        operation = compile_markets_statement(
            statement,
            context=context,
            operation="select",
            models=[Asset.__table__],
            access="read",
        )
        result = execute_markets_operation(operation, context=context)
        for row in operation_result_rows(result):
            unique_identifier = row.get("unique_identifier")
            uid = row.get("uid")
            if not unique_identifier or uid in (None, ""):
                continue
            refs[str(unique_identifier)] = ValmerAssetRef(
                unique_identifier=str(unique_identifier),
                uid=_uuid_value(uid),
                asset_type=row.get("asset_type"),
            )
        progress.advance(len(batch))
    if logger is not None:
        logger.info(
            f"Resolved {len(identifiers)} Valmer asset refs: "
            f"{len(refs)} existing, {len(identifiers) - len(refs)} missing."
        )
    return refs


def resolve_valmer_asset_uids(
    unique_identifiers: Sequence[object],
    *,
    batch_size: int | None = None,
    ensure_schemas: bool = True,
    logger=None,
) -> dict[str, uuid.UUID]:
    """Resolve only AssetTable.uid values keyed by Valmer unique identifier."""

    identifiers = normalize_valmer_unique_identifiers(unique_identifiers)
    if not identifiers:
        return {}
    if ensure_schemas:
        ensure_valmer_asset_runtime()

    from msm.api.base import operation_result_rows
    from msm.repositories.base import compile_markets_statement
    from msm.repositories.base import execute_markets_operation

    Asset = _asset_model()
    context = Asset._active_context()
    uid_map: dict[str, uuid.UUID] = {}
    resolved_batch_size = resolve_valmer_meta_operation_batch_size(batch_size)
    batches = batched_values(identifiers, resolved_batch_size)
    if logger is not None:
        logger.info(
            f"Resolving {len(identifiers)} Valmer asset UIDs in {len(batches)} batches "
            f"of up to {resolved_batch_size}."
        )
    progress = _ProgressMilestones(logger, "Resolving Valmer asset UIDs", len(identifiers))
    for batch in batches:
        statement = (
            select(
                Asset.__table__.unique_identifier.label("unique_identifier"),
                Asset.__table__.uid.label("uid"),
            )
            .where(Asset.__table__.unique_identifier.in_(batch))
            .limit(len(batch))
        )
        operation = compile_markets_statement(
            statement,
            context=context,
            operation="select",
            models=[Asset.__table__],
            access="read",
        )
        result = execute_markets_operation(operation, context=context)
        for row in operation_result_rows(result):
            unique_identifier = row.get("unique_identifier")
            uid = row.get("uid")
            if unique_identifier and uid not in (None, ""):
                uid_map[str(unique_identifier)] = _uuid_value(uid)
        progress.advance(len(batch))
    if logger is not None:
        logger.info(
            f"Resolved {len(identifiers)} Valmer asset UIDs: "
            f"{len(uid_map)} existing, {len(identifiers) - len(uid_map)} missing."
        )
    return uid_map


def upsert_valmer_assets(
    unique_identifiers: Sequence[object],
    *,
    batch_size: int | None = None,
    ensure_schemas: bool = True,
    logger=None,
) -> dict[str, Asset]:
    """Idempotently create or update Valmer assets as typed bond assets."""

    identifiers = normalize_valmer_unique_identifiers(unique_identifiers)
    if not identifiers:
        return {}
    if ensure_schemas:
        ensure_valmer_asset_runtime()

    from msm.api.base import operation_result_rows
    from msm.repositories.crud import bulk_upsert_model

    Asset = _asset_model()
    context = Asset._active_context()
    assets: dict[str, Asset] = {}
    resolved_batch_size = resolve_valmer_asset_upsert_batch_size(batch_size)
    batches = batched_values(identifiers, resolved_batch_size)
    if logger is not None:
        logger.info(
            f"Upserting {len(identifiers)} Valmer assets in {len(batches)} batches "
            f"of up to {resolved_batch_size}."
        )
    progress = _ProgressMilestones(logger, "Upserting Valmer assets", len(identifiers))
    for batch_index, batch in enumerate(batches, start=1):
        result = bulk_upsert_model(
            context,
            model=Asset.__table__,
            values=[
                {
                    "unique_identifier": unique_identifier,
                    "asset_type": ASSET_TYPE_BOND,
                }
                for unique_identifier in batch
            ],
            conflict_columns=Asset.__upsert_keys__,
        )
        for row in operation_result_rows(result):
            asset = Asset.model_validate(row)
            assets[asset.unique_identifier] = asset
        progress.advance(len(batch))
        if logger is not None:
            logger.info(
                "Completed Valmer asset bulk upsert batch "
                f"{batch_index}/{len(batches)} ({len(batch)} assets, "
                f"{progress.completed}/{len(identifiers)} total)."
            )
    return assets


def _required_identity_part(row: Mapping[str, object], field: str) -> str:
    if field not in row:
        raise KeyError(f"Missing Valmer asset identity field: {field}")
    value = row[field]
    if pd.isna(value):
        raise ValueError(f"Valmer asset identity field {field!r} cannot be null")
    text = str(value)
    if not text:
        raise ValueError(f"Valmer asset identity field {field!r} cannot be empty")
    return text


def batched_values(values: Sequence[object], batch_size: int) -> list[list[object]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [list(values[start : start + batch_size]) for start in range(0, len(values), batch_size)]


def _uuid_value(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


class _ProgressMilestones:
    def __init__(self, logger, label: str, total: int):
        self.logger = logger
        self.label = label
        self.total = total
        self.completed = 0
        self.milestones = [
            (percent, max(1, (total * percent + 99) // 100))
            for percent in (1, 5, 20, 40, 60, 80, 100)
        ]
        self.next_milestone = 0

    def advance(self, count: int = 1) -> None:
        if self.logger is None or not self.total:
            return
        self.completed += count
        while self.next_milestone < len(self.milestones):
            percent, threshold = self.milestones[self.next_milestone]
            if self.completed < threshold:
                return
            self.logger.info(
                f"{self.label}: {percent}% complete ({self.completed}/{self.total})."
            )
            self.next_milestone += 1


def _asset_model():
    from msm.api.assets import Asset

    return Asset


__all__ = [
    "VALMER_ASSET_IDENTITY_COLUMNS",
    "ValmerAssetRef",
    "add_valmer_unique_identifier",
    "batched_values",
    "build_valmer_unique_identifier",
    "ensure_valmer_asset_runtime",
    "ensure_valmer_asset_schemas",
    "normalize_valmer_unique_identifiers",
    "resolve_valmer_assets",
    "resolve_valmer_asset_refs",
    "resolve_valmer_asset_uids",
    "upsert_valmer_assets",
]
