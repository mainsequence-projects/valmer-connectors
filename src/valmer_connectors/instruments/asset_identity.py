from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import pandas as pd
from msm.constants import ASSET_TYPE_BOND

if TYPE_CHECKING:
    from msm.api.assets import Asset

VALMER_ASSET_IDENTITY_COLUMNS = ("tipovalor", "emisora", "serie")

_RUNTIME_READY = False


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
    batch_size: int = 5000,
    ensure_schemas: bool = True,
) -> dict[str, Asset]:
    """Resolve existing typed markets assets by Valmer unique identifier."""

    identifiers = normalize_valmer_unique_identifiers(unique_identifiers)
    if not identifiers:
        return {}
    if ensure_schemas:
        ensure_valmer_asset_runtime()

    from msm.api.base import operation_result_rows
    from msm.repositories.crud import search_model

    Asset = _asset_model()
    context = Asset._active_context()
    assets: dict[str, Asset] = {}
    for batch in _batches(identifiers, batch_size):
        result = search_model(
            context,
            model=Asset.__table__,
            in_filters={"unique_identifier": batch},
            limit=len(batch),
        )
        for row in operation_result_rows(result):
            asset = Asset.model_validate(row)
            assets[asset.unique_identifier] = asset
    return assets


def upsert_valmer_assets(
    unique_identifiers: Sequence[object],
    *,
    batch_size: int = 5000,
    ensure_schemas: bool = True,
) -> dict[str, Asset]:
    """Idempotently create or update Valmer assets as typed bond assets."""

    identifiers = normalize_valmer_unique_identifiers(unique_identifiers)
    if not identifiers:
        return {}
    if ensure_schemas:
        ensure_valmer_asset_runtime()

    from msm.repositories.crud import upsert_model

    Asset = _asset_model()
    context = Asset._active_context()
    assets: dict[str, Asset] = {}
    for batch in _batches(identifiers, batch_size):
        for unique_identifier in batch:
            result = upsert_model(
                context,
                model=Asset.__table__,
                values={
                    "unique_identifier": unique_identifier,
                    "asset_type": ASSET_TYPE_BOND,
                },
                conflict_columns=Asset.__upsert_keys__,
            )
            asset = Asset._from_operation_result(result)
            assets[asset.unique_identifier] = asset
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


def _batches(values: Sequence[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [list(values[start : start + batch_size]) for start in range(0, len(values), batch_size)]


def _asset_model():
    from msm.api.assets import Asset

    return Asset


__all__ = [
    "VALMER_ASSET_IDENTITY_COLUMNS",
    "add_valmer_unique_identifier",
    "build_valmer_unique_identifier",
    "ensure_valmer_asset_runtime",
    "ensure_valmer_asset_schemas",
    "normalize_valmer_unique_identifiers",
    "resolve_valmer_assets",
    "upsert_valmer_assets",
]
