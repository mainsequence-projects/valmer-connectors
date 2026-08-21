from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import structlog
from msm.api.assets import Asset as MarketsAsset

from valmer_connectors.asset_classification import classify_valmer_asset_type
from valmer_connectors.data_nodes.nodes import (
    _persist_valmer_pricing_details_batch,
    _publish_valmer_asset_snapshots,
)
from valmer_connectors.instruments.asset_identity import (
    _upsert_asset_table_rows,
    add_valmer_unique_identifier,
    resolve_valmer_asset_refs,
)
from valmer_connectors.instruments.vector_to_asset import (
    build_qll_bond_from_row,
    get_instrument_conventions,
    normalize_column_name,
)
from valmer_connectors.meta_tables.valmer_asset_details import (
    upsert_valmer_asset_details,
)
from valmer_connectors.settings import (
    resolve_valmer_meta_operation_batch_size,
    resolve_valmer_pricing_details_batch_size,
)

VALMER_IDENTITY_COLUMNS = ("tipovalor", "emisora", "serie")
VALMER_DETAIL_REQUIRED_COLUMNS = (*VALMER_IDENTITY_COLUMNS, "fecha")
VALMER_SNAPSHOT_REQUIRED_COLUMNS = ("unique_identifier", "fecha", "nombrecompleto")

ValmerAssetTypeClassifier = Callable[[Mapping[str, Any]], str | None]


@dataclass(frozen=True)
class ValmerAssetRegistrationResult:
    assets_by_identifier: dict[str, MarketsAsset]
    asset_type_by_identifier: dict[str, str]
    details_upserted_count: int = 0
    snapshots_published_count: int = 0
    pricing_details_persisted_count: int = 0
    skipped_unsupported: list[str] = field(default_factory=list)
    pricing_failed: dict[str, str] = field(default_factory=dict)


def register_valmer_assets_from_rows(
    rows: pd.DataFrame,
    *,
    asset_type_classifier: ValmerAssetTypeClassifier | None = None,
    include_pricing_details: bool = True,
    publish_snapshots: bool = True,
    strict_unsupported: bool = False,
    strict_pricing: bool = False,
    batch_size: int | None = None,
    logger=None,
) -> ValmerAssetRegistrationResult:
    """Register Valmer rows through Asset, details, snapshot, and optional pricing layers."""

    log = logger or structlog.get_logger("mainsequence")
    classifier = asset_type_classifier or classify_valmer_asset_type
    operation_batch_size = resolve_valmer_meta_operation_batch_size(batch_size)
    pricing_batch_size = resolve_valmer_pricing_details_batch_size(batch_size)

    working = _normalize_registration_rows(rows)
    if working.empty:
        return ValmerAssetRegistrationResult(
            assets_by_identifier={},
            asset_type_by_identifier={},
        )

    asset_type_by_identifier: dict[str, str] = {}
    skipped_unsupported: list[str] = []
    latest_rows = working.drop_duplicates("unique_identifier", keep="last")
    for row in latest_rows.to_dict(orient="records"):
        unique_identifier = str(row["unique_identifier"])
        asset_type = classifier(row)
        if asset_type is None:
            skipped_unsupported.append(unique_identifier)
            continue
        asset_type = str(asset_type).strip()
        if not asset_type:
            raise ValueError(
                f"Asset type classifier returned an empty type for {unique_identifier!r}."
            )
        asset_type_by_identifier[unique_identifier] = asset_type

    if strict_unsupported and skipped_unsupported:
        raise ValueError(
            "Unsupported Valmer rows found during strict registration: "
            f"{_summarize_identifiers(skipped_unsupported)}"
        )
    if not asset_type_by_identifier:
        log.info(
            "No Valmer rows selected for registration; "
            f"{len(skipped_unsupported)} rows were unsupported."
        )
        return ValmerAssetRegistrationResult(
            assets_by_identifier={},
            asset_type_by_identifier={},
            skipped_unsupported=skipped_unsupported,
        )

    supported_identifiers = list(asset_type_by_identifier)
    supported_rows = working[working["unique_identifier"].isin(supported_identifiers)].copy()
    _validate_registration_columns(
        supported_rows,
        VALMER_DETAIL_REQUIRED_COLUMNS,
        step="Valmer asset details",
    )
    if publish_snapshots:
        _validate_registration_columns(
            supported_rows,
            VALMER_SNAPSHOT_REQUIRED_COLUMNS,
            step="Valmer asset snapshots",
        )

    log.info(
        "Starting Valmer asset registration: "
        f"{len(working)} source rows, {len(supported_identifiers)} supported assets, "
        f"{len(skipped_unsupported)} unsupported assets, batch size {operation_batch_size}."
    )

    existing_asset_refs = resolve_valmer_asset_refs(
        supported_identifiers,
        batch_size=operation_batch_size,
        logger=log,
    )
    asset_type_conflicts = [
        unique_identifier
        for unique_identifier, asset_ref in existing_asset_refs.items()
        if asset_ref.asset_type != asset_type_by_identifier[unique_identifier]
    ]
    if asset_type_conflicts:
        raise RuntimeError(
            "Valmer asset type conflict for existing AssetTable rows: "
            f"{_summarize_identifiers(asset_type_conflicts)}. Refusing to overwrite "
            "AssetTable.asset_type values."
        )

    assets_by_identifier = {
        unique_identifier: asset_ref.as_asset()
        for unique_identifier, asset_ref in existing_asset_refs.items()
    }
    missing_identifiers = [
        unique_identifier
        for unique_identifier in supported_identifiers
        if unique_identifier not in assets_by_identifier
    ]
    if missing_identifiers:
        upserted_assets = _upsert_asset_table_rows(
            {
                unique_identifier: asset_type_by_identifier[unique_identifier]
                for unique_identifier in missing_identifiers
            },
            batch_size=operation_batch_size,
            logger=log,
        )
        assets_by_identifier.update(upserted_assets)

    unresolved = [
        unique_identifier
        for unique_identifier in supported_identifiers
        if unique_identifier not in assets_by_identifier
    ]
    if unresolved:
        raise RuntimeError(
            "Valmer AssetTable registration did not return all supported assets: "
            f"{_summarize_identifiers(unresolved)}"
        )

    detail_rows = upsert_valmer_asset_details(
        supported_rows,
        assets_by_identifier,
        logger=log,
    )
    snapshot_count = (
        _publish_valmer_asset_snapshots(supported_rows, assets_by_identifier, logger=log)
        if publish_snapshots
        else 0
    )
    persisted_pricing_uids: list[str] = []
    pricing_failed: dict[str, str] = {}
    if include_pricing_details:
        instrument_map, pricing_failed = _build_pricing_detail_map(supported_rows, log)
        assets_for_pricing = {
            unique_identifier: assets_by_identifier[unique_identifier]
            for unique_identifier in instrument_map
            if unique_identifier in assets_by_identifier
        }
        if instrument_map:
            persisted_pricing_uids = _persist_valmer_pricing_details_batch(
                assets_for_update=assets_for_pricing,
                instrument_pricing_detail_map=instrument_map,
                batch_size=pricing_batch_size,
                logger=log,
            )
        if strict_pricing and pricing_failed:
            raise RuntimeError(
                "Valmer pricing detail build failed for supported rows: "
                f"{_summarize_identifiers(pricing_failed)}"
            )

    return ValmerAssetRegistrationResult(
        assets_by_identifier=assets_by_identifier,
        asset_type_by_identifier=asset_type_by_identifier,
        details_upserted_count=len(detail_rows),
        snapshots_published_count=snapshot_count,
        pricing_details_persisted_count=len(persisted_pricing_uids),
        skipped_unsupported=skipped_unsupported,
        pricing_failed=pricing_failed,
    )


def _normalize_registration_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows is None:
        raise ValueError("rows cannot be None.")
    working = rows.copy()
    working.columns = [normalize_column_name(column) for column in working.columns]
    _validate_registration_columns(
        working, VALMER_IDENTITY_COLUMNS, step="Valmer identity"
    )
    working = add_valmer_unique_identifier(working)
    _validate_registration_columns(
        working,
        ("unique_identifier", *VALMER_IDENTITY_COLUMNS),
        step="Valmer identity",
    )
    return working


def _validate_registration_columns(
    rows: pd.DataFrame,
    columns: tuple[str, ...],
    *,
    step: str,
) -> None:
    missing_columns = [column for column in columns if column not in rows.columns]
    if missing_columns:
        raise KeyError(f"{step} requires source columns: {missing_columns}")
    bad_columns: dict[str, list[str]] = {}
    for column in columns:
        series = rows[column]
        invalid = series.isna() | series.astype("string").str.strip().eq("")
        if invalid.any():
            if "unique_identifier" in rows.columns:
                bad_columns[column] = (
                    rows.loc[invalid, "unique_identifier"].astype(str).head(10).tolist()
                )
            else:
                bad_columns[column] = [
                    str(index) for index in rows.loc[invalid].head(10).index
                ]
    if bad_columns:
        raise ValueError(f"{step} has missing required values: {bad_columns}")


def _build_pricing_detail_map(
    rows: pd.DataFrame,
    logger,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    latest_rows = rows.drop_duplicates("unique_identifier", keep="last").set_index(
        "unique_identifier"
    )
    instrument_map: dict[str, dict[str, Any]] = {}
    failures: dict[str, str] = {}
    for unique_identifier, row in latest_rows.iterrows():
        try:
            calendar, business_day_convention, settlement_days, day_count = (
                get_instrument_conventions(row)
            )
            instrument = build_qll_bond_from_row(
                row=row,
                calendar=calendar,
                dc=day_count,
                bdc=business_day_convention,
                settlement_days=settlement_days,
            )
        except Exception as exc:
            failures[str(unique_identifier)] = str(exc)
            logger.error(
                "Cannot build Valmer pricing details for "
                f"{unique_identifier}: pricing adapter failed: {exc}"
            )
            continue
        instrument_map[str(unique_identifier)] = {
            "instrument": instrument,
            "pricing_details_date": row["fecha"],
        }
    return instrument_map, failures


def _summarize_identifiers(values: object, *, limit: int = 10) -> str:
    identifiers = list(values) if not isinstance(values, Mapping) else list(values.keys())
    sample = ", ".join(str(value) for value in identifiers[:limit])
    suffix = (
        ""
        if len(identifiers) <= limit
        else f", ... (+{len(identifiers) - limit} more)"
    )
    return f"[{sample}{suffix}]"


__all__ = [
    "ValmerAssetRegistrationResult",
    "ValmerAssetTypeClassifier",
    "classify_valmer_asset_type",
    "register_valmer_assets_from_rows",
]
