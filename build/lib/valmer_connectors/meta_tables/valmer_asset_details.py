from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd
from msm.base import (
    MarketsBase,
    markets_table_args,
)
from msm.models.assets import AssetTable
from sqlalchemy import Date, DateTime, Float, ForeignKey, Index, Integer, String, select
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from valmer_connectors.markets import ValmerMarketsMetaTableMixin
from valmer_connectors.settings import (
    resolve_valmer_asset_upsert_batch_size,
    resolve_valmer_meta_operation_batch_size,
)

VALMER_ASSET_DETAIL_VECTOR_COLUMNS = frozenset(
    {
        "security_type",
        "issuer",
        "series",
        "full_name",
        "sector",
        "issued_amount",
        "issue_date",
        "issue_term",
        "maturity_date",
        "face_value",
        "issue_currency",
        "underlying",
        "placement_yield",
        "placement_spread",
        "coupon_frequency",
        "coupon_rate",
        "coupon_rule",
        "coupons_at_issue",
    }
)

VALMER_ASSET_DETAIL_SOURCE_COLUMNS = frozenset(
    {
        "tipovalor",
        "emisora",
        "serie",
        "nombrecompleto",
        "sector",
        "montoemitido",
        "fechaemision",
        "plazoemision",
        "fechavcto",
        "valornominal",
        "monedaemision",
        "subyacente",
        "rendcolocacion",
        "stcolocacion",
        "freccpn",
        "tasacupon",
        "reglacupon",
        "cuponesemision",
    }
)

_VALMER_ASSET_DETAILS_CONTEXT = None


@dataclass(frozen=True)
class ValmerAssetDetailVersion:
    asset_uid: uuid.UUID
    valmer_unique_identifier: str
    details_asof: dt.datetime | None


def _column_info(label: str, description: str) -> dict[str, str]:
    return {"label": label, "description": description}


class ValmerAssetDetailsTable(ValmerMarketsMetaTableMixin, MarketsBase):
    """Latest Valmer vendor details linked 1:1 to the canonical Asset row."""

    __metatable_identifier__ = "ValmerAssetDetails"
    __metatable_description__ = (
        "Latest static Valmer asset descriptors keyed one-to-one by AssetTable.uid. "
        "Rows hold vendor master-data fields that do not belong in time-indexed "
        "vector price storage."
    )
    __table_args__ = markets_table_args(
        __metatable_identifier__,
        Index(
            None,
            "valmer_unique_identifier",
            unique=True,
        ),
        Index(
            None,
            "issuer",
        ),
        Index(
            None,
            "maturity_date",
        ),
    )

    asset_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            f"{AssetTable.__table__.fullname}.uid",
            ondelete="CASCADE",
        ),
        primary_key=True,
        nullable=False,
        info=_column_info(
            "Asset UID",
            "Foreign key to AssetTable.uid for the canonical asset described by this row.",
        ),
    )
    valmer_unique_identifier: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        info=_column_info(
            "Valmer Unique Identifier",
            "Valmer instrument identifier used to join source vector rows to AssetTable.",
        ),
    )
    details_asof: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        info=_column_info(
            "Details As Of",
            "UTC timestamp of the latest Valmer source row used to hydrate these details.",
        ),
    )
    security_type: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        info=_column_info("Security Type", "Valmer TIPO VALOR security type code."),
    )
    issuer: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        info=_column_info("Issuer", "Valmer EMISORA issuer code."),
    )
    series: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        info=_column_info("Series", "Valmer SERIE instrument series code."),
    )
    full_name: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
        info=_column_info(
            "Full Name", "Full Valmer instrument name from NOMBRE COMPLETO."
        ),
    )
    sector: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        info=_column_info("Sector", "Valmer SECTOR classification."),
    )
    issued_amount: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info=_column_info(
            "Issued Amount", "Original issued amount from MONTO EMITIDO."
        ),
    )
    issue_date: Mapped[dt.date | None] = mapped_column(
        Date,
        nullable=True,
        info=_column_info("Issue Date", "Instrument issue date from FECHA EMISION."),
    )
    issue_term: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info=_column_info("Issue Term", "Original issue term from PLAZO EMISION."),
    )
    maturity_date: Mapped[dt.date | None] = mapped_column(
        Date,
        nullable=True,
        info=_column_info("Maturity Date", "Instrument maturity date from FECHA VCTO."),
    )
    face_value: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info=_column_info("Face Value", "Instrument face value from VALOR NOMINAL."),
    )
    issue_currency: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        info=_column_info(
            "Issue Currency", "Original issue currency from MONEDA EMISION."
        ),
    )
    underlying: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        info=_column_info("Underlying", "Underlying reference from Valmer SUBYACENTE."),
    )
    placement_yield: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info=_column_info("Placement Yield", "Placement yield from REND. COLOCACION."),
    )
    placement_spread: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info=_column_info("Placement Spread", "Placement spread from STCOLOCACION."),
    )
    coupon_frequency: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        info=_column_info("Coupon Frequency", "Coupon frequency from Valmer FREC CPN."),
    )
    coupon_rate: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info=_column_info("Coupon Rate", "Coupon rate from Valmer TASA CUPON."),
    )
    coupon_rule: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        info=_column_info("Coupon Rule", "Coupon rule from Valmer REGLA CUPON."),
    )
    coupons_at_issue: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        info=_column_info(
            "Coupons At Issue",
            "Coupon count at issuance from Valmer CUPONES EMISION.",
        ),
    )


def ensure_valmer_asset_detail_runtime(
    *,
    timeout: int | float | tuple[float, float] | None = None,
):
    """Verify the Valmer detail table is attached in the active markets runtime."""

    global _VALMER_ASSET_DETAILS_CONTEXT
    if _VALMER_ASSET_DETAILS_CONTEXT is not None:
        return _VALMER_ASSET_DETAILS_CONTEXT

    from msm.bootstrap import get_runtime
    from msm.repositories.base import MarketsRepositoryContext

    try:
        markets_runtime = get_runtime()
    except RuntimeError as exc:
        raise RuntimeError(
            "ValmerAssetDetailsTable requires the project bootstrap entry point. "
            "Run valmer_connectors.instruments.bootstrap.bootstrap_runtime() once during "
            "application initialization before Valmer detail row operations."
        ) from exc
    try:
        markets_runtime.table(ValmerAssetDetailsTable).meta_table_uid
    except ValueError as exc:
        raise RuntimeError(
            "ValmerAssetDetailsTable is not present in the active markets runtime. "
            "Attach it through valmer_connectors.instruments.bootstrap.bootstrap_runtime(); row "
            "operations must not register MetaTables implicitly."
        ) from exc
    _VALMER_ASSET_DETAILS_CONTEXT = MarketsRepositoryContext(
        timeout=timeout,
        namespace=markets_runtime.namespace,
    )
    return _VALMER_ASSET_DETAILS_CONTEXT


def ensure_valmer_asset_detail_schemas(
    *,
    timeout: int | float | tuple[float, float] | None = None,
):
    """Compatibility wrapper for the old schema-oriented helper name."""

    return ensure_valmer_asset_detail_runtime(timeout=timeout)


def resolve_valmer_asset_details(
    asset_uids: list[Any],
    *,
    batch_size: int | None = None,
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, dict[str, Any]]:
    """Resolve latest Valmer detail rows keyed by Valmer unique identifier."""

    normalized_asset_uids = [
        asset_uid for asset_uid in asset_uids if not pd.isna(asset_uid)
    ]
    if not normalized_asset_uids:
        return {}

    from msm.api.base import operation_result_rows
    from msm.repositories.crud import search_model

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    details: dict[str, dict[str, Any]] = {}
    resolved_batch_size = resolve_valmer_meta_operation_batch_size(batch_size)
    for batch in _batches(normalized_asset_uids, resolved_batch_size):
        result = search_model(
            context,
            model=ValmerAssetDetailsTable,
            in_filters={"asset_uid": batch},
            limit=len(batch),
        )
        for row in operation_result_rows(result):
            unique_identifier = row.get("valmer_unique_identifier")
            if unique_identifier:
                details[str(unique_identifier)] = row
    return details


def resolve_valmer_asset_detail_versions(
    asset_uids: list[Any],
    *,
    batch_size: int | None = None,
    timeout: int | float | tuple[float, float] | None = None,
    logger=None,
) -> dict[str, ValmerAssetDetailVersion]:
    """Resolve only Valmer detail freshness fields keyed by unique identifier."""

    normalized_asset_uids = _normalize_asset_uids(asset_uids)
    if not normalized_asset_uids:
        return {}

    from msm.api.base import operation_result_rows
    from msm.repositories.base import compile_markets_statement, execute_markets_operation

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    versions: dict[str, ValmerAssetDetailVersion] = {}
    resolved_batch_size = resolve_valmer_meta_operation_batch_size(batch_size)
    batches = _batches(normalized_asset_uids, resolved_batch_size)
    if logger is not None:
        logger.info(
            "Resolving "
            f"{len(normalized_asset_uids)} Valmer asset detail versions in "
            f"{len(batches)} batches of up to {resolved_batch_size}."
        )
    processed = 0
    for batch in batches:
        statement = (
            select(
                ValmerAssetDetailsTable.asset_uid.label("asset_uid"),
                ValmerAssetDetailsTable.valmer_unique_identifier.label(
                    "valmer_unique_identifier"
                ),
                ValmerAssetDetailsTable.details_asof.label("details_asof"),
            )
            .where(ValmerAssetDetailsTable.asset_uid.in_(batch))
            .limit(len(batch))
        )
        operation = compile_markets_statement(
            statement,
            context=context,
            operation="select",
            models=[ValmerAssetDetailsTable],
            access="read",
        )
        result = execute_markets_operation(operation, context=context)
        for row in operation_result_rows(result):
            unique_identifier = row.get("valmer_unique_identifier")
            asset_uid = row.get("asset_uid")
            if not unique_identifier or asset_uid in (None, ""):
                continue
            versions[str(unique_identifier)] = ValmerAssetDetailVersion(
                asset_uid=_uuid_value(asset_uid),
                valmer_unique_identifier=str(unique_identifier),
                details_asof=_normalize_detail_datetime(row.get("details_asof")),
            )
        previous = processed
        processed += len(batch)
        if logger is not None:
            _log_progress(
                logger,
                label="Resolving Valmer asset detail versions",
                previous=previous,
                completed=processed,
                total=len(normalized_asset_uids),
            )
    if logger is not None:
        logger.info(
            "Resolved Valmer asset detail versions: "
            f"{len(versions)} existing, "
            f"{len(normalized_asset_uids) - len(versions)} missing."
        )
    return versions


def upsert_valmer_asset_details(
    df_latest: pd.DataFrame,
    assets_by_unique_identifier: Mapping[str, Any],
    *,
    timeout: int | float | tuple[float, float] | None = None,
    logger=None,
) -> dict[str, dict[str, Any]]:
    """Upsert latest 1:1 Valmer detail rows for registered assets."""

    if df_latest.empty or not assets_by_unique_identifier:
        return {}

    from msm.api.base import operation_result_rows
    from msm.repositories.crud import bulk_upsert_model

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    latest_rows = (
        df_latest[df_latest["unique_identifier"].notna()]
        .drop_duplicates("unique_identifier", keep="last")
        .set_index("unique_identifier", drop=False)
    )

    upserted: dict[str, dict[str, Any]] = {}
    values_by_identifier: list[tuple[str, dict[str, Any]]] = []
    for unique_identifier, row in latest_rows.iterrows():
        asset = assets_by_unique_identifier.get(str(unique_identifier))
        if asset is None:
            continue
        values_by_identifier.append(
            (
                str(unique_identifier),
                build_valmer_asset_detail_values(row, asset.uid),
            )
        )

    total = len(values_by_identifier)
    resolved_batch_size = resolve_valmer_asset_upsert_batch_size()
    existing_details = resolve_valmer_asset_detail_versions(
        [values["asset_uid"] for _, values in values_by_identifier],
        timeout=timeout,
        logger=logger,
    )
    values_to_upsert: list[tuple[str, dict[str, Any]]] = []
    skipped_not_newer = 0
    for unique_identifier, values in values_by_identifier:
        existing = existing_details.get(unique_identifier)
        if existing is not None and not _is_candidate_detail_newer(existing, values):
            skipped_not_newer += 1
            continue
        values_to_upsert.append((unique_identifier, values))

    batches = _batches(values_to_upsert, resolved_batch_size)
    if logger is not None:
        skipped = len(latest_rows) - total
        logger.info(
            "Prepared Valmer asset detail sync: "
            f"{total} candidate rows, {len(values_to_upsert)} missing/changed rows, "
            f"{skipped_not_newer} skipped because source date is not newer, "
            f"{skipped} rows skipped without assets."
        )
        if values_to_upsert:
            logger.info(
                f"Upserting {len(values_to_upsert)} Valmer asset detail rows in "
                f"{len(batches)} batches of up to {resolved_batch_size}."
            )
        else:
            logger.info("No Valmer asset detail upserts required.")
    processed = 0
    for batch_index, batch in enumerate(batches, start=1):
        result = bulk_upsert_model(
            context,
            model=ValmerAssetDetailsTable,
            values=[values for _, values in batch],
            conflict_columns=("asset_uid",),
        )
        for result_row in operation_result_rows(result):
            unique_identifier = result_row.get("valmer_unique_identifier")
            if unique_identifier:
                upserted[str(unique_identifier)] = result_row
        previous = processed
        processed += len(batch)
        if logger is not None:
            logger.info(
                "Completed Valmer asset detail bulk upsert batch "
                f"{batch_index}/{len(batches)} ({len(batch)} rows, "
                f"{processed}/{len(values_to_upsert)} total)."
            )
            _log_progress(
                logger,
                label="Upserting Valmer asset detail rows",
                previous=previous,
                completed=processed,
                total=len(values_to_upsert),
            )
    return upserted


def build_valmer_asset_detail_values(
    row: Mapping[str, Any],
    asset_uid: uuid.UUID | str,
) -> dict[str, Any]:
    return {
        "asset_uid": asset_uid,
        "valmer_unique_identifier": _string_value(row, "unique_identifier"),
        "details_asof": _datetime_value(row, "fecha", format="%Y%m%d"),
        "security_type": _string_value(row, "tipovalor"),
        "issuer": _string_value(row, "emisora"),
        "series": _string_value(row, "serie"),
        "full_name": _string_value(row, "nombrecompleto"),
        "sector": _string_value(row, "sector"),
        "issued_amount": _float_value(row, "montoemitido"),
        "issue_date": _date_value(row, "fechaemision"),
        "issue_term": _float_value(row, "plazoemision"),
        "maturity_date": _date_value(row, "fechavcto"),
        "face_value": _float_value(row, "valornominal"),
        "issue_currency": _string_value(row, "monedaemision"),
        "underlying": _string_value(row, "subyacente"),
        "placement_yield": _float_value(row, "rendcolocacion"),
        "placement_spread": _float_value(row, "stcolocacion"),
        "coupon_frequency": _string_value(row, "freccpn"),
        "coupon_rate": _float_value(row, "tasacupon"),
        "coupon_rule": _string_value(row, "reglacupon"),
        "coupons_at_issue": _int_value(row, "cuponesemision"),
    }


def _raw_value(row: Mapping[str, Any], field: str) -> Any:
    if field not in row:
        return None
    value = row[field]
    if pd.isna(value):
        return None
    return value


def _string_value(row: Mapping[str, Any], field: str) -> str | None:
    value = _raw_value(row, field)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_value(row: Mapping[str, Any], field: str) -> float | None:
    value = _raw_value(row, field)
    if value is None:
        return None
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _int_value(row: Mapping[str, Any], field: str) -> int | None:
    value = _raw_value(row, field)
    if value is None:
        return None
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return int(numeric)


def _date_value(row: Mapping[str, Any], field: str) -> dt.date | None:
    value = _datetime_value(row, field)
    if value is None:
        return None
    return value.date()


def _datetime_value(
    row: Mapping[str, Any],
    field: str,
    *,
    format: str | None = None,
) -> dt.datetime | None:
    value = _raw_value(row, field)
    if value is None:
        return None
    parse_value = str(value) if format is not None else value
    timestamp = pd.to_datetime(parse_value, format=format, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    return timestamp.to_pydatetime()


def _is_candidate_detail_newer(
    existing: ValmerAssetDetailVersion | Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> bool:
    if isinstance(existing, ValmerAssetDetailVersion):
        raw_existing_asof = existing.details_asof
    else:
        raw_existing_asof = existing.get("details_asof")
    existing_asof = _normalize_detail_datetime(raw_existing_asof)
    candidate_asof = _normalize_detail_datetime(candidate.get("details_asof"))
    if candidate_asof is None:
        return False
    if existing_asof is None:
        return True
    return candidate_asof > existing_asof


def _normalize_detail_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        timestamp = value.to_pydatetime()
    elif isinstance(value, dt.datetime):
        timestamp = value
    elif isinstance(value, dt.date):
        timestamp = dt.datetime.combine(value, dt.time.min)
    else:
        timestamp = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(timestamp):
            return None
        timestamp = timestamp.to_pydatetime()
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=dt.timezone.utc)
    return timestamp.astimezone(dt.timezone.utc)


def _normalize_asset_uids(asset_uids: list[Any]) -> list[Any]:
    seen: set[str] = set()
    normalized: list[Any] = []
    for asset_uid in asset_uids:
        if pd.isna(asset_uid):
            continue
        key = str(asset_uid)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(asset_uid)
    return normalized


def _uuid_value(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _batches(values: list[Any], batch_size: int) -> list[list[Any]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [
        values[start : start + batch_size]
        for start in range(0, len(values), batch_size)
    ]


def _log_progress(
    logger,
    *,
    label: str,
    previous: int,
    completed: int,
    total: int,
) -> None:
    if not total:
        return
    thresholds = {
        max(1, (total * milestone + 99) // 100)
        for milestone in (1, 5, 20, 40, 60, 80, 100)
    }
    for threshold in sorted(thresholds):
        if previous < threshold <= completed:
            percent = min(100, round((completed / total) * 100))
            logger.info(f"{label}: {percent}% complete ({completed}/{total}).")


__all__ = [
    "VALMER_ASSET_DETAIL_VECTOR_COLUMNS",
    "VALMER_ASSET_DETAIL_SOURCE_COLUMNS",
    "ValmerAssetDetailVersion",
    "ValmerAssetDetailsTable",
    "build_valmer_asset_detail_values",
    "ensure_valmer_asset_detail_runtime",
    "ensure_valmer_asset_detail_schemas",
    "resolve_valmer_asset_detail_versions",
    "resolve_valmer_asset_details",
    "upsert_valmer_asset_details",
]
