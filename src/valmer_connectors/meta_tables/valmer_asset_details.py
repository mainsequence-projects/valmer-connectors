from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Mapping
from typing import Any

import pandas as pd
from mainsequence.meta_tables import MetaTableForeignKey
from msm.base import (
    MarketsBase,
    MarketsMetaTableMixin,
    markets_index_name,
    markets_table_args,
)
from msm.models.assets import AssetTable
from sqlalchemy import Date, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

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


class ValmerAssetDetailsTable(MarketsMetaTableMixin, MarketsBase):
    """Latest Valmer vendor details linked 1:1 to the canonical Asset row."""

    __metatable_identifier__ = "ValmerAssetDetails"
    __table_args__ = markets_table_args(
        __metatable_identifier__,
        Index(
            markets_index_name(__metatable_identifier__, "valmer_unique_identifier", unique=True),
            "valmer_unique_identifier",
            unique=True,
        ),
        Index(
            markets_index_name(__metatable_identifier__, "issuer"),
            "issuer",
        ),
        Index(
            markets_index_name(__metatable_identifier__, "maturity_date"),
            "maturity_date",
        ),
    )

    asset_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        MetaTableForeignKey(
            AssetTable,
            column="uid",
            ondelete="CASCADE",
        ),
        primary_key=True,
        nullable=False,
    )
    valmer_unique_identifier: Mapped[str] = mapped_column(String(255), nullable=False)
    details_asof: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    security_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    issuer: Mapped[str | None] = mapped_column(String(128), nullable=True)
    series: Mapped[str | None] = mapped_column(String(128), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    sector: Mapped[str | None] = mapped_column(String(255), nullable=True)
    issued_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    issue_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    issue_term: Mapped[float | None] = mapped_column(Float, nullable=True)
    maturity_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    face_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    issue_currency: Mapped[str | None] = mapped_column(String(32), nullable=True)
    underlying: Mapped[str | None] = mapped_column(String(255), nullable=True)
    placement_yield: Mapped[float | None] = mapped_column(Float, nullable=True)
    placement_spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    coupon_frequency: Mapped[str | None] = mapped_column(String(64), nullable=True)
    coupon_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    coupon_rule: Mapped[str | None] = mapped_column(String(255), nullable=True)
    coupons_at_issue: Mapped[int | None] = mapped_column(Integer, nullable=True)


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
    batch_size: int = 5000,
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, dict[str, Any]]:
    """Resolve latest Valmer detail rows keyed by Valmer unique identifier."""

    normalized_asset_uids = [asset_uid for asset_uid in asset_uids if not pd.isna(asset_uid)]
    if not normalized_asset_uids:
        return {}

    from msm.api.base import operation_result_rows
    from msm.repositories.crud import search_model

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    details: dict[str, dict[str, Any]] = {}
    for batch in _batches(normalized_asset_uids, batch_size):
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


def upsert_valmer_asset_details(
    df_latest: pd.DataFrame,
    assets_by_unique_identifier: Mapping[str, Any],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, dict[str, Any]]:
    """Upsert latest 1:1 Valmer detail rows for registered assets."""

    if df_latest.empty or not assets_by_unique_identifier:
        return {}

    from msm.api.base import operation_result_rows
    from msm.repositories.crud import upsert_model

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    latest_rows = (
        df_latest[df_latest["unique_identifier"].notna()]
        .drop_duplicates("unique_identifier", keep="last")
        .set_index("unique_identifier")
    )

    upserted: dict[str, dict[str, Any]] = {}
    for unique_identifier, row in latest_rows.iterrows():
        asset = assets_by_unique_identifier.get(str(unique_identifier))
        if asset is None:
            continue

        result = upsert_model(
            context,
            model=ValmerAssetDetailsTable,
            values=build_valmer_asset_detail_values(row, asset.uid),
            conflict_columns=("asset_uid",),
        )
        rows = operation_result_rows(result)
        if rows:
            upserted[str(unique_identifier)] = rows[0]
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


def _batches(values: list[Any], batch_size: int) -> list[list[Any]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [values[start : start + batch_size] for start in range(0, len(values), batch_size)]


__all__ = [
    "VALMER_ASSET_DETAIL_VECTOR_COLUMNS",
    "VALMER_ASSET_DETAIL_SOURCE_COLUMNS",
    "ValmerAssetDetailsTable",
    "build_valmer_asset_detail_values",
    "ensure_valmer_asset_detail_runtime",
    "ensure_valmer_asset_detail_schemas",
    "resolve_valmer_asset_details",
    "upsert_valmer_asset_details",
]
