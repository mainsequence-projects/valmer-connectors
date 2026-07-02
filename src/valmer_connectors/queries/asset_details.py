from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd
from msm.models.assets import AssetTable
from sqlalchemy import or_, select

from valmer_connectors.meta_tables.valmer_asset_details import (
    ValmerAssetDetailsTable,
    ensure_valmer_asset_detail_runtime,
)
from valmer_connectors.queries._normalization import (
    clean_valmer_identifiers,
    string_or_none,
)


def read_valmer_asset_detail_alias_frame(
    asset_identifiers: Iterable[object],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> pd.DataFrame:
    """Return Valmer detail rows expanded to both Valmer and AssetTable aliases."""

    identifiers = clean_valmer_identifiers(asset_identifiers)
    if not identifiers:
        return pd.DataFrame()
    rows = _read_valmer_asset_detail_rows(identifiers, timeout=timeout)
    return expand_valmer_asset_detail_alias_frame(rows)


def read_valmer_asset_detail_maturity_fields(
    asset_identifiers: Iterable[object],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> pd.DataFrame:
    """Return Valmer static maturity fields for asset identifiers."""

    return read_valmer_asset_detail_alias_frame(asset_identifiers, timeout=timeout)


def resolve_valmer_detail_identifier_aliases(
    asset_identifiers: Iterable[object],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, str]:
    """Map each accepted asset identifier alias to its Valmer unique identifier."""

    frame = read_valmer_asset_detail_alias_frame(asset_identifiers, timeout=timeout)
    if frame.empty:
        return {}
    return {
        str(row.asset_identifier): str(row.valmer_unique_identifier)
        for row in frame.itertuples(index=False)
        if string_or_none(getattr(row, "asset_identifier", None)) is not None
        and string_or_none(getattr(row, "valmer_unique_identifier", None)) is not None
    }


def _read_valmer_asset_detail_rows(
    identifiers: Sequence[str],
    *,
    timeout: int | float | tuple[float, float] | None,
) -> list[dict[str, Any]]:
    from msm.api.base import operation_result_rows
    from msm.repositories.base import compile_markets_statement, execute_markets_operation

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    statement = (
        select(
            AssetTable.uid.label("asset_uid"),
            AssetTable.unique_identifier.label("asset_table_identifier"),
            ValmerAssetDetailsTable.valmer_unique_identifier.label("asset_identifier"),
            ValmerAssetDetailsTable.valmer_unique_identifier.label("valmer_unique_identifier"),
            ValmerAssetDetailsTable.security_type.label("valmer_security_type"),
            ValmerAssetDetailsTable.issuer.label("valmer_issuer"),
            ValmerAssetDetailsTable.series.label("valmer_series"),
            ValmerAssetDetailsTable.full_name.label("valmer_full_name"),
            ValmerAssetDetailsTable.issue_date.label("valmer_issue_date"),
            ValmerAssetDetailsTable.maturity_date.label("valmer_maturity_date"),
            ValmerAssetDetailsTable.maturity_date.label("maturity_date"),
            ValmerAssetDetailsTable.face_value.label("valmer_face_value"),
            ValmerAssetDetailsTable.coupon_frequency.label("valmer_coupon_frequency"),
            ValmerAssetDetailsTable.coupon_rate.label("valmer_coupon_rate"),
        )
        .select_from(ValmerAssetDetailsTable)
        .outerjoin(AssetTable, ValmerAssetDetailsTable.asset_uid == AssetTable.uid)
        .where(
            or_(
                ValmerAssetDetailsTable.valmer_unique_identifier.in_(identifiers),
                AssetTable.unique_identifier.in_(identifiers),
            )
        )
        .order_by(ValmerAssetDetailsTable.valmer_unique_identifier)
        .limit(len(identifiers))
    )
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[ValmerAssetDetailsTable, AssetTable],
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    return [dict(row) for row in operation_result_rows(result)]


def expand_valmer_asset_detail_alias_frame(
    rows: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    """Expand Valmer detail rows so AssetTable identifiers also resolve."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    alias_rows: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        asset_identifier = string_or_none(row.get("asset_identifier"))
        asset_table_identifier = string_or_none(row.get("asset_table_identifier"))
        if asset_identifier is None:
            if asset_table_identifier is None:
                continue
            row["asset_identifier"] = asset_table_identifier
            alias_rows.append(row)
            continue
        alias_rows.append(row)
        if asset_table_identifier and asset_table_identifier != asset_identifier:
            alias_row = dict(row)
            alias_row["asset_identifier"] = asset_table_identifier
            alias_rows.append(alias_row)
    if not alias_rows:
        return pd.DataFrame()
    return (
        pd.DataFrame(alias_rows)
        .drop_duplicates("asset_identifier", keep="first")
        .reset_index(drop=True)
    )
