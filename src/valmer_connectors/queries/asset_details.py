from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd
from sqlalchemy import or_, select

from valmer_connectors.meta_tables.valmer_asset_details import (
    ValmerAssetDetailsTable,
    ensure_valmer_asset_detail_runtime,
)
from valmer_connectors.queries._normalization import (
    clean_valmer_identifiers,
    string_or_none,
)

# Projection of the Valmer detail columns, in the public frame's column order
# after asset_uid and asset_table_identifier.
_DETAIL_PROJECTION: tuple[tuple[str, str], ...] = (
    ("asset_identifier", "valmer_unique_identifier"),
    ("valmer_unique_identifier", "valmer_unique_identifier"),
    ("valmer_security_type", "security_type"),
    ("valmer_issuer", "issuer"),
    ("valmer_series", "series"),
    ("valmer_full_name", "full_name"),
    ("valmer_issue_date", "issue_date"),
    ("valmer_maturity_date", "maturity_date"),
    ("maturity_date", "maturity_date"),
    ("valmer_face_value", "face_value"),
    ("valmer_coupon_frequency", "coupon_frequency"),
    ("valmer_coupon_rate", "coupon_rate"),
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
    """Valmer detail rows whose Valmer or Markets asset identifier is requested.

    The Valmer detail table is read with compiled SQL on Valmer's own runtime;
    the Markets assets are read with ``Asset.filter`` row calls (Markets API
    calls under the remote backend, ms-markets ADR 0047) and merged in Python,
    instead of joining ``AssetTable``.
    """

    from msm.api.assets import Asset
    from msm.api.base import operation_result_rows
    from msm.repositories.base import compile_markets_statement, execute_markets_operation

    context = ensure_valmer_asset_detail_runtime(timeout=timeout)
    # Assets named by their Markets identifier, which may differ from the Valmer one.
    assets = {
        str(asset.uid): asset
        for asset in Asset.filter(unique_identifier__in=list(identifiers), limit=len(identifiers))
    }
    details = ValmerAssetDetailsTable
    condition = details.valmer_unique_identifier.in_(list(identifiers))
    if assets:
        condition = or_(condition, details.asset_uid.in_([asset.uid for asset in assets.values()]))
    statement = (
        select(
            details.asset_uid.label("detail_asset_uid"),
            *(
                getattr(details, column).label(label)
                for label, column in _DETAIL_PROJECTION
            ),
        )
        .where(condition)
        .order_by(details.valmer_unique_identifier)
        .limit(len(identifiers))
    )
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
    )
    result = execute_markets_operation(operation, context=context)
    detail_rows = [dict(row) for row in operation_result_rows(result)]

    # The Markets identifier of every other detail row, in one batch read.
    missing = sorted(
        {str(row["detail_asset_uid"]) for row in detail_rows if row.get("detail_asset_uid")}
        - set(assets)
    )
    if missing:
        assets.update(
            {str(asset.uid): asset for asset in Asset.filter(uid__in=missing, limit=len(missing))}
        )
    rows: list[dict[str, Any]] = []
    for row in detail_rows:
        asset = assets.get(str(row.pop("detail_asset_uid", None)))
        rows.append(
            {
                "asset_uid": None if asset is None else asset.uid,
                "asset_table_identifier": None if asset is None else asset.unique_identifier,
                **row,
            }
        )
    return rows


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
