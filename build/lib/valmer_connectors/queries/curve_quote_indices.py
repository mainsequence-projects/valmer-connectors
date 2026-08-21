"""Strict reads for persisted Valmer quote and upstream curve observations."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd
from msm.api.base import operation_result_rows
from msm.repositories.base import (
    MarketsRepositoryContext,
    compile_markets_statement,
    execute_markets_operation,
)
from msm_pricing.data_nodes.curves.key_nodes import decompress_key_nodes_from_string
from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
from sqlalchemy import and_, select

from valmer_connectors.data_nodes.canonical_index_values import (
    DailyIndexValuesStorage,
)

# NOTE: canonical IndexValuesTS dropped the `unit` column in ms-markets >=0.0.98;
# unit provenance now rides in metadata_json (see repo status notes 2026-07-25).
_QUOTE_COLUMNS = [
    "time_index",
    "index_identifier",
    "value",
    "definition_uid",
    "observation_status",
    "source_as_of",
    "metadata_json",
]


class ValmerCurveQuoteSnapshotError(RuntimeError):
    """Raised when persisted curve inputs are incomplete or inconsistent."""


def load_valmer_curve_quote_snapshot(
    *,
    storage_table=DailyIndexValuesStorage,
    time_index: Any,
    source_families: Iterable[str],
    timeout: int | float | tuple[float, float] | None = None,
) -> pd.DataFrame:
    """Read one exact-date persisted Valmer quote snapshot with no latest fallback."""

    target = _utc_midnight(time_index)
    frame = _read_quote_rows(
        storage_table=storage_table,
        time_index=target,
        timeout=timeout,
    )
    return validate_valmer_curve_quote_snapshot(
        frame,
        time_index=target,
        source_families=source_families,
    )


def load_valmer_curve_quote_snapshots(
    *,
    storage_table=DailyIndexValuesStorage,
    source_families: Iterable[str],
    after_time_index: Any | None = None,
    timeout: int | float | tuple[float, float] | None = None,
) -> pd.DataFrame:
    """Read every complete requested Valmer snapshot after an optional cutoff."""

    frame = _read_quote_rows(
        storage_table=storage_table,
        after_time_index=(
            None if after_time_index is None else _utc_midnight(after_time_index)
        ),
        timeout=timeout,
    )
    if frame.empty:
        return frame
    validated = []
    for time_index, snapshot in frame.groupby("time_index", sort=True):
        validated.append(
            validate_valmer_curve_quote_snapshot(
                snapshot,
                time_index=time_index,
                source_families=source_families,
            )
        )
    return pd.concat(validated, ignore_index=True).sort_values(
        ["time_index", "index_identifier"]
    )


def validate_valmer_curve_quote_snapshot(
    frame: pd.DataFrame,
    *,
    time_index: Any,
    source_families: Iterable[str],
) -> pd.DataFrame:
    """Validate exact date, identity, status, units, and requested family coverage."""

    target = _utc_midnight(time_index)
    requested = tuple(str(item) for item in source_families)
    if not requested:
        raise ValmerCurveQuoteSnapshotError("source_families cannot be empty.")
    if frame.empty:
        raise ValmerCurveQuoteSnapshotError(
            f"No persisted Valmer quote rows exist for {target.isoformat()}."
        )
    working = frame.copy()
    working["time_index"] = pd.to_datetime(working["time_index"], utc=True)
    if set(working["time_index"]) != {target}:
        raise ValmerCurveQuoteSnapshotError("Valmer quote snapshot contains mixed dates.")
    if working[["time_index", "index_identifier"]].duplicated().any():
        raise ValmerCurveQuoteSnapshotError("Valmer quote snapshot contains duplicate rows.")
    if not working["index_identifier"].astype(str).str.startswith(
        "VALMER_CURVE_QUOTE."
    ).all():
        raise ValmerCurveQuoteSnapshotError("Snapshot contains a non-Valmer quote Index.")
    if not working["observation_status"].isin({"ready", "partial"}).all():
        raise ValmerCurveQuoteSnapshotError(
            "Valmer quote snapshot contains a non-ready observation."
        )
    if working["definition_uid"].notna().any():
        raise ValmerCurveQuoteSnapshotError(
            "Source-published Valmer quote observations must not have definition_uid."
        )

    working["source_family"] = working["metadata_json"].map(
        lambda value: value.get("source_family") if isinstance(value, dict) else None
    )
    selected = working.loc[working["source_family"].isin(requested)].copy()
    missing = sorted(set(requested).difference(selected["source_family"]))
    if missing:
        raise ValmerCurveQuoteSnapshotError(
            f"Valmer quote snapshot is missing source families {missing!r}."
        )
    expected_units = {
        "fx_spot": "mxn_per_usd",
        "fx_forward": "mxn_per_usd",
        "sofr_future": "price",
        "tiie_ois": "decimal",
        "sofr_ois": "decimal",
        "fedfunds_ois": "decimal",
        "fedfunds_sofr_basis": "decimal",
        "tiie_sofr_xccy_basis": "decimal",
    }
    units = working.loc[selected.index, "metadata_json"].map(
        lambda value: value.get("unit") if isinstance(value, dict) else None
    )
    invalid = selected.loc[
        [
            unit is not None and unit != expected_units.get(family)
            for unit, family in zip(units, selected["source_family"], strict=True)
        ]
    ]
    if not invalid.empty:
        raise ValmerCurveQuoteSnapshotError(
            "Valmer quote snapshot contains family/unit mismatches."
        )
    return selected[_QUOTE_COLUMNS + ["source_family"]].sort_values(
        "index_identifier"
    )


def load_discount_curve_key_nodes(
    *,
    storage_table=DiscountCurvesStorage,
    time_index: Any,
    curve_identifiers: Iterable[str],
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Read and decode exact-date upstream curve key nodes for XCCY construction."""

    target = _utc_midnight(time_index)
    identifiers = tuple(str(value) for value in curve_identifiers)
    table = storage_table.__table__
    context = MarketsRepositoryContext(timeout=timeout)
    statement = (
        select(
            table.c.time_index,
            table.c.curve_identifier,
            table.c.key_nodes,
        )
        .where(
            and_(
                table.c.time_index == target,
                table.c.curve_identifier.in_(identifiers),
            )
        )
        .order_by(table.c.curve_identifier)
    )
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[storage_table],
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    rows = list(operation_result_rows(result))
    if len(rows) != len(identifiers):
        found = {str(row["curve_identifier"]) for row in rows}
        raise ValmerCurveQuoteSnapshotError(
            f"Missing upstream curves at {target.isoformat()}: "
            f"{sorted(set(identifiers).difference(found))!r}."
        )
    decoded = {}
    for row in rows:
        key_nodes = decompress_key_nodes_from_string(str(row["key_nodes"]))
        if not isinstance(key_nodes, list):
            raise ValmerCurveQuoteSnapshotError("Upstream curve key_nodes must be a list.")
        decoded[str(row["curve_identifier"])] = key_nodes
    return decoded


def _read_quote_rows(
    *,
    storage_table,
    time_index: pd.Timestamp | None = None,
    after_time_index: pd.Timestamp | None = None,
    timeout: int | float | tuple[float, float] | None = None,
) -> pd.DataFrame:
    table = storage_table.__table__
    filters = [table.c.index_identifier.like("VALMER_CURVE_QUOTE.%")]
    if time_index is not None:
        filters.append(table.c.time_index == time_index)
    if after_time_index is not None:
        filters.append(table.c.time_index > after_time_index)
    context = MarketsRepositoryContext(timeout=timeout)
    statement = (
        select(*(table.c[name] for name in _QUOTE_COLUMNS))
        .where(*filters)
        .order_by(table.c.time_index, table.c.index_identifier)
    )
    operation = compile_markets_statement(
        statement,
        context=context,
        operation="select",
        models=[storage_table],
        access="read",
    )
    result = execute_markets_operation(operation, context=context)
    return pd.DataFrame(list(operation_result_rows(result)), columns=_QUOTE_COLUMNS)


def _utc_midnight(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    timestamp = (
        timestamp.tz_localize("UTC")
        if timestamp.tzinfo is None
        else timestamp.tz_convert("UTC")
    )
    return timestamp.normalize()


__all__ = [
    "ValmerCurveQuoteSnapshotError",
    "load_discount_curve_key_nodes",
    "load_valmer_curve_quote_snapshot",
    "load_valmer_curve_quote_snapshots",
    "validate_valmer_curve_quote_snapshot",
]
