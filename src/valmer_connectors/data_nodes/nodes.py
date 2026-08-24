import os
import re
import shutil
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, List, Literal, Union
from urllib.parse import urlparse

import pandas as pd
from msm.api.assets import Asset as MarketsAsset
from msm.api.base import operation_result_rows
from msm.constants import ASSET_TYPE_BOND
from msm.data_nodes import AssetIndexedDataNode, AssetIndexedDataNodeConfiguration
from msm.data_nodes.assets import AssetSnapshot
from msm.repositories.base import compile_markets_statement, execute_markets_operation
from msm.settings import ASSET_IDENTIFIER_DIMENSION
from msm_pricing.api import add_many_pricing_details
from msm_pricing.api.pricing_details import AssetCurrentPricingDetails
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from tqdm import tqdm

from mainsequence.client.metatables import MetaTable
from mainsequence.client.models_foundry import Artifact
from valmer_connectors.asset_classification import classify_valmer_asset_type
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
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
    VALMER_ASSET_DETAIL_SOURCE_COLUMNS,
    upsert_valmer_asset_details,
)
from valmer_connectors.settings import (
    resolve_valmer_force_pricing_details_patch,
    resolve_valmer_meta_operation_batch_size,
    resolve_valmer_pricing_details_batch_size,
    resolve_valmer_vector_bypass_cursor_filter,
    resolve_valmer_vector_local_copy_chunk_size,
)

_VALMER_EXCEL_NA_VALUES = (
    "",
    "#N/A",
    "#N/A N/A",
    "#NA",
    "-1.#IND",
    "-1.#QNAN",
    "-NaN",
    "-nan",
    "1.#IND",
    "1.#QNAN",
    "<NA>",
    "N/A",
    "NULL",
    "NaN",
    "None",
    "n/a",
    "nan",
    "null",
)


@dataclass(frozen=True)
class ValmerColumnSpec:
    source_name: str | None
    column_name: str
    dtype: str
    transform: str
    label: str
    description: str


def _coerce_valmer_series(series: pd.Series, transform: str) -> pd.Series:
    if transform == "string":
        return series.astype("string")

    if transform == "float":
        return pd.to_numeric(series, errors="coerce").astype("float64")

    if transform == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    if transform == "percent":
        cleaned = (
            series.astype("string")
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        return pd.to_numeric(cleaned, errors="coerce").astype("float64")

    if transform == "datetime":
        return _as_utc_ns(pd.to_datetime(series, errors="coerce", utc=True))

    if transform == "date_ymd":
        return _parse_valmer_valuation_dates(series)

    raise ValueError(f"Unsupported Valmer transform: {transform}")


def _as_utc_ns(series: pd.Series) -> pd.Series:
    return pd.Series(series, index=series.index).astype("datetime64[ns, UTC]")


def _parse_valmer_valuation_dates(series: pd.Series) -> pd.Series:
    strict_ymd = pd.to_datetime(
        series.astype("string"),
        format="%Y%m%d",
        errors="coerce",
        utc=True,
    )
    flexible = pd.to_datetime(series, errors="coerce", utc=True)
    return _as_utc_ns(strict_ymd.fillna(flexible))


def _as_utc_ns_timestamp(value: object) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return pd.Series([timestamp]).astype("datetime64[ns, UTC]").iloc[0]


def _pricing_detail_face_value(instrument_dump: dict) -> object:
    if not isinstance(instrument_dump, dict):
        return None
    if "face_value" in instrument_dump:
        return instrument_dump.get("face_value")
    wrapped_instrument = instrument_dump.get("instrument")
    if isinstance(wrapped_instrument, dict):
        return wrapped_instrument.get("face_value")
    return None


def _summarize_uids(uids: list[str], *, limit: int = 10) -> str:
    if not uids:
        return ""
    sample = ", ".join(uids[:limit])
    if len(uids) > limit:
        sample = f"{sample}, ... (+{len(uids) - limit} more)"
    return sample


def _summarize_uid_asset_pairs(
    uids: list[str],
    assets: dict[str, MarketsAsset],
    *,
    limit: int = 10,
) -> str:
    pairs = [
        f"{uid} ({getattr(assets[uid], 'uid', 'missing-asset')})"
        for uid in uids[:limit]
        if uid in assets
    ]
    sample = ", ".join(pairs)
    if len(uids) > limit:
        sample = f"{sample}, ... (+{len(uids) - limit} more)"
    return sample


def _summarize_failure_reasons(
    failures: dict[str, str],
    *,
    limit: int = 5,
) -> str:
    if not failures:
        return ""
    pairs = [
        f"{uid}: {reason}"
        for uid, reason in list(failures.items())[:limit]
    ]
    sample = "; ".join(pairs)
    if len(failures) > limit:
        sample = f"{sample}; ... (+{len(failures) - limit} more)"
    return sample


_PRICING_ADAPTER_REQUIRED_FIELDS: tuple[str, ...] = (
    "fecha",
    "tipovalor",
    "emisora",
    "serie",
    "subyacente",
    "monedaemision",
    "fechaemision",
    "fechavcto",
    "valornominalactualizado",
    "reglacupon",
    "cuponesemision",
)

_PRICING_ADAPTER_CONTEXT_FIELDS: tuple[str, ...] = (
    "fecha",
    "unique_identifier",
    "tipovalor",
    "emisora",
    "serie",
    "subyacente",
    "monedaemision",
    "fechaemision",
    "plazoemision",
    "fechavcto",
    "freccpn",
    "cuponesxcobrar",
    "diastransccpn",
    "cuponactual",
    "cuponesemision",
    "reglacupon",
    "tasacupon",
    "tasaderendimiento",
    "valornominalactualizado",
    "sobretasa",
)

_PRICING_ADAPTER_SCHEDULE_FIELDS: tuple[str, ...] = (
    "fecha",
    "fechaemision",
    "fechavcto",
    "freccpn",
    "cuponesxcobrar",
    "diastransccpn",
    "cuponactual",
    "cuponesemision",
)


def _format_pricing_adapter_value(value: object) -> str:
    if value is None:
        return "None"
    try:
        if pd.isna(value):
            return "NULL"
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def _pricing_adapter_failure_detail(row: pd.Series, exc: BaseException) -> str:
    row_index = set(row.index)
    missing_required = [
        field for field in _PRICING_ADAPTER_REQUIRED_FIELDS if field not in row_index
    ]
    null_required = [
        field
        for field in _PRICING_ADAPTER_REQUIRED_FIELDS
        if field in row_index and pd.isna(row[field])
    ]
    schedule_context = {
        field: _format_pricing_adapter_value(row[field])
        for field in _PRICING_ADAPTER_SCHEDULE_FIELDS
        if field in row_index
    }
    row_context = {
        field: _format_pricing_adapter_value(row[field])
        for field in _PRICING_ADAPTER_CONTEXT_FIELDS
        if field in row_index
    }

    parts = [f"{type(exc).__name__}: {exc}"]
    if missing_required:
        parts.append(f"missing required pricing columns={missing_required}")
    if null_required:
        parts.append(f"null required pricing fields={null_required}")
    if "Failed to insert extra dates" in str(exc):
        parts.append(
            "schedule reconciliation failed: QuantLib could not fit "
            "cuponesxcobrar coupon dates between valuation/settlement and fechavcto "
            "using freccpn; check cuponesxcobrar, diastransccpn, freccpn, fecha, "
            "fechaemision, and fechavcto"
        )
    parts.append(f"schedule_inputs={schedule_context}")
    parts.append(f"row_context={row_context}")
    return "; ".join(parts)


def _pricing_detail_failure_summary(
    *,
    missing_latest_rows: list[str],
    instrument_build_failures: dict[str, str],
    missing_assets_for_pricing: list[str],
    missing_instruments_for_pricing: list[str],
    missing_after_persist: list[str],
    assets_for_update: dict[str, MarketsAsset],
) -> str:
    parts: list[str] = []
    if missing_latest_rows:
        parts.append(
            "missing latest vector rows "
            f"({len(missing_latest_rows)}): {_summarize_uids(missing_latest_rows)}"
        )
    if instrument_build_failures:
        parts.append(
            "instrument build failures "
            f"({len(instrument_build_failures)}): "
            f"{_summarize_failure_reasons(instrument_build_failures)}"
        )
    if missing_assets_for_pricing:
        parts.append(
            "missing Asset rows "
            f"({len(missing_assets_for_pricing)}): {_summarize_uids(missing_assets_for_pricing)}"
        )
    if missing_instruments_for_pricing:
        parts.append(
            "missing instrument payloads "
            f"({len(missing_instruments_for_pricing)}): "
            f"{_summarize_uids(missing_instruments_for_pricing)}"
        )
    if missing_after_persist:
        parts.append(
            "current pricing rows missing after persist/readback "
            f"({len(missing_after_persist)}): "
            f"{_summarize_uid_asset_pairs(missing_after_persist, assets_for_update)}"
        )
    return "Failure categories: " + " | ".join(parts)


def _build_valmer_asset_snapshot_rows(
    df_latest: pd.DataFrame,
    asset_identifiers: list[str],
) -> list[dict[str, object]]:
    """Build canonical AssetSnapshot rows from latest Valmer source rows."""

    required_columns = {"unique_identifier", "fecha", "nombrecompleto"}
    if not required_columns.issubset(df_latest.columns):
        return []

    target_identifiers = set(asset_identifiers)
    if not target_identifiers:
        return []

    latest = (
        df_latest[df_latest["unique_identifier"].isin(target_identifiers)]
        .drop_duplicates("unique_identifier", keep="last")
        .copy()
    )
    rows: list[dict[str, object]] = []
    for source_row in latest.to_dict(orient="records"):
        unique_identifier = str(source_row["unique_identifier"])
        name = _clean_valmer_snapshot_name(source_row.get("nombrecompleto"))
        time_index = _valmer_snapshot_time_index(source_row.get("fecha"))
        if name is None or time_index is None:
            continue
        rows.append(
            {
                "time_index": time_index,
                ASSET_IDENTIFIER_DIMENSION: unique_identifier,
                "name": name,
            }
        )
    return rows


def _publish_valmer_asset_snapshots(
    df_latest: pd.DataFrame,
    assets: dict[str, MarketsAsset],
    *,
    logger,
) -> int:
    """Publish AssetSnapshot.name rows from Valmer NOMBRE COMPLETO values."""

    snapshot_rows = _build_valmer_asset_snapshot_rows(
        df_latest,
        list(assets.keys()),
    )
    if not snapshot_rows:
        logger.info(
            "No Valmer asset snapshot rows to publish. "
            "Source must include unique_identifier, fecha, and nombrecompleto."
        )
        return 0

    logger.info(
        f"Publishing {len(snapshot_rows)} Valmer asset snapshot rows with "
        "name mapped from NOMBRE COMPLETO."
    )
    node = AssetSnapshot()

    result = node.set_snapshots(snapshot_rows, verify_existing=False).run(force_update=True)
    if isinstance(result, tuple) and len(result) == 2:
        error_on_last_update, frame = result
    else:
        error_on_last_update, frame = False, result
    if error_on_last_update:
        message = f"Valmer asset snapshot update failed for {len(snapshot_rows)} rows."
        if isinstance(error_on_last_update, BaseException):
            raise RuntimeError(message) from error_on_last_update
        raise RuntimeError(f"{message} {error_on_last_update}")
    row_count = len(frame) if isinstance(frame, pd.DataFrame) else len(snapshot_rows)
    logger.info(f"Published {row_count} Valmer asset snapshot rows.")
    return row_count


def _clean_valmer_snapshot_name(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _valmer_snapshot_time_index(value: object) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    valuation_date = pd.to_datetime(value, utc=True)
    if pd.isna(valuation_date):
        return None
    return valuation_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


def _source_vector_time_index(fecha: pd.Series) -> pd.Series:
    valuation_date = _parse_valmer_valuation_dates(fecha)
    return _as_utc_ns(
        valuation_date.dt.normalize()
        + pd.Timedelta(days=1)
        - pd.Timedelta(seconds=1)
    )


def _asset_cursor_map_from_update_statistics(update_statistics: object) -> dict[str, pd.Timestamp]:
    if update_statistics is None:
        return {}

    if hasattr(update_statistics, "iter_index_progress_coordinates"):
        cursor_map: dict[str, pd.Timestamp] = {}
        for coordinate, value in update_statistics.iter_index_progress_coordinates(
            identity_dimensions=[ASSET_IDENTIFIER_DIMENSION]
        ):
            asset_identifier = coordinate.get(ASSET_IDENTIFIER_DIMENSION)
            timestamp = _as_utc_ns_timestamp(value)
            if asset_identifier is not None and timestamp is not None:
                cursor_map[str(asset_identifier)] = timestamp
        return cursor_map

    index_progress = getattr(update_statistics, "index_progress", None)
    if not isinstance(index_progress, dict):
        return {}

    cursor_map = {}
    for asset_identifier, value in index_progress.items():
        timestamp = _as_utc_ns_timestamp(value)
        if timestamp is not None:
            cursor_map[str(asset_identifier)] = timestamp
    return cursor_map


def _pricing_details_datetime(value: object):
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("pricing_details_date cannot be missing.")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def _persist_valmer_pricing_details_batch(
    *,
    assets_for_update: dict[str, MarketsAsset],
    instrument_pricing_detail_map: dict[str, dict],
    batch_size: int,
    logger,
) -> list[str]:
    date_groups: dict[Any, list[tuple[str, MarketsAsset, Any]]] = {}
    for uid, asset in assets_for_update.items():
        pricing_details = instrument_pricing_detail_map[uid]
        pricing_details_date = _pricing_details_datetime(
            pricing_details["pricing_details_date"]
        )
        date_groups.setdefault(pricing_details_date, []).append(
            (uid, asset, pricing_details["instrument"])
        )

    total_items = sum(len(group) for group in date_groups.values())
    logger.info(
        "Persisting Valmer pricing details in bulk: "
        f"{total_items} items, {len(date_groups)} date groups, "
        f"batch size {batch_size}."
    )

    persisted_uids: list[str] = []
    for pricing_details_date, group in date_groups.items():
        group_uids = [uid for uid, _, _ in group]
        items = [
            {
                "asset": asset,
                "instrument": instrument,
                "source": "valmer",
                "metadata_json": {"valmer_unique_identifier": uid},
            }
            for uid, asset, instrument in group
        ]
        result = add_many_pricing_details(
            items,
            pricing_details_date=pricing_details_date,
            batch_size=batch_size,
        )
        pricing_detail_rows = getattr(result, "pricing_details", None)
        returned_row_count = "unknown" if pricing_detail_rows is None else len(pricing_detail_rows)
        if pricing_detail_rows is not None and len(pricing_detail_rows) != len(items):
            raise RuntimeError(
                "msm_pricing.add_many_pricing_details returned an incomplete "
                "timestamped pricing-details result for Valmer batch dated "
                f"{pricing_details_date.isoformat()}: submitted {len(items)} items, "
                f"returned {len(pricing_detail_rows)} rows. "
                f"Submitted UIDs: {_summarize_uids(group_uids)}"
            )

        persisted_uids.extend(group_uids)
        logger.info(
            "Persisted Valmer pricing-details batch dated "
            f"{pricing_details_date.isoformat()}: {len(group_uids)} timestamped rows, "
            f"{getattr(result, 'updated_current_count', 0)} current rows updated, "
            f"{returned_row_count} rows returned by msm_pricing."
        )

    return persisted_uids


VALMER_DERIVED_COLUMN_SPECS = ()


VALMER_SOURCE_COLUMN_SPECS = (
    ValmerColumnSpec(
        source_name="fecha",
        column_name="valuation_date",
        dtype="datetime",
        transform="date_ymd",
        label="Valuation Date",
        description="Source FECHA value normalized to a UTC date.",
    ),
    ValmerColumnSpec(
        source_name="preciolimpio",
        column_name="clean_price",
        dtype="float",
        transform="float",
        label="Clean Price",
        description="Clean price from PRECIO LIMPIO.",
    ),
    ValmerColumnSpec(
        source_name="preciosucio",
        column_name="dirty_price",
        dtype="float",
        transform="float",
        label="Dirty Price",
        description="Dirty price from PRECIO SUCIO.",
    ),
    ValmerColumnSpec(
        source_name="interesesacumulados",
        column_name="accrued_interest",
        dtype="float",
        transform="float",
        label="Accrued Interest",
        description="Accrued interest from INTERESES ACUMULADOS.",
    ),
    ValmerColumnSpec(
        source_name="cuponactual",
        column_name="current_coupon",
        dtype="float",
        transform="float",
        label="Current Coupon",
        description="Current coupon from CUPON ACTUAL.",
    ),
    ValmerColumnSpec(
        source_name="sobretasa",
        column_name="spread",
        dtype="float",
        transform="float",
        label="Spread",
        description="Spread from SOBRETASA.",
    ),
    ValmerColumnSpec(
        source_name="montoencirculacion",
        column_name="amount_outstanding",
        dtype="float",
        transform="float",
        label="Amount Outstanding",
        description="Outstanding amount from MONTO EN CIRCULACION.",
    ),
    ValmerColumnSpec(
        source_name="diastransccpn",
        column_name="days_since_coupon",
        dtype="float",
        transform="float",
        label="Days Since Coupon",
        description="Days since coupon from DIAS TRANSC. CPN.",
    ),
    ValmerColumnSpec(
        source_name="cuponesxcobrar",
        column_name="coupons_remaining",
        dtype="float",
        transform="float",
        label="Coupons Remaining",
        description="Remaining coupon count from CUPONES X COBRAR.",
    ),
    ValmerColumnSpec(
        source_name="hechodemkt",
        column_name="market_event",
        dtype="string",
        transform="string",
        label="Market Event",
        description="Market event marker from HECHO DE MKT.",
    ),
    ValmerColumnSpec(
        source_name="fechauh",
        column_name="uh_date",
        dtype="datetime",
        transform="datetime",
        label="UH Date",
        description="Vendor FECHA U.H. date field preserved as provided by Valmer.",
    ),
    ValmerColumnSpec(
        source_name="precioteorico",
        column_name="theoretical_price",
        dtype="float",
        transform="float",
        label="Theoretical Price",
        description="Theoretical price from PRECIO TEORICO.",
    ),
    ValmerColumnSpec(
        source_name="postcompra",
        column_name="posted_bid",
        dtype="float",
        transform="float",
        label="Posted Bid",
        description="Posted bid from POST COMPRA.",
    ),
    ValmerColumnSpec(
        source_name="postventa",
        column_name="posted_ask",
        dtype="float",
        transform="float",
        label="Posted Ask",
        description="Posted ask from POST VENTA.",
    ),
    ValmerColumnSpec(
        source_name="yieldcompra",
        column_name="bid_yield",
        dtype="float",
        transform="float",
        label="Bid Yield",
        description="Bid yield from YIELD COMPRA.",
    ),
    ValmerColumnSpec(
        source_name="yieldventa",
        column_name="ask_yield",
        dtype="float",
        transform="float",
        label="Ask Yield",
        description="Ask yield from YIELD VENTA.",
    ),
    ValmerColumnSpec(
        source_name="spreadcompra",
        column_name="bid_spread",
        dtype="float",
        transform="float",
        label="Bid Spread",
        description="Bid spread from SPREAD COMPRA.",
    ),
    ValmerColumnSpec(
        source_name="spreadventa",
        column_name="ask_spread",
        dtype="float",
        transform="float",
        label="Ask Spread",
        description="Ask spread from SPREAD VENTA.",
    ),
    ValmerColumnSpec(
        source_name="mdys",
        column_name="moodys_rating",
        dtype="string",
        transform="string",
        label="Moody's Rating",
        description="Moody's rating from MDYS.",
    ),
    ValmerColumnSpec(
        source_name="sp",
        column_name="sp_rating",
        dtype="string",
        transform="string",
        label="S&P Rating",
        description="S&P rating from S&P.",
    ),
    ValmerColumnSpec(
        source_name="bursatilidad",
        column_name="marketability",
        dtype="string",
        transform="string",
        label="Marketability",
        description="Marketability label from BURSATILIDAD.",
    ),
    ValmerColumnSpec(
        source_name="liquidez",
        column_name="liquidity",
        dtype="float",
        transform="float",
        label="Liquidity",
        description="Liquidity value from LIQUIDEZ.",
    ),
    ValmerColumnSpec(
        source_name="cambiodiario",
        column_name="daily_change_pct",
        dtype="float",
        transform="percent",
        label="Daily Change Pct",
        description="Daily change from CAMBIO DIARIO, stored as a numeric percentage value.",
    ),
    ValmerColumnSpec(
        source_name="cambiosemanal",
        column_name="weekly_change_pct",
        dtype="float",
        transform="percent",
        label="Weekly Change Pct",
        description="Weekly change from CAMBIO SEMANAL, stored as a numeric percentage value.",
    ),
    ValmerColumnSpec(
        source_name="preciomax12m",
        column_name="max_price_12m",
        dtype="float",
        transform="float",
        label="Max Price 12M",
        description="Twelve-month high price from PRECIO MAX 12M.",
    ),
    ValmerColumnSpec(
        source_name="preciomin12m",
        column_name="min_price_12m",
        dtype="float",
        transform="float",
        label="Min Price 12M",
        description="Twelve-month low price from PRECIO MIN 12M.",
    ),
    ValmerColumnSpec(
        source_name="suspension",
        column_name="suspension_status",
        dtype="string",
        transform="string",
        label="Suspension Status",
        description="Suspension status from SUSPENSION.",
    ),
    ValmerColumnSpec(
        source_name="volatilidad",
        column_name="volatility",
        dtype="float",
        transform="float",
        label="Volatility",
        description="Volatility from VOLATILIDAD.",
    ),
    ValmerColumnSpec(
        source_name="volatilidad2",
        column_name="volatility_secondary",
        dtype="float",
        transform="float",
        label="Secondary Volatility",
        description="Secondary volatility from VOLATILIDAD 2.",
    ),
    ValmerColumnSpec(
        source_name="duracion",
        column_name="duration",
        dtype="float",
        transform="float",
        label="Duration",
        description="Duration from DURACION.",
    ),
    ValmerColumnSpec(
        source_name="duracionmonet",
        column_name="monetary_duration",
        dtype="float",
        transform="float",
        label="Monetary Duration",
        description="Monetary duration from DURACION MONET.",
    ),
    ValmerColumnSpec(
        source_name="convexidad",
        column_name="convexity",
        dtype="float",
        transform="float",
        label="Convexity",
        description="Convexity from CONVEXIDAD.",
    ),
    ValmerColumnSpec(
        source_name="var",
        column_name="value_at_risk",
        dtype="float",
        transform="float",
        label="Value At Risk",
        description="Value at risk from VAR.",
    ),
    ValmerColumnSpec(
        source_name="desviacionstand",
        column_name="standard_deviation",
        dtype="float",
        transform="float",
        label="Standard Deviation",
        description="Standard deviation from DESVIACION STAND.",
    ),
    ValmerColumnSpec(
        source_name="valornominalactualizado",
        column_name="adjusted_face_value",
        dtype="float",
        transform="float",
        label="Adjusted Face Value",
        description="Adjusted face value from VALOR NOMINAL ACTUALIZADO.",
    ),
    ValmerColumnSpec(
        source_name="calificacionfitch",
        column_name="fitch_rating",
        dtype="string",
        transform="string",
        label="Fitch Rating",
        description="Fitch rating from CALIFICACION FITCH.",
    ),
    ValmerColumnSpec(
        source_name="fechapreciomaximo",
        column_name="max_price_date",
        dtype="datetime",
        transform="datetime",
        label="Max Price Date",
        description="Date of the maximum price from FECHA PRECIO MAXIMO.",
    ),
    ValmerColumnSpec(
        source_name="fechapreciominimo",
        column_name="min_price_date",
        dtype="datetime",
        transform="datetime",
        label="Min Price Date",
        description="Date of the minimum price from FECHA PRECIO MINIMO.",
    ),
    ValmerColumnSpec(
        source_name="sensibilidad",
        column_name="sensitivity",
        dtype="float",
        transform="float",
        label="Sensitivity",
        description="Sensitivity from SENSIBILIDAD.",
    ),
    ValmerColumnSpec(
        source_name="duracionmacaulay",
        column_name="macaulay_duration",
        dtype="float",
        transform="float",
        label="Macaulay Duration",
        description="Macaulay duration from DURACION MACAULAY.",
    ),
    ValmerColumnSpec(
        source_name="tasaderendimiento",
        column_name="yield_rate",
        dtype="float",
        transform="float",
        label="Yield Rate",
        description="Yield rate from TASA DE RENDIMIENTO.",
    ),
    ValmerColumnSpec(
        source_name="hrratings",
        column_name="hr_rating",
        dtype="string",
        transform="string",
        label="HR Rating",
        description="HR rating from HR RATINGS.",
    ),
)


VALMER_TIMESERIES_SOURCE_COLUMN_SPECS = VALMER_SOURCE_COLUMN_SPECS
VALMER_VECTOR_COLUMN_SPECS = VALMER_DERIVED_COLUMN_SPECS + VALMER_TIMESERIES_SOURCE_COLUMN_SPECS

VALMER_REQUIRED_IDENTITY_SOURCE_COLUMNS = ("fecha", "tipovalor", "emisora", "serie")
VALMER_OPTIONAL_SOURCE_COLUMNS = tuple(
    dict.fromkeys(
        [
            *[
                spec.source_name
                for spec in VALMER_SOURCE_COLUMN_SPECS
                if spec.source_name is not None
            ],
            *sorted(VALMER_ASSET_DETAIL_SOURCE_COLUMNS),
        ]
    )
)
VALMER_REQUIRED_SOURCE_COLUMNS = tuple(
    dict.fromkeys(
        [
            "unique_identifier",
            *VALMER_REQUIRED_IDENTITY_SOURCE_COLUMNS,
        ]
    )
)

PERSISTED_VECTOR_TO_SOURCE_COLUMNS = {
    "security_type": "tipovalor",
    "issuer": "emisora",
    "series": "serie",
    "issue_currency": "monedaemision",
    "underlying": "subyacente",
    "issue_date": "fechaemision",
    "adjusted_face_value": "valornominalactualizado",
}


def _prepare_frame_for_target_bond_rules(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        persisted: source
        for persisted, source in PERSISTED_VECTOR_TO_SOURCE_COLUMNS.items()
        if persisted in df.columns and source not in df.columns
    }
    if not rename_map:
        return df.copy()
    return df.rename(columns=rename_map).copy()


class MetaTableValmerSourceConfig(BaseModel):
    source_name: str = Field(
        ...,
        description="Stable label used for diagnostics for this Valmer MetaTable source.",
    )
    metatable_identifier: str | None = Field(
        default=None,
        description="Logical MetaTable identifier to read from.",
    )
    metatable_uid: str | None = Field(
        default=None,
        description="MetaTable uid to read from. Use this when no identifier is available.",
    )
    direct_mssql_table: str | None = Field(
        default=None,
        description=(
            "Explicit schema-qualified SQL Server table used only when the configured "
            "Main Sequence MSSQL DataSource cannot register MetaTables. Credentials "
            "remain environment-managed and are never stored in this config."
        ),
    )
    column_map: dict[str, str] = Field(
        ...,
        description=(
            "Strict source-column to normalized Valmer field mapping. "
            "Example: {'Fecha': 'fecha', 'TV': 'tipovalor'}."
        ),
    )
    sql_dialect: Literal["mssql", "postgresql"] = Field(
        default="mssql",
        description="SQL quoting/limit dialect used by MetaTable.run_query for this source.",
    )
    max_rows: int | None = Field(
        default=None,
        description="Optional read cap for exploratory runs. Production ingestion should omit it.",
    )

    @model_validator(mode="after")
    def validate_source(self) -> "MetaTableValmerSourceConfig":
        source_references = [
            self.metatable_identifier,
            self.metatable_uid,
            self.direct_mssql_table,
        ]
        if sum(reference is not None for reference in source_references) != 1:
            raise ValueError(
                "Pass exactly one of metatable_identifier, metatable_uid, or "
                "direct_mssql_table."
            )
        if self.direct_mssql_table and self.sql_dialect != "mssql":
            raise ValueError("direct_mssql_table requires sql_dialect='mssql'.")
        missing_targets = sorted(set(VALMER_REQUIRED_IDENTITY_SOURCE_COLUMNS) - set(self.column_map.values()))
        if missing_targets:
            raise ValueError(
                f"MetaTableValmerSource {self.source_name!r} is missing required "
                f"normalized targets: {missing_targets}."
            )
        duplicate_targets = sorted(
            {
                target
                for target in self.column_map.values()
                if list(self.column_map.values()).count(target) > 1
            }
        )
        if duplicate_targets:
            raise ValueError(
                f"MetaTableValmerSource {self.source_name!r} maps multiple source "
                f"columns to the same target(s): {duplicate_targets}."
            )
        if self.max_rows is not None and self.max_rows <= 0:
            raise ValueError("max_rows must be positive when provided.")
        return self


class _ProgressLogger:
    def __init__(
        self,
        logger,
        label: str,
        total: int,
        *,
        milestones: tuple[int, ...] = (1, 5, 20, 40, 60, 80, 100),
    ):
        self.logger = logger
        self.label = label
        self.total = total
        self.thresholds = {
            max(1, (total * milestone + 99) // 100)
            for milestone in milestones
        }
        self.completed = 0
        self.logged: set[int] = set()
        if total:
            self.logger.info(f"{label}: starting {total} items.")

    def advance(self, count: int = 1) -> None:
        if not self.total:
            return
        self.completed += count
        for threshold in sorted(self.thresholds):
            if threshold in self.logged or self.completed < threshold:
                continue
            self.logged.add(threshold)
            percent = min(100, round((self.completed / self.total) * 100))
            self.logger.info(
                f"{self.label}: {percent}% complete ({self.completed}/{self.total})."
            )


class ImportValmerConfig(AssetIndexedDataNodeConfiguration):
    bucket_name: str = Field(
        ...,
        description="Valmer artifact bucket used by this updater.",
        examples=["Hitorical Valmer Vector Analytico"],
    )
    source_kind: Literal["artifact", "metatable"] = Field(
        default="artifact",
        description="Valmer source adapter to use for this updater run.",
        examples=["artifact", "metatable"],
    )
    source_metatables: list[MetaTableValmerSourceConfig] | None = Field(
        default=None,
        description=(
            "One or more MetaTable-backed Valmer source specifications. "
            "Required when source_kind='metatable'."
        ),
    )

    @model_validator(mode="after")
    def validate_source_config(self) -> "ImportValmerConfig":
        if self.source_kind == "metatable" and not self.source_metatables:
            raise ValueError("source_metatables is required when source_kind='metatable'.")
        if self.source_kind == "artifact" and self.source_metatables:
            raise ValueError("source_metatables is only valid when source_kind='metatable'.")
        return self


class ImportValmer(AssetIndexedDataNode):
    def __init__(self, config: ImportValmerConfig, **kwargs):
        """
        Initializes the ImportValmer DataNode.

        Args:
            config: DataNode configuration including the source artifact bucket.
        """
        self.bucket_name = config.bucket_name
        self.source_kind = config.source_kind
        self.source_metatables = list(config.source_metatables or [])
        self.artifact_data = None
        self.source_data = None
        self.asset_list = None
        self._publication_unique_identifiers: set[str] = set()
        super().__init__(config=config, **kwargs)

    @classmethod
    def _required_storage_table(cls):
        return ValmerVectorPricesStorage

    def maximum_forward_fill(self):
        return timedelta(days=1) - pd.Timedelta("5ms")

    def get_explanation(self):
        explanation = (
            "### Data From Valmer\n\n"
            "This node reads all files from the specified Valmer bucket, "
            "combines them, and processes them in a single operation. "
            "It normalizes all column headers by lowercasing them and removing special characters."
        )
        return explanation

    @staticmethod
    def _stage_debug_artifact_file(
        path: Path,
        *,
        logger=None,
        attempts: int = 3,
    ) -> tuple[tempfile.TemporaryDirectory, Path]:
        temp_dir = tempfile.TemporaryDirectory(prefix="valmer-vector-")
        staged_path = Path(temp_dir.name) / path.name
        last_error: BaseException | None = None
        chunk_size = resolve_valmer_vector_local_copy_chunk_size()
        chunk_size_kb = max(1, chunk_size // 1024)

        for attempt in range(1, attempts + 1):
            try:
                if logger is not None:
                    logger.info(
                        f"Staging local vector file {path.name} to temporary local storage "
                        f"(attempt {attempt}/{attempts}, chunk_size={chunk_size_kb} KiB) ..."
                    )
                started = time.monotonic()
                with path.open("rb") as source, staged_path.open("wb") as target:
                    shutil.copyfileobj(source, target, length=chunk_size)
                if logger is not None:
                    logger.info(
                        f"Staged local vector file {path.name} in "
                        f"{round(time.monotonic() - started, 1)}s."
                    )
                return temp_dir, staged_path
            except OSError as exc:
                last_error = exc
                staged_path.unlink(missing_ok=True)
                if attempt < attempts:
                    time.sleep(2)

        temp_dir.cleanup()
        raise RuntimeError(
            f"Failed to stage local vector file {path}. If this path is backed by "
            "OneDrive/iCloud/CloudStorage, make sure the file is downloaded locally "
            "and retry."
        ) from last_error

    @staticmethod
    def _read_debug_artifact_file(path: Path, logger=None) -> pd.DataFrame:
        engine = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
        size_mb = round(path.stat().st_size / 1_048_576, 1)
        if logger is not None:
            logger.info(
                f"Reading local vector file {path.name} ({size_mb} MB, engine={engine}) ..."
            )

        started = time.monotonic()
        temp_dir, staged_path = ImportValmer._stage_debug_artifact_file(
            path,
            logger=logger,
        )
        try:
            # Read once from the staged local copy. ``NA`` is a valid EMISORA
            # issuer code, so preserve it while keeping the rest of pandas'
            # default Excel NA tokens.
            df = pd.read_excel(
                staged_path,
                engine=engine,
                dtype={"TIPO VALOR": "string", "SERIE": "string"},
                keep_default_na=False,
                na_values=_VALMER_EXCEL_NA_VALUES,
            )
        finally:
            temp_dir.cleanup()

        if "EMISORA" in df.columns:
            non_emisora_columns = [column for column in df.columns if column != "EMISORA"]
            if non_emisora_columns:
                df.loc[:, non_emisora_columns] = df.loc[:, non_emisora_columns].replace(
                    "NA",
                    pd.NA,
                )
            df["EMISORA"] = (
                df["EMISORA"]
                .astype("string")
                .replace("", pd.NA)
            )
        else:
            raise KeyError(
                f"Local Valmer vector file {path} does not contain required column EMISORA."
            )
        if logger is not None:
            logger.info(
                f"Read local vector file {path.name}: {len(df)} rows "
                f"in {round(time.monotonic() - started, 1)}s."
            )
        return df

    @classmethod
    def _read_debug_artifact_files(
        cls,
        paths: list[Path],
        logger=None,
    ) -> list[pd.DataFrame]:
        return [cls._read_debug_artifact_file(path, logger) for path in paths]

    @classmethod
    def _read_debug_artifact_path(cls, debug_artifact_path: str, logger=None) -> list[pd.DataFrame]:
        base = Path(debug_artifact_path)
        if not base.exists():
            raise FileNotFoundError(f"DEBUG_ARTIFACT_PATH does not exist: {base}")
        if base.is_file():
            return [cls._read_debug_artifact_file(base, logger)]
        paths = sorted(base.rglob("*.xls*"))
        if logger is not None:
            logger.info(f"Found {len(paths)} local vector file(s) under {base}.")
        return cls._read_debug_artifact_files(paths, logger)

    @staticmethod
    def _vector_time_index_from_valuation_date(value: object) -> pd.Timestamp | None:
        valuation_date = _as_utc_ns_timestamp(value)
        if valuation_date is None:
            return None
        return _as_utc_ns_timestamp(
            valuation_date.normalize()
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )

    def _latest_vector_cursor_by_asset(self) -> dict[str, pd.Timestamp]:
        try:
            update_statistics = self.local_persist_manager.get_update_statistics_for_table()
        except AttributeError:
            return {}
        cursor_map = _asset_cursor_map_from_update_statistics(update_statistics)
        self.logger.info(
            "Resolved Valmer vector per-asset cursors from target storage: "
            f"{len(cursor_map)} assets with existing observations."
        )
        return cursor_map

    @staticmethod
    def _materialize_optional_source_columns(frame: pd.DataFrame) -> pd.DataFrame:
        materialized = frame.copy()
        for column in VALMER_OPTIONAL_SOURCE_COLUMNS:
            if column not in materialized.columns:
                materialized[column] = pd.NA
        return materialized

    @classmethod
    def _filter_source_rows_from_last_vector_observation(
        cls,
        frame: pd.DataFrame,
        cursor_by_asset: dict[str, pd.Timestamp],
        *,
        source_name: str,
        logger,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()

        missing = sorted(set(VALMER_REQUIRED_SOURCE_COLUMNS) - set(frame.columns))
        if missing:
            raise KeyError(
                f"Valmer source {source_name!r} is missing required columns for "
                f"per-asset vector filtering: {missing}."
            )

        working = frame.copy()
        time_index = _source_vector_time_index(working["fecha"])
        invalid_dates = time_index.isna()
        if invalid_dates.any():
            bad_count = int(invalid_dates.sum())
            raise ValueError(
                f"Valmer source {source_name!r} has {bad_count} rows with invalid fecha values."
            )

        asset_identifier = working["unique_identifier"].astype("string")
        latest = asset_identifier.map(cursor_by_asset)
        latest = pd.to_datetime(latest, errors="coerce", utc=True)
        mask = latest.isna() | (time_index > latest)
        filtered = working.loc[mask].copy()
        logger.info(
            f"Filtered Valmer source {source_name!r} from last vector observation: "
            f"{len(filtered)}/{len(working)} rows kept across "
            f"{asset_identifier.nunique(dropna=True)} source assets."
        )
        return filtered

    @staticmethod
    def _quote_metatable_identifier(identifier: str, *, dialect: str) -> str:
        if dialect == "mssql":
            return ".".join(f"[{part.replace(']', ']]')}]" for part in identifier.split("."))
        return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in identifier.split("."))

    @classmethod
    def _resolve_source_metatable(cls, source: MetaTableValmerSourceConfig) -> MetaTable:
        if source.metatable_uid:
            return MetaTable.get(uid=source.metatable_uid)
        assert source.metatable_identifier is not None
        return MetaTable.get(identifier=source.metatable_identifier)

    @staticmethod
    def _source_valuation_date_column(source: MetaTableValmerSourceConfig) -> str:
        return next(
            source_column
            for source_column, normalized_column in source.column_map.items()
            if normalized_column == "fecha"
        )

    @staticmethod
    def _minimum_source_valuation_date(
        cursor_by_asset: Mapping[str, pd.Timestamp],
    ) -> pd.Timestamp | None:
        if not cursor_by_asset:
            return None
        values = pd.to_datetime(
            list(cursor_by_asset.values()),
            errors="coerce",
            utc=True,
        )
        valid_values = values[~values.isna()]
        if valid_values.empty:
            return None
        return pd.Timestamp(valid_values.min())

    @classmethod
    def _build_source_select_sql(
        cls,
        source: MetaTableValmerSourceConfig,
        *,
        physical_table_name: str,
        minimum_valuation_date: pd.Timestamp | None,
    ) -> str:
        table_name = cls._quote_metatable_identifier(
            physical_table_name,
            dialect=source.sql_dialect,
        )
        columns = [
            cls._quote_metatable_identifier(column, dialect=source.sql_dialect)
            for column in source.column_map
        ]
        if source.sql_dialect == "mssql" and source.max_rows is not None:
            select_clause = f"SELECT TOP ({source.max_rows}) {', '.join(columns)}"
            limit_clause = ""
        else:
            select_clause = f"SELECT {', '.join(columns)}"
            limit_clause = f" LIMIT {source.max_rows}" if source.max_rows is not None else ""

        where_clause = ""
        if minimum_valuation_date is not None:
            timestamp = pd.Timestamp(minimum_valuation_date)
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize("UTC")
            else:
                timestamp = timestamp.tz_convert("UTC")
            if source.sql_dialect == "mssql":
                timestamp_literal = timestamp.tz_localize(None).isoformat(timespec="milliseconds")
            else:
                timestamp_literal = timestamp.isoformat(timespec="microseconds")
            date_column = cls._quote_metatable_identifier(
                cls._source_valuation_date_column(source),
                dialect=source.sql_dialect,
            )
            where_clause = f" WHERE {date_column} > '{timestamp_literal}'"

        return f"{select_clause} FROM {table_name}{where_clause}{limit_clause}"

    @staticmethod
    def _metatable_contract_columns(meta_table: MetaTable) -> set[str]:
        columns = {column.name for column in getattr(meta_table, "columns", [])}
        if columns:
            return columns
        contract = getattr(meta_table, "table_contract", None)
        if not isinstance(contract, dict):
            return set()
        return {
            column["name"]
            for column in contract.get("columns", [])
            if isinstance(column, dict) and "name" in column
        }

    @classmethod
    def _validate_metatable_source_contract(
        cls,
        source: MetaTableValmerSourceConfig,
        meta_table: MetaTable,
    ) -> None:
        contract_columns = cls._metatable_contract_columns(meta_table)
        if not contract_columns:
            return
        missing = sorted(set(source.column_map) - contract_columns)
        if missing:
            raise KeyError(
                f"MetaTableValmerSource {source.source_name!r} maps columns that "
                f"are not in MetaTable {meta_table.uid}: {missing}."
            )

    @staticmethod
    def _query_result_keys(result: object) -> list[str]:
        if not isinstance(result, Mapping):
            return []
        return sorted(str(key) for key in result.keys())

    @classmethod
    def _query_error_message(cls, result: object) -> str:
        if not isinstance(result, Mapping):
            return ""
        error = result.get("error")
        messages: list[str] = []
        if isinstance(error, Mapping):
            messages.extend(
                str(error.get(key, ""))
                for key in ("message", "detail", "kind")
                if error.get(key)
            )
        elif error is not None:
            messages.append(str(error))
        for key in ("message", "detail"):
            if result.get(key):
                messages.append(str(result[key]))
        return " ".join(messages)

    @classmethod
    def _is_backend_mssql_dsn_error(cls, result: object) -> bool:
        message = cls._query_error_message(result).lower()
        return "invalid dsn" in message and "mssql://" in message

    @staticmethod
    def _run_metatable_query(
        meta_table: MetaTable,
        sql: str,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        return meta_table.run_query(sql, timeout=timeout)

    @staticmethod
    def _mssql_connection_config_from_env() -> dict[str, Any]:
        raw_host = os.environ.get("VALMER_METATABLE_MSSQL_HOST") or os.environ.get(
            "EXTERNAL_URL"
        )
        database = os.environ.get("VALMER_METATABLE_MSSQL_DATABASE") or os.environ.get(
            "EXTERNAL_BD"
        )
        user = os.environ.get("VALMER_METATABLE_MSSQL_USER") or os.environ.get(
            "EXTERNAL_USER"
        )
        password = os.environ.get("VALMER_METATABLE_MSSQL_PASSWORD") or os.environ.get(
            "EXTERNAL_PWD"
        )
        if not all((raw_host, database, user, password)):
            raise RuntimeError(
                "Direct MSSQL MetaTable fallback requires "
                "VALMER_METATABLE_MSSQL_HOST, VALMER_METATABLE_MSSQL_DATABASE, "
                "VALMER_METATABLE_MSSQL_USER, and VALMER_METATABLE_MSSQL_PASSWORD "
                "or EXTERNAL_URL, EXTERNAL_BD, EXTERNAL_USER, and EXTERNAL_PWD."
            )

        parsed = urlparse(raw_host if "://" in raw_host else f"//{raw_host}")
        host = parsed.hostname or raw_host
        port = parsed.port or int(os.environ.get("VALMER_METATABLE_MSSQL_PORT", "1433"))
        return {
            "server": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }

    @classmethod
    def _run_direct_mssql_query(cls, sql: str) -> pd.DataFrame:
        try:
            import pymssql
        except ImportError as exc:
            raise RuntimeError(
                "Direct MSSQL MetaTable fallback requires pymssql to be installed."
            ) from exc

        config = cls._mssql_connection_config_from_env()
        with pymssql.connect(
            server=config["server"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            login_timeout=10,
            timeout=120,
            as_dict=True,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return pd.DataFrame.from_records(cursor.fetchall())

    @staticmethod
    def _query_column_names(columns: object) -> list[str] | None:
        if columns is None or isinstance(columns, str | bytes):
            return None
        if not isinstance(columns, Sequence):
            return None

        names: list[str] = []
        for column in columns:
            if isinstance(column, Mapping):
                name = (
                    column.get("name")
                    or column.get("column_name")
                    or column.get("field")
                    or column.get("key")
                    or column.get("label")
                )
            else:
                name = getattr(column, "name", None)
                if name is None:
                    name = column
            if name is None:
                return None
            names.append(str(name))
        return names

    @classmethod
    def _frame_from_metatable_query_payload(
        cls,
        payload: object,
        *,
        columns: list[str] | None = None,
    ) -> pd.DataFrame | None:
        if payload is None:
            return None

        if isinstance(payload, pd.DataFrame):
            return payload.copy()

        if isinstance(payload, Mapping):
            nested_columns = cls._query_column_names(
                payload.get("columns")
                or payload.get("column_names")
                or payload.get("fields")
            ) or columns
            for key in ("rows", "results", "data", "records"):
                if key in payload:
                    return cls._frame_from_metatable_query_payload(
                        payload.get(key),
                        columns=nested_columns,
                    )
            return None

        if isinstance(payload, Sequence) and not isinstance(payload, str | bytes):
            rows = list(payload)
            if not rows:
                return pd.DataFrame(columns=columns)
            if all(isinstance(row, Mapping) for row in rows):
                return pd.DataFrame(rows)
            if columns is not None:
                return pd.DataFrame(rows, columns=columns)
            return pd.DataFrame(rows)

        return None

    @classmethod
    def _frame_from_metatable_query_result(
        cls,
        result: object,
        *,
        source_name: str,
    ) -> pd.DataFrame:
        if isinstance(result, Mapping) and result.get("ok") is False:
            raise RuntimeError(
                f"MetaTableValmerSource {source_name!r} query failed: "
                f"{result.get('error') or result}."
            )

        columns = None
        if isinstance(result, Mapping):
            columns = cls._query_column_names(
                result.get("columns")
                or result.get("column_names")
                or result.get("fields")
            )
            for key in ("rows", "results", "data", "records"):
                if key not in result:
                    continue
                frame = cls._frame_from_metatable_query_payload(
                    result.get(key),
                    columns=columns,
                )
                if frame is not None:
                    return frame
        else:
            frame = cls._frame_from_metatable_query_payload(result, columns=None)
            if frame is not None:
                return frame

        raise RuntimeError(
            f"MetaTableValmerSource {source_name!r} did not return tabular rows. "
            f"Result keys: {cls._query_result_keys(result)}."
        )

    @staticmethod
    def _source_column_match_key(column: object) -> str:
        return normalize_column_name(str(column))

    @classmethod
    def _align_metatable_source_columns(
        cls,
        frame: pd.DataFrame,
        *,
        expected_columns: Sequence[str],
    ) -> pd.DataFrame:
        if frame.empty and len(frame.columns) == 0:
            return frame

        expected = list(expected_columns)
        actual_by_key: dict[str, object] = {}
        duplicates: set[str] = set()
        for actual_column in frame.columns:
            key = cls._source_column_match_key(actual_column)
            if key in actual_by_key and actual_by_key[key] != actual_column:
                duplicates.add(key)
                continue
            actual_by_key[key] = actual_column

        if duplicates:
            raise KeyError(
                "MetaTable query returned duplicate columns after Valmer "
                f"normalization: {sorted(duplicates)}."
            )

        rename_map: dict[object, str] = {}
        for expected_column in expected:
            if expected_column in frame.columns:
                continue
            actual_column = actual_by_key.get(cls._source_column_match_key(expected_column))
            if actual_column is not None:
                rename_map[actual_column] = expected_column

        if not rename_map:
            return frame
        return frame.rename(columns=rename_map)

    @classmethod
    def _read_metatable_source_frame(
        cls,
        source: MetaTableValmerSourceConfig,
        *,
        logger,
        minimum_valuation_date: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        if source.direct_mssql_table:
            meta_table = None
            physical_table_name = source.direct_mssql_table
        else:
            meta_table = cls._resolve_source_metatable(source)
            cls._validate_metatable_source_contract(source, meta_table)
            physical_table_name = meta_table.physical_table_name

        sql = cls._build_source_select_sql(
            source,
            physical_table_name=physical_table_name,
            minimum_valuation_date=minimum_valuation_date,
        )

        if meta_table is None:
            logger.warning(
                "Reading direct MSSQL Valmer source "
                f"{source.source_name!r} ({physical_table_name}). This compatibility "
                "path is not a governed MetaTable query."
            )
            frame = cls._run_direct_mssql_query(sql)
            result: object = {"source": "direct_mssql"}
        else:
            logger.info(
                "Reading MetaTable Valmer source "
                f"{source.source_name!r} ({meta_table.identifier or meta_table.uid})."
            )
            result = cls._run_metatable_query(meta_table, sql)
            if source.sql_dialect == "mssql" and cls._is_backend_mssql_dsn_error(result):
                logger.warning(
                    "Backend MetaTable query failed with an MSSQL DSN error for "
                    f"{source.source_name!r}; retrying through direct MSSQL fallback."
                )
                frame = cls._run_direct_mssql_query(sql)
            else:
                frame = cls._frame_from_metatable_query_result(
                    result,
                    source_name=source.source_name,
                )
        frame = cls._align_metatable_source_columns(
            frame,
            expected_columns=tuple(source.column_map),
        )
        missing = sorted(set(source.column_map) - set(frame.columns))
        if missing:
            raise KeyError(
                f"MetaTableValmerSource {source.source_name!r} returned rows missing "
                f"mapped source columns: {missing}. Actual returned columns: "
                f"{[str(column) for column in frame.columns]}. Result keys: "
                f"{cls._query_result_keys(result)}."
            )
        return frame

    @classmethod
    def _normalize_metatable_source_frame(
        cls,
        frame: pd.DataFrame,
        source: MetaTableValmerSourceConfig,
    ) -> pd.DataFrame:
        frame = cls._align_metatable_source_columns(
            frame,
            expected_columns=tuple(source.column_map),
        )
        missing = sorted(set(source.column_map) - set(frame.columns))
        if missing:
            raise KeyError(
                f"MetaTableValmerSource {source.source_name!r} returned rows missing "
                f"mapped source columns: {missing}. Actual returned columns: "
                f"{[str(column) for column in frame.columns]}."
            )
        normalized = frame[list(source.column_map)].rename(columns=source.column_map).copy()
        required_missing = sorted(set(VALMER_REQUIRED_IDENTITY_SOURCE_COLUMNS) - set(normalized.columns))
        if required_missing:
            raise KeyError(
                f"MetaTableValmerSource {source.source_name!r} is missing required "
                f"normalized columns: {required_missing}."
            )
        normalized = cls._materialize_optional_source_columns(normalized)
        normalized = add_valmer_unique_identifier(normalized)
        return normalized

    @staticmethod
    def _raise_on_duplicate_source_keys(frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        keys = pd.DataFrame(
            {
                "time_index": _source_vector_time_index(frame["fecha"]),
                "unique_identifier": frame["unique_identifier"].astype("string"),
            },
            index=frame.index,
        )
        duplicates = keys.duplicated(["time_index", "unique_identifier"], keep=False)
        if duplicates.any():
            duplicate_rows = keys.loc[duplicates]
            sample = duplicate_rows.head(10).to_dict(orient="records")
            raise ValueError(
                "Duplicate Valmer source rows for (time_index, asset_identifier) "
                f"after concatenating MetaTable sources. Sample: {sample}"
            )

    def _set_metatable_source_data(
        self,
        *,
        bypass_vector_cursor_filter: bool = False,
    ):
        if self.source_data is not None:
            return None

        cursor_by_asset = self._latest_vector_cursor_by_asset()
        minimum_valuation_date = (
            None
            if bypass_vector_cursor_filter
            else self._minimum_source_valuation_date(cursor_by_asset)
        )
        frames: list[pd.DataFrame] = []
        for source in self.source_metatables:
            raw_frame = self._read_metatable_source_frame(
                source,
                logger=self.logger,
                minimum_valuation_date=minimum_valuation_date,
            )
            normalized_frame = self._normalize_metatable_source_frame(raw_frame, source)
            if bypass_vector_cursor_filter:
                filtered_frame = normalized_frame
            else:
                filtered_frame = self._filter_source_rows_from_last_vector_observation(
                    normalized_frame,
                    cursor_by_asset,
                    source_name=source.source_name,
                    logger=self.logger,
                )
            if not filtered_frame.empty:
                frames.append(filtered_frame)

        if not frames:
            self.source_data = pd.DataFrame()
            self.logger.info("No MetaTable Valmer source rows are newer than vector storage.")
            return None

        combined = pd.concat(frames, ignore_index=True, sort=False)
        self._raise_on_duplicate_source_keys(combined)
        self.source_data = combined
        self.logger.info(
            "Combined MetaTable Valmer sources into "
            f"{len(self.source_data)} rows after per-asset vector filtering."
        )

    def _set_artifact_data(self):
        """
        Reads all artifacts from the bucket, normalizes columns, and concatenates them into a single DataFrame.
        Optionally filters for new artifacts based on the 'process_all_files' flag.
        """
        debug_artifact_files = os.environ.get("DEBUG_ARTIFACT_FILES", None)
        debug_artifact_path = os.environ.get("DEBUG_ARTIFACT_PATH", None)
        if debug_artifact_files:
            paths = [Path(p) for p in debug_artifact_files.split(os.pathsep) if p]
            self.logger.info(f"Reading local Valmer vector batch of {len(paths)} file(s) ...")
            sorted_artifacts = self._read_debug_artifact_files(paths, self.logger)
            source_label = f"local batch ({len(paths)} file(s))"
        elif debug_artifact_path:
            self.logger.info(
                f"Reading local Valmer vector from DEBUG_ARTIFACT_PATH '{debug_artifact_path}' ..."
            )
            sorted_artifacts = self._read_debug_artifact_path(debug_artifact_path, self.logger)
            source_label = f"local DEBUG_ARTIFACT_PATH '{debug_artifact_path}'"
        else:
            if self.artifact_data is not None:
                return None

            artifacts, _artifact_dates = self._get_artifacts(self.logger, self.bucket_name)
            sorted_artifacts = artifacts

            self.logger.info(f"Processing {len(sorted_artifacts)} artifacts...")
            if not sorted_artifacts:
                self.logger.info("No new artifacts to process. Task finished.")
                self.artifact_data = pd.DataFrame()
                return None
            source_label = f"Artifact bucket '{self.bucket_name}'"

        frames = self._concatenate_artifacts_content(
            sorted_artifacts,
            self.logger,
            source_label=source_label,
        )

        self.artifact_data = pd.concat(frames, ignore_index=True, sort=False)

        self.logger.info(
            f"Combined all artifacts into a single DataFrame with {len(self.artifact_data)} rows."
        )

    def dependencies(self) -> dict:
        return {}

    # ------- Helpers for bond and vector filter -------#
    @staticmethod
    def _get_target_bonds(df_latest: pd.DataFrame):
        working = _prepare_frame_for_target_bond_rules(df_latest)
        required_cols = {"fechaemision"}
        missing = sorted(required_cols - set(working.columns))
        if missing:
            raise KeyError(
                f"ImportValmer target-bond selection requires columns {required_cols}. Missing: {missing}"
            )

        target_mask = working.apply(
            lambda row: classify_valmer_asset_type(row.to_dict()) == ASSET_TYPE_BOND,
            axis=1,
        )

        all_target_bonds = df_latest.loc[target_mask].copy()
        all_target_bonds = all_target_bonds.loc[working.loc[target_mask, "fechaemision"].notna()]

        return all_target_bonds

    @staticmethod
    def _get_missing_asset_uids(
        unique_identifiers: List[str],
        existing_assets: dict[str, MarketsAsset],
    ) -> List[str]:
        return [u for u in unique_identifiers if u not in existing_assets]

    @staticmethod
    def _get_current_pricing_face_values_by_uid(
        existing_assets: dict[str, MarketsAsset],
        *,
        batch_size: int,
        logger,
    ) -> dict[str, object]:
        if not existing_assets:
            return {}

        asset_uid_to_valmer_uid = {str(asset.uid): uid for uid, asset in existing_assets.items()}
        asset_uids = [asset.uid for asset in existing_assets.values()]
        context = AssetCurrentPricingDetails._active_context()
        batches = [
            asset_uids[start : start + batch_size]
            for start in range(0, len(asset_uids), batch_size)
        ]
        logger.info(
            "Resolving current pricing face values for "
            f"{len(asset_uids)} target assets in {len(batches)} batches "
            f"of up to {batch_size}."
        )
        progress = _ProgressLogger(
            logger,
            "Resolving current pricing face values",
            len(asset_uids),
        )

        face_values: dict[str, object] = {}
        rows_found = 0
        for batch in batches:
            statement = (
                select(
                    AssetCurrentPricingDetails.__table__.asset_uid.label("asset_uid"),
                    AssetCurrentPricingDetails.__table__.instrument_dump.label(
                        "instrument_dump"
                    ),
                )
                .where(AssetCurrentPricingDetails.__table__.asset_uid.in_(batch))
                .limit(len(batch))
            )
            operation = compile_markets_statement(
                statement,
                context=context,
                operation="select",
                models=[AssetCurrentPricingDetails.__table__],
                access="read",
            )
            result = execute_markets_operation(operation, context=context)
            for row in operation_result_rows(result):
                asset_uid = row.get("asset_uid")
                valmer_uid = asset_uid_to_valmer_uid.get(str(asset_uid))
                if valmer_uid is not None:
                    face_values[valmer_uid] = _pricing_detail_face_value(
                        row.get("instrument_dump")
                    )
                    rows_found += 1
            progress.advance(len(batch))
        logger.info(
            "Resolved current pricing face values: "
            f"{rows_found} existing pricing-detail rows, "
            f"{len(asset_uids) - rows_found} missing rows."
        )
        return face_values

    @staticmethod
    def _get_pricing_refresh_uids(
        unique_identifiers: List[str],
        existing_assets: dict[str, MarketsAsset],
        current_pricing_face_values: dict[str, object],
        all_target_bonds: pd.DataFrame,
        *,
        force_update: bool = False,
        logger=None,
    ) -> List[str]:
        """
        Decide which target UIDs need a pricing-detail refresh.
        """
        if all_target_bonds.empty:
            return []

        target_rows = all_target_bonds.drop_duplicates("unique_identifier", keep="last").set_index(
            "unique_identifier"
        )
        target_uids = set(target_rows.index)

        pricing_updates: List[str] = []
        missing_assets = 0
        missing_pricing_details = 0
        missing_face_values = 0
        changed_face_values = 0
        forced_refreshes = 0

        for u in unique_identifiers:
            if u not in target_uids:
                continue
            asset = existing_assets.get(u)

            if asset is None:
                missing_assets += 1
                pricing_updates.append(u)
                continue

            if force_update:
                forced_refreshes += 1
                pricing_updates.append(u)
                continue

            if u not in current_pricing_face_values:
                missing_pricing_details += 1
                pricing_updates.append(u)
                continue

            old_face_value = current_pricing_face_values.get(u)
            if old_face_value is None:
                missing_face_values += 1
                pricing_updates.append(u)
                continue

            # Compare against latest nominal value in targets
            row = target_rows.loc[u]
            new_face_value = row.get("valornominalactualizado", row.get("adjusted_face_value"))
            if old_face_value is None or old_face_value != new_face_value:
                changed_face_values += 1
                pricing_updates.append(u)

        # Deduplicate while preserving order
        def _dedup(seq: List[str]) -> List[str]:
            return list(dict.fromkeys(seq))

        deduped = _dedup(pricing_updates)
        if logger is not None:
            logger.info(
                "Pricing refresh decision: "
                f"{len(target_uids)} target assets, "
                f"{missing_assets} missing assets, "
                f"{missing_pricing_details} missing pricing details, "
                f"{missing_face_values} missing face values, "
                f"{changed_face_values} changed face values, "
                f"{forced_refreshes} forced refreshes, "
                f"{len(deduped)} refreshes."
            )
        return deduped

    @staticmethod
    def _get_artifacts(logger, bucket_name):
        artifacts = Artifact.filter(
            bucket__name=bucket_name,
        )
        sorted_artifacts = sorted(artifacts, key=lambda artifact: artifact.name)

        logger.info(f"Found {len(sorted_artifacts)} artifacts in bucket '{bucket_name}'.")

        # --- Conditional processing based on process_all_files flag ---
        artifact_dates = []
        for artifact in sorted_artifacts:
            match = re.search(r"(\d{4}-\d{2}-\d{2})", artifact.name)
            if match:
                artifact_dates.append(pd.to_datetime(match.group(1), utc=True))
            else:
                continue

        return sorted_artifacts, artifact_dates

    @staticmethod
    def _concatenate_artifacts_content(sorted_artifacts, logger, *, source_label: str | None = None):
        frames = []
        total = len(sorted_artifacts)
        for index, artifact in enumerate(tqdm(sorted_artifacts), start=1):
            if isinstance(artifact, Artifact):
                artifact_name = artifact.name
                name_l = artifact.name.lower()
                content = artifact.content
                buf = content

                df = None
                started = time.monotonic()
                if name_l.endswith(".xls"):
                    import xlrd  # noqa: F401

                    logger.info(f"[{index}/{total}] Reading Excel artifact {artifact_name} ...")
                    df = pd.read_excel(buf, engine="xlrd")
                elif name_l.endswith(".csv"):
                    logger.info(f"[{index}/{total}] Reading CSV artifact {artifact_name} ...")
                    try:
                        df = pd.read_csv(buf, encoding="latin1", engine="pyarrow")
                    except Exception:
                        df = pd.read_csv(buf, encoding="latin1", low_memory=False)
                else:
                    logger.info(f"Skipping unsupported file type: {artifact.name}")
                    continue

                if df is None or df.empty:
                    logger.info(f"[{index}/{total}] {artifact_name}: empty, skipping.")
                    continue
                logger.info(
                    f"[{index}/{total}] Parsed {artifact_name}: {len(df)} rows "
                    f"in {round(time.monotonic() - started, 1)}s."
                )
            else:
                df = artifact
                artifact_name = source_label or "local dataframe"

            # Normalize all column names
            df.columns = [normalize_column_name(col) for col in df.columns]

            # Check for required columns for instrument identifier
            required_cols = {"tipovalor", "emisora", "serie"}
            if required_cols.issubset(df.columns):
                df = add_valmer_unique_identifier(df)
            else:
                logger.warning(
                    f"Skipping unique_identifier creation for {artifact_name} due to missing columns."
                )
                continue

            frames.append(df)

        if not frames:
            source = source_label or "provided artifacts"
            raise ValueError(f"No valid data frames could be created from {source}.")
        return frames

    @staticmethod
    def _pick_latest_artifact(artifacts, logger):
        """Pick the single latest artifact by YYYY-MM-DD in its name; fallback to last by name."""
        import re

        if not artifacts:
            return None

        def _parse_dt(a):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", a.name)
            return pd.to_datetime(m.group(1), utc=True) if m else pd.NaT

        dated = [(a, _parse_dt(a)) for a in artifacts]
        dated = [(a, d) for a, d in dated if pd.notna(d)]
        if dated:
            max_date = max(d for _, d in dated)
            candidates = [a for a, d in dated if d == max_date]
            selected = sorted(candidates, key=lambda x: x.name)[-1]
            logger.info(f"Selected latest artifact: {selected.name} ({max_date.date()})")
            return selected

        logger.warning("No parsable dates in artifact names; falling back to last by name.")
        return sorted(artifacts, key=lambda a: a.name)[-1]

    def _sync_asset_registry_and_pricing(
        self,
        unique_identifiers: List[str],
        df_latest: pd.DataFrame,
        all_target_bonds: pd.DataFrame,
        *,
        force_update: bool = False,
    ) -> list:
        """Sync Valmer asset rows, Valmer asset details, and current pricing details."""
        per_page_assets = resolve_valmer_meta_operation_batch_size()
        source_unique_identifiers = list(dict.fromkeys(unique_identifiers))
        target_uids = set(all_target_bonds["unique_identifier"].dropna().unique())
        registration_unique_identifiers = [
            uid for uid in source_unique_identifiers if uid in target_uids
        ]
        df_latest_idx = df_latest.drop_duplicates("unique_identifier", keep="last").set_index(
            "unique_identifier"
        )
        target_asset_types = {
            uid: asset_type
            for uid in registration_unique_identifiers
            if uid in df_latest_idx.index
            for asset_type in [classify_valmer_asset_type(df_latest_idx.loc[uid].to_dict())]
            if asset_type is not None
        }
        unclassified_registration_uids = [
            uid for uid in registration_unique_identifiers if uid not in target_asset_types
        ]
        self.logger.info(
            "Starting Valmer asset registry and pricing sync: "
            f"{len(source_unique_identifiers)} source assets, "
            f"{len(target_uids)} target pricing assets, "
            f"{len(registration_unique_identifiers)} registration-scope assets, "
            f"batch size {per_page_assets}."
        )

        existing_asset_refs = resolve_valmer_asset_refs(
            source_unique_identifiers,
            batch_size=per_page_assets,
            logger=self.logger,
        )
        existing_assets = {
            uid: asset_ref.as_asset()
            for uid, asset_ref in existing_asset_refs.items()
        }
        asset_type_conflicts = [
            uid
            for uid, asset_ref in existing_asset_refs.items()
            if uid in target_asset_types
            and asset_ref.asset_type != target_asset_types[uid]
        ]
        existing_registration_count = sum(
            1 for uid in registration_unique_identifiers if uid in existing_assets
        )
        self.logger.info(
            "Valmer asset registry decision: "
            f"{len(existing_assets)} existing, "
            f"{len(registration_unique_identifiers) - existing_registration_count} missing, "
                f"{len(asset_type_conflicts)} wrong asset_type."
        )
        if unclassified_registration_uids:
            raise RuntimeError(
                "Valmer target-bond rows are missing an explicit asset classification: "
                f"{_summarize_uids(unclassified_registration_uids)}. "
                "Refusing to write AssetTable rows from the pricing-target filter alone."
            )
        if asset_type_conflicts:
            repair_asset_types = {
                uid: target_asset_types[uid]
                for uid in asset_type_conflicts
            }
            self.logger.warning(
                "Repairing Valmer AssetTable asset_type conflicts using explicit "
                "Valmer asset classification: "
                f"{_summarize_uids(asset_type_conflicts)}."
            )
            repaired_assets = _upsert_asset_table_rows(
                repair_asset_types,
                batch_size=per_page_assets,
                logger=self.logger,
            )
            existing_assets.update(repaired_assets)
        current_pricing_face_values = self._get_current_pricing_face_values_by_uid(
            existing_assets,
            batch_size=per_page_assets,
            logger=self.logger,
        )
        current_pricing_uids = set(current_pricing_face_values)
        missing_assets = self._get_missing_asset_uids(
            registration_unique_identifiers,
            existing_assets,
        )
        pricing_updates = self._get_pricing_refresh_uids(
            registration_unique_identifiers,
            existing_assets,
            current_pricing_face_values,
            all_target_bonds,
            force_update=force_update,
            logger=self.logger,
        )

        assets_to_upsert = list(dict.fromkeys(missing_assets))
        if assets_to_upsert:
            self.logger.info(
                f"Upserting {len(assets_to_upsert)} Valmer target-bond assets "
                "using explicit Valmer asset classification."
            )
            upserted_assets = _upsert_asset_table_rows(
                {uid: target_asset_types[uid] for uid in assets_to_upsert},
                batch_size=per_page_assets,
                logger=self.logger,
            )
            existing_assets.update(upserted_assets)

        hydration_assets = {
            uid: existing_assets[uid]
            for uid in target_uids
            if uid in existing_assets
        }
        if hydration_assets:
            _publish_valmer_asset_snapshots(
                df_latest,
                hydration_assets,
                logger=self.logger,
            )

        detail_source = df_latest[
            df_latest["unique_identifier"].isin(hydration_assets)
        ].copy()
        if not detail_source.empty:
            detail_rows = upsert_valmer_asset_details(
                detail_source,
                hydration_assets,
                logger=self.logger,
            )
            self.logger.info(f"Upserted {len(detail_rows)} Valmer asset detail rows.")

        # --- decide pricing recipients ---
        pricing_uid_list = list(
            dict.fromkeys([*pricing_updates, *(u for u in missing_assets if u in target_uids)])
        )

        if pricing_uid_list:
            instrument_pricing_detail_map: dict[str, dict] = {}
            instrument_build_failures: dict[str, str] = {}
            missing_latest_rows: list[str] = []
            instrument_progress = _ProgressLogger(
                self.logger,
                "Building Valmer pricing instruments",
                len(pricing_uid_list),
            )
            for uid in pricing_uid_list:
                if uid not in df_latest_idx.index:
                    missing_latest_rows.append(uid)
                    self.logger.error(
                        f"Cannot build current pricing details for {uid}: latest vector row missing."
                    )
                    instrument_progress.advance()
                    continue
                row = df_latest_idx.loc[uid]

                try:
                    icalendar, business_day_convention, settlement_days, day_count = (
                        get_instrument_conventions(row)
                    )
                    ql_bond = build_qll_bond_from_row(
                        row=row,
                        calendar=icalendar,
                        dc=day_count,
                        bdc=business_day_convention,
                        settlement_days=settlement_days,
                    )
                except Exception as exc:
                    failure_detail = _pricing_adapter_failure_detail(row, exc)
                    instrument_build_failures[uid] = failure_detail
                    self.logger.error(
                        "Cannot build current pricing details for "
                        f"{uid}: pricing adapter failed: {failure_detail}"
                    )
                else:
                    instrument_pricing_detail_map[uid] = {
                        "instrument": ql_bond,
                        "pricing_details_date": row["fecha"],
                    }
                instrument_progress.advance()

            # target the correct asset objects (newly registered + existing)
            assets_for_update: dict[str, MarketsAsset] = {
                u: existing_assets[u]
                for u in pricing_uid_list
                if u in existing_assets and u in instrument_pricing_detail_map
            }
            missing_assets_for_pricing = [u for u in pricing_uid_list if u not in existing_assets]
            missing_instruments_for_pricing = [
                u for u in pricing_uid_list if u not in instrument_pricing_detail_map
            ]
            if missing_assets_for_pricing:
                self.logger.error(
                    "Cannot persist current pricing details because Asset rows are missing for "
                    f"{len(missing_assets_for_pricing)} target UIDs: "
                    f"{_summarize_uids(missing_assets_for_pricing)}"
                )
            if missing_instruments_for_pricing:
                self.logger.error(
                    "Cannot persist current pricing details because instrument payloads are missing for "
                    f"{len(missing_instruments_for_pricing)} target UIDs: "
                    f"{_summarize_uids(missing_instruments_for_pricing)}"
                )

            persisted_uids = _persist_valmer_pricing_details_batch(
                assets_for_update=assets_for_update,
                instrument_pricing_detail_map=instrument_pricing_detail_map,
                batch_size=resolve_valmer_pricing_details_batch_size(),
                logger=self.logger,
            )

            verified_uids: set[str] = set()
            if persisted_uids:
                persisted_assets = {uid: assets_for_update[uid] for uid in persisted_uids}
                verified_uids = set(
                    self._get_current_pricing_face_values_by_uid(
                        persisted_assets,
                        batch_size=per_page_assets,
                        logger=self.logger,
                    )
                )
                current_pricing_uids.update(verified_uids)
            missing_after_persist = [
                uid for uid in persisted_uids if uid not in verified_uids
            ]
            if missing_after_persist:
                self.logger.error(
                    "Pricing details were persisted but current rows were not visible on readback for "
                    f"{len(missing_after_persist)} target UIDs/assets: "
                    f"{_summarize_uid_asset_pairs(missing_after_persist, assets_for_update)}"
                )

            failed_uids = list(
                dict.fromkeys(
                    [
                        *missing_latest_rows,
                        *missing_assets_for_pricing,
                        *missing_instruments_for_pricing,
                        *missing_after_persist,
                    ]
                )
            )
            if failed_uids:
                failure_summary = _pricing_detail_failure_summary(
                    missing_latest_rows=missing_latest_rows,
                    instrument_build_failures=instrument_build_failures,
                    missing_assets_for_pricing=missing_assets_for_pricing,
                    missing_instruments_for_pricing=missing_instruments_for_pricing,
                    missing_after_persist=missing_after_persist,
                    assets_for_update=assets_for_update,
                )
                self.logger.error(
                    "Pricing detail insertion incomplete: "
                    f"{len(failed_uids)} failed of {len(pricing_uid_list)} target refreshes. "
                    f"{failure_summary}"
                )
                raise RuntimeError(
                    "Pricing detail insertion failed for "
                    f"{len(failed_uids)} target UIDs: {_summarize_uids(failed_uids)}. "
                    f"{failure_summary}"
                )

            self.logger.info(
                "Pricing detail insertion complete: "
                f"{len(persisted_uids)} persisted, {len(verified_uids)} verified."
            )

        publication_unique_identifiers = [
            uid
            for uid in source_unique_identifiers
            if uid in existing_assets and uid in current_pricing_uids
        ]
        dropped_without_pricing = [
            uid for uid in source_unique_identifiers if uid not in publication_unique_identifiers
        ]
        self._publication_unique_identifiers = set(publication_unique_identifiers)
        self.logger.info(
            "Valmer vector publication eligibility: "
            f"{len(publication_unique_identifiers)} assets with current pricing details, "
            f"{len(dropped_without_pricing)} dropped without current pricing details."
        )
        if dropped_without_pricing:
            self.logger.info(
                "Dropped Valmer vector rows without current pricing details: "
                f"{_summarize_uids(dropped_without_pricing)}"
            )

        return [existing_assets[uid] for uid in publication_unique_identifiers]

    def update_pricing_details_from_last_vector(
        self,
        force_update=False,
    ):
        artifacts, artifact_dates = self._get_artifacts(self.logger, self.bucket_name)
        if not artifacts:
            self.logger.info("No artifacts to process.")
            return []

        last_artifact = self._pick_latest_artifact(artifacts, self.logger)
        if last_artifact is None:
            self.logger.info("No latest artifact could be selected.")
            return []
        source_df_list = self._concatenate_artifacts_content([last_artifact], self.logger)
        if not source_df_list:
            self.logger.info("Latest artifact produced no usable rows.")
            return []
        source_df = source_df_list[0]
        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(source_df)
        self.logger.info(f"[last vector] Found {len(unique_identifiers)} unique assets to process.")
        return self._sync_asset_registry_and_pricing(
            unique_identifiers,
            df_latest,
            all_target_bonds,
            force_update=force_update,
        )

    def _prepare_latest_inputs(self, df: pd.DataFrame):
        """Common prep: normalize, latest rows per UID, target bonds, and universe of UIDs."""
        df = df[df["unique_identifier"].notna()].copy()
        df = self._materialize_optional_source_columns(df)
        df["fecha"] = _parse_valmer_valuation_dates(df["fecha"])

        idx = df.groupby("unique_identifier")["fecha"].idxmax()
        df_latest = df.loc[idx].reset_index(drop=True)

        try:
            all_target_bonds = self._get_target_bonds(df_latest)
        except KeyError as exc:
            all_target_bonds = pd.DataFrame(columns=["unique_identifier"])
            self.logger.info(
                "No Valmer pricing-detail hydration targets selected because "
                f"instrument-definition columns are missing: {exc}."
            )

        unique_identifiers = df["unique_identifier"].unique().tolist()
        return df_latest, all_target_bonds, unique_identifiers

    def prepare_source_data(
        self,
        *,
        bypass_vector_cursor_filter: bool | None = None,
    ) -> pd.DataFrame:
        """Load Valmer artifact rows for the current DataNode update."""
        resolved_bypass_vector_cursor_filter = resolve_valmer_vector_bypass_cursor_filter(
            bypass_vector_cursor_filter,
        )
        if self.source_kind == "metatable":
            self._set_metatable_source_data(
                bypass_vector_cursor_filter=resolved_bypass_vector_cursor_filter,
            )
        else:
            self._set_artifact_data()
            self.source_data = self.artifact_data
        if self.source_data is None:
            self.source_data = pd.DataFrame()
        if not self.source_data.empty:
            self.source_data = self._materialize_optional_source_columns(self.source_data)
            if self.source_kind != "metatable" and not resolved_bypass_vector_cursor_filter:
                self.source_data = self._filter_source_rows_from_last_vector_observation(
                    self.source_data,
                    self._latest_vector_cursor_by_asset(),
                    source_name="artifact",
                    logger=self.logger,
                )
            elif self.source_kind != "metatable":
                self.logger.info(
                    "Bypassing Valmer artifact source-row cursor filter for repair run."
                )
        return self.source_data

    def prepare_for_update(
        self,
        *,
        force_pricing_update: bool | None = None,
        bypass_vector_cursor_filter: bool | None = None,
    ) -> list:
        """
        Prepare the Valmer update explicitly before the DataNode run.

        This registers AssetTable rows, upserts static Valmer asset details,
        hydrates current pricing details for target bonds, and stores the
        resulting AssetTable scope for get_asset_list().
        """
        resolved_force_pricing_update = resolve_valmer_force_pricing_details_patch(
            force_pricing_update,
        )
        source_data = self.prepare_source_data(
            bypass_vector_cursor_filter=bypass_vector_cursor_filter,
        )
        if self.source_data.empty:
            self.asset_list = None
            return []

        df_latest, all_target_bonds, unique_identifiers = self._prepare_latest_inputs(source_data)
        self.logger.info(f"Found {len(unique_identifiers)} unique assets to process.")
        self.asset_list = self._sync_asset_registry_and_pricing(
            unique_identifiers,
            df_latest,
            all_target_bonds,
            force_update=resolved_force_pricing_update,
        )

        publication_uids = set(getattr(self, "_publication_unique_identifiers", set()))
        if not publication_uids and self.asset_list:
            publication_uids = {
                asset.unique_identifier
                for asset in self.asset_list
                if getattr(asset, "unique_identifier", None)
            }
        self.source_data = source_data[
            source_data["unique_identifier"].isin(publication_uids)
        ].copy()
        self.logger.info(
            "Scoped Valmer vector publication to priceable assets: "
            f"{len(self.source_data)} rows for {len(publication_uids)} assets."
        )
        if self.source_data.empty:
            self.asset_list = None
        return self.asset_list

    def get_asset_list(self) -> Union[None, list]:
        """Return the already-prepared asset scope for this DataNode run."""
        return super().get_asset_list()

    def update(self):
        source_data = self.source_data
        assert source_data is not None, "Source data is not available"

        if source_data.empty:
            return pd.DataFrame()

        source_data = self._materialize_optional_source_columns(source_data)
        missing_columns = sorted(set(VALMER_REQUIRED_SOURCE_COLUMNS) - set(source_data.columns))
        if missing_columns:
            raise KeyError(
                "ImportValmer requires Valmer source identity and date columns. "
                f"Missing normalized columns: {missing_columns}"
            )

        vector_df = pd.DataFrame(index=source_data.index)
        for spec in VALMER_TIMESERIES_SOURCE_COLUMN_SPECS:
            assert spec.source_name is not None
            vector_df[spec.column_name] = _coerce_valmer_series(
                source_data[spec.source_name], spec.transform
            )

        valuation_date = vector_df["valuation_date"]
        time_index = _as_utc_ns(valuation_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))

        vector_df["time_index"] = time_index
        vector_df[ASSET_IDENTIFIER_DIMENSION] = source_data["unique_identifier"].astype("string")

        ordered_columns = [spec.column_name for spec in VALMER_VECTOR_COLUMN_SPECS]
        vector_df = vector_df[["time_index", ASSET_IDENTIFIER_DIMENSION, *ordered_columns]]
        vector_df.set_index(["time_index", ASSET_IDENTIFIER_DIMENSION], inplace=True)
        vector_df = self.update_statistics.filter_df_by_latest_value(vector_df)

        return vector_df
