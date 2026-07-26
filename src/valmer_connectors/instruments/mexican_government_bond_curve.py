from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import pandas as pd
from msm_pricing.pricing_engine.curves import (
    CurveObservationExportConfig,
    export_curve_observation_nodes,
    reconstruct_curve_handle_from_key_nodes,
)

from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_CURVE_QUOTE_SIDE,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
)

CETES_FACE_VALUE = 10.0
M_BONOS_FACE_VALUE = 100.0
M_BONOS_COUPON_PERIOD_DAYS = 182
MEXICAN_GOVERNMENT_CURRENCY_CODE = "MPS"
MEXICAN_GOVERNMENT_SECTOR = "GUBERNAMENTAL"
PRICE_TOLERANCE = 1e-4
MEXICAN_GOVERNMENT_CALENDAR_CODE = {"name": "Mexico"}
MEXICAN_GOVERNMENT_EXPORT_CONFIG = CurveObservationExportConfig(
    quote_convention="zero_rate",
    rate_unit="decimal",
    day_counter_code="Actual360",
    compounding="compounded",
    compounding_frequency="annual",
)

BOOTSTRAP_INSTRUMENT_KEYS = frozenset({("BI", "CETES"), ("M", "BONOS")})
REQUIRED_BOOTSTRAP_COLUMNS = frozenset(
    {
        "fecha",
        "tipovalor",
        "emisora",
        "serie",
        "fechavcto",
        "monedaemision",
        "preciosucio",
    }
)


class MexicanGovernmentBondCurveError(ValueError):
    """Raised when Valmer rows cannot build the MXN government bond curve."""


@dataclass(frozen=True)
class BootstrapInstrument:
    unique_identifier: str
    family: str
    valuation_date: pd.Timestamp
    maturity_date: pd.Timestamp
    quote: float
    key_node: dict[str, Any]


def derive_vector_time_index(value: Any) -> pd.Timestamp:
    valuation_date = _parse_valuation_date(value)
    return valuation_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


def select_mxn_government_bootstrap_instruments(df: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(REQUIRED_BOOTSTRAP_COLUMNS - set(df.columns))
    if missing:
        raise MexicanGovernmentBondCurveError(
            f"Mexican government curve selection missing required columns: {missing}"
        )

    working = df.copy()
    for column in ("tipovalor", "emisora", "monedaemision"):
        working[column] = _normalized_string_series(working[column])

    instrument_keys = list(zip(working["tipovalor"], working["emisora"]))
    mask = pd.Series(
        [key in BOOTSTRAP_INSTRUMENT_KEYS for key in instrument_keys],
        index=working.index,
    )
    mask &= working["monedaemision"].eq(MEXICAN_GOVERNMENT_CURRENCY_CODE)

    if "sector" in working.columns:
        working["sector"] = _normalized_string_series(working["sector"])
        sector_mask = working["sector"].eq(MEXICAN_GOVERNMENT_SECTOR)
        mask &= sector_mask

    selected = working.loc[mask].copy()
    if selected.empty:
        raise MexicanGovernmentBondCurveError(
            "No CETES or M Bonos rows selected for MXN government curve bootstrap."
        )

    if "unique_identifier" not in selected.columns:
        selected["unique_identifier"] = (
            selected["tipovalor"].astype(str)
            + "_"
            + selected["emisora"].astype(str)
            + "_"
            + selected["serie"].astype(str)
        )
    return selected


def build_cetes_zero_coupon_key_node(row: pd.Series) -> BootstrapInstrument:
    valuation_date = _parse_valuation_date(row["fecha"])
    maturity_date = _parse_date(row["fechavcto"], "fechavcto")
    _validate_maturity_after_valuation(valuation_date, maturity_date, row)

    price = _required_float(row, "preciosucio")
    if price <= 0:
        raise MexicanGovernmentBondCurveError(
            f"CETES {row.get('unique_identifier')} has non-positive price {price}."
        )

    face_value = _optional_float(row, "valornominal") or CETES_FACE_VALUE
    return BootstrapInstrument(
        unique_identifier=str(row["unique_identifier"]),
        family="CETES",
        valuation_date=valuation_date,
        maturity_date=maturity_date,
        quote=price,
        key_node=_clean_key_node(
            {
                "maturity_date": maturity_date.date().isoformat(),
                "source_reference": {
                    "type": "asset",
                    "identifier": str(row["unique_identifier"]),
                },
                "instrument_type": "zero_coupon_bond",
                "helper_type": "zero_coupon_bond_helper",
                "quote": price,
                "quote_type": "clean_price",
                "quote_unit": "price_per_face",
                "quote_side": VALMER_CURVE_QUOTE_SIDE,
                "quote_source": "preciosucio",
                "source_quote_type": "dirty_price",
                "yield": _optional_decimal_yield(row),
                "yield_type": "yield_to_maturity",
                "yield_unit": "decimal",
                "yield_source": "tasaderendimiento",
                "face_value": face_value,
                "day_counter": "Actual360",
                "issue_date": valuation_date.date().isoformat(),
                "settlement_days": 0,
                "calendar_code": MEXICAN_GOVERNMENT_CALENDAR_CODE,
                "payment_convention": "Following",
            }
        ),
    )


def build_m_bono_fixed_rate_key_node(row: pd.Series) -> BootstrapInstrument:
    valuation_date = _parse_valuation_date(row["fecha"])
    issue_date = _parse_date(row["fechaemision"], "fechaemision")
    maturity_date = _parse_date(row["fechavcto"], "fechavcto")
    _validate_maturity_after_valuation(valuation_date, maturity_date, row)

    coupon_period_days = _parse_coupon_period_days(row.get("freccpn"))
    if coupon_period_days != M_BONOS_COUPON_PERIOD_DAYS:
        raise MexicanGovernmentBondCurveError(
            f"M Bono {row.get('unique_identifier')} has unsupported coupon frequency "
            f"{row.get('freccpn')!r}; expected 182 days."
        )

    clean_price = _required_float(row, "preciolimpio")
    dirty_price = _required_float(row, "preciosucio")
    accrued_interest = _required_float(row, "interesesacumulados")
    face_value = _optional_float(row, "valornominal") or M_BONOS_FACE_VALUE
    coupon_rate = _normalise_coupon_rate(_required_float(row, "tasacupon"))

    _validate_clean_dirty_prices(row, clean_price, dirty_price, accrued_interest)
    _validate_m_bono_accrual(row, face_value, coupon_rate, accrued_interest)

    return BootstrapInstrument(
        unique_identifier=str(row["unique_identifier"]),
        family="M_BONOS",
        valuation_date=valuation_date,
        maturity_date=maturity_date,
        quote=clean_price,
        key_node=_clean_key_node(
            {
                "maturity_date": maturity_date.date().isoformat(),
                "source_reference": {
                    "type": "asset",
                    "identifier": str(row["unique_identifier"]),
                },
                "instrument_type": "fixed_rate_bond",
                "helper_type": "fixed_rate_bond_helper",
                "quote": clean_price,
                "quote_type": "clean_price",
                "quote_unit": "price_per_100",
                "quote_side": VALMER_CURVE_QUOTE_SIDE,
                "quote_source": "preciolimpio",
                "source_quote_type": "clean_price",
                "yield": _optional_decimal_yield(row),
                "yield_type": "yield_to_maturity",
                "yield_unit": "decimal",
                "yield_source": "tasaderendimiento",
                "dirty_price": dirty_price,
                "dirty_price_source": "preciosucio",
                "accrued_interest": accrued_interest,
                "issue_date": issue_date.date().isoformat(),
                "coupon_rate": coupon_rate,
                "coupon_period_days": M_BONOS_COUPON_PERIOD_DAYS,
                "face_value": face_value,
                "day_counter": "Actual360",
                "calendar_code": MEXICAN_GOVERNMENT_CALENDAR_CODE,
                "day_counter_code": "Actual360",
                "settlement_days": 0,
                "payment_convention": "Following",
                "business_day_convention": "Following",
            }
        ),
    )


def build_mxn_government_bootstrap_instruments(df: pd.DataFrame) -> list[BootstrapInstrument]:
    instruments: list[BootstrapInstrument] = []
    seen_identifiers: set[str] = set()
    for _, row in df.iterrows():
        unique_identifier = str(row["unique_identifier"])
        if unique_identifier in seen_identifiers:
            raise MexicanGovernmentBondCurveError(
                f"Duplicate bootstrap instrument {unique_identifier}."
            )
        seen_identifiers.add(unique_identifier)

        security_type = str(row["tipovalor"])
        issuer = str(row["emisora"])
        if (security_type, issuer) == ("BI", "CETES"):
            instruments.append(build_cetes_zero_coupon_key_node(row))
        elif (security_type, issuer) == ("M", "BONOS"):
            instruments.append(build_m_bono_fixed_rate_key_node(row))
        else:
            raise MexicanGovernmentBondCurveError(
                f"Unsupported bootstrap row {unique_identifier}: {(security_type, issuer)!r}."
            )
    return _deduplicate_pillars(instruments)


def build_mxn_government_curve_frame(
    source_df: pd.DataFrame,
    *,
    curve_identifier: str = VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
) -> pd.DataFrame:
    selected = select_mxn_government_bootstrap_instruments(source_df)
    valuation_dates = {
        _parse_valuation_date(value)
        for value in selected["fecha"].dropna().unique().tolist()
    }
    if len(valuation_dates) != 1:
        raise MexicanGovernmentBondCurveError(
            f"Expected one valuation date for curve build, got {len(valuation_dates)}."
        )
    valuation_date = next(iter(valuation_dates))

    instruments = build_mxn_government_bootstrap_instruments(selected)
    families = {instrument.family for instrument in instruments}
    if "CETES" not in families or "M_BONOS" not in families:
        raise MexicanGovernmentBondCurveError(
            "Insufficient bootstrap instruments: expected at least one CETES helper "
            "and one M Bonos helper."
        )

    key_nodes = _build_curve_key_nodes(instruments)
    curve = _reconstruct_discount_curve(valuation_date, key_nodes)
    curve_points = _export_zero_rate_points(
        curve,
        valuation_date,
        node_days=_node_days_from_key_nodes(key_nodes, valuation_date=valuation_date),
    )
    time_index = derive_vector_time_index(valuation_date)
    return pd.DataFrame(
        [
            {
                "time_index": time_index,
                "curve_identifier": curve_identifier,
                "curve": curve_points,
                "key_nodes": key_nodes,
            }
        ]
    ).set_index(["time_index", "curve_identifier"])


def build_mxn_government_curve_from_vector(
    *,
    update_statistics,
    curve_identifier: str,
    base_node_curve_points=None,
    source_df: pd.DataFrame,
) -> pd.DataFrame:
    _ = update_statistics, base_node_curve_points
    return build_mxn_government_curve_frame(
        source_df,
        curve_identifier=curve_identifier,
    )


def _reconstruct_discount_curve(valuation_date: pd.Timestamp, key_nodes: list[dict[str, Any]]):
    try:
        return reconstruct_curve_handle_from_key_nodes(
            key_nodes,
            valuation_date=valuation_date.date(),
            day_counter="Actual360",
            bootstrap_method="piecewise_log_linear_discount",
            extrapolation=True,
        )
    except Exception as exc:
        raise MexicanGovernmentBondCurveError(
            "Unable to reconstruct MXN government curve from bond-helper key nodes."
        ) from exc


def _export_zero_rate_points(
    curve: Any,
    valuation_date: pd.Timestamp,
    *,
    node_days: tuple[int, ...],
) -> dict[int, float]:
    try:
        nodes = export_curve_observation_nodes(
            curve,
            valuation_date=valuation_date.date(),
            node_days=node_days,
            include_pillar_dates=True,
            config=MEXICAN_GOVERNMENT_EXPORT_CONFIG,
        )
    except Exception as exc:
        raise MexicanGovernmentBondCurveError(
            "Bootstrapped MXN government curve produced no exportable points."
        ) from exc

    points = {
        int(node["days_to_maturity"]): float(node["zero"])
        for node in nodes
        if int(node["days_to_maturity"]) > 0 and node.get("zero") is not None
    }
    if not points:
        raise MexicanGovernmentBondCurveError("Bootstrapped curve produced no pillar points.")
    return points


def _node_days_from_key_nodes(
    key_nodes: list[dict[str, Any]],
    *,
    valuation_date: pd.Timestamp,
) -> tuple[int, ...]:
    node_days = {
        int((_parse_date(node["maturity_date"], "maturity_date") - valuation_date).days)
        for node in key_nodes
    }
    return tuple(sorted(days for days in node_days if days > 0))


def _build_curve_key_nodes(
    instruments: list[BootstrapInstrument],
) -> list[dict[str, Any]]:
    return [dict(instrument.key_node) for instrument in instruments]


def _deduplicate_pillars(
    instruments: list[BootstrapInstrument],
) -> list[BootstrapInstrument]:
    priority = {"CETES": 0, "M_BONOS": 1}
    selected_by_maturity: dict[pd.Timestamp, BootstrapInstrument] = {}
    for instrument in sorted(
        instruments,
        key=lambda item: (item.maturity_date, priority.get(item.family, 99)),
    ):
        selected_by_maturity.setdefault(instrument.maturity_date, instrument)

    deduped = sorted(selected_by_maturity.values(), key=lambda item: item.maturity_date)
    if len(deduped) < 2:
        raise MexicanGovernmentBondCurveError(
            "At least two unique curve pillars are required for bootstrap."
        )
    previous = deduped[0].maturity_date
    for instrument in deduped[1:]:
        if instrument.maturity_date <= previous:
            raise MexicanGovernmentBondCurveError("Curve pillars are not strictly increasing.")
        previous = instrument.maturity_date
    return deduped


def _validate_maturity_after_valuation(
    valuation_date: pd.Timestamp,
    maturity_date: pd.Timestamp,
    row: pd.Series,
) -> None:
    if maturity_date <= valuation_date:
        raise MexicanGovernmentBondCurveError(
            f"{row.get('unique_identifier')} matures on or before valuation date."
        )


def _validate_clean_dirty_prices(
    row: pd.Series,
    clean_price: float,
    dirty_price: float,
    accrued_interest: float,
) -> None:
    if abs((clean_price + accrued_interest) - dirty_price) > PRICE_TOLERANCE:
        raise MexicanGovernmentBondCurveError(
            f"M Bono {row.get('unique_identifier')} clean plus accrued does not match dirty price."
        )


def _validate_m_bono_accrual(
    row: pd.Series,
    face_value: float,
    coupon_rate: float,
    accrued_interest: float,
) -> None:
    if "diastransccpn" not in row or pd.isna(row.get("diastransccpn")):
        return
    days_since_coupon = _required_float(row, "diastransccpn")
    expected_accrued = face_value * coupon_rate * days_since_coupon / 360
    if abs(expected_accrued - accrued_interest) > PRICE_TOLERANCE:
        raise MexicanGovernmentBondCurveError(
            f"M Bono {row.get('unique_identifier')} accrued interest does not match "
            "Actual/360 coupon accrual."
        )


def _parse_coupon_period_days(value: Any) -> int:
    if value is None or pd.isna(value):
        raise MexicanGovernmentBondCurveError("M Bono coupon frequency is missing.")
    text = str(value).strip().lower()
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        raise MexicanGovernmentBondCurveError(f"Unsupported coupon frequency {value!r}.")
    return int(digits)


def _parse_valuation_date(value: Any) -> pd.Timestamp:
    timestamp = _parse_date(value, "fecha")
    return timestamp.normalize()


def _parse_date(value: Any, column: str) -> pd.Timestamp:
    if value is None or pd.isna(value):
        raise MexicanGovernmentBondCurveError(f"{column} is required.")
    if isinstance(value, pd.Timestamp):
        timestamp = value
    elif isinstance(value, dt.datetime | dt.date):
        timestamp = pd.Timestamp(value)
    else:
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        if text.isdigit() and len(text) == 8:
            timestamp = pd.to_datetime(text, format="%Y%m%d", utc=True, errors="coerce")
        else:
            timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise MexicanGovernmentBondCurveError(f"{column} is not parseable: {value!r}.")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.normalize()


def _required_float(row: pd.Series, column: str) -> float:
    value = _optional_float(row, column)
    if value is None:
        raise MexicanGovernmentBondCurveError(
            f"{row.get('unique_identifier')} missing required numeric field {column}."
        )
    return value


def _optional_float(row: pd.Series, column: str) -> float | None:
    if column not in row or pd.isna(row.get(column)):
        return None
    value = pd.to_numeric(row.get(column), errors="coerce")
    if pd.isna(value):
        return None
    return float(value)


def _optional_decimal_yield(row: pd.Series) -> float | None:
    for column in ("yield_rate", "tasaderendimiento"):
        value = _optional_float(row, column)
        if value is not None:
            return _normalise_percent_rate(value)
    return None


def _normalise_coupon_rate(value: float) -> float:
    return _normalise_percent_rate(value)


def _normalise_percent_rate(value: float) -> float:
    if abs(value) > 1:
        return value / 100
    return value


def _clean_key_node(node: dict[str, Any]) -> dict[str, Any]:
    has_yield = node.get("yield") is not None
    normalized = {}
    for key, value in node.items():
        if value is None:
            continue
        if key in {"yield_type", "yield_unit", "yield_source"} and not has_yield:
            continue
        normalized[key] = value
    return normalized


def _normalized_string_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.upper()


__all__ = [
    "BootstrapInstrument",
    "CETES_FACE_VALUE",
    "M_BONOS_COUPON_PERIOD_DAYS",
    "M_BONOS_FACE_VALUE",
    "MexicanGovernmentBondCurveError",
    "build_cetes_zero_coupon_key_node",
    "build_m_bono_fixed_rate_key_node",
    "build_mxn_government_curve_frame",
    "build_mxn_government_curve_from_vector",
    "build_mxn_government_bootstrap_instruments",
    "derive_vector_time_index",
    "select_mxn_government_bootstrap_instruments",
]
