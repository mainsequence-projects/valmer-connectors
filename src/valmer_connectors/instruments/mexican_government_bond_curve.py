from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import pandas as pd

from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
)

CETES_FACE_VALUE = 10.0
M_BONOS_FACE_VALUE = 100.0
M_BONOS_COUPON_PERIOD_DAYS = 182
MEXICAN_GOVERNMENT_CURRENCY_CODE = "MPS"
MEXICAN_GOVERNMENT_SECTOR = "GUBERNAMENTAL"
PRICE_TOLERANCE = 1e-4

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
    helper: Any


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


def build_cetes_zero_coupon_helper(row: pd.Series) -> BootstrapInstrument:
    ql = _quantlib()
    valuation_date = _parse_valuation_date(row["fecha"])
    maturity_date = _parse_date(row["fechavcto"], "fechavcto")
    _validate_maturity_after_valuation(valuation_date, maturity_date, row)

    price = _required_float(row, "preciosucio")
    if price <= 0:
        raise MexicanGovernmentBondCurveError(
            f"CETES {row.get('unique_identifier')} has non-positive price {price}."
        )

    face_value = _optional_float(row, "valornominal") or CETES_FACE_VALUE
    bond = ql.ZeroCouponBond(
        0,
        _mexico_calendar(),
        face_value,
        _ql_date(maturity_date),
        ql.Following,
        face_value,
        _ql_date(valuation_date),
    )
    helper = ql.BondHelper(
        ql.QuoteHandle(ql.SimpleQuote(price)),
        bond,
        ql.BondPrice.Clean,
    )
    return BootstrapInstrument(
        unique_identifier=str(row["unique_identifier"]),
        family="CETES",
        valuation_date=valuation_date,
        maturity_date=maturity_date,
        helper=helper,
    )


def build_m_bono_fixed_rate_helper(row: pd.Series) -> BootstrapInstrument:
    ql = _quantlib()
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

    calendar = _mexico_calendar()
    schedule = ql.Schedule(
        _ql_date(issue_date),
        _ql_date(maturity_date),
        ql.Period(M_BONOS_COUPON_PERIOD_DAYS, ql.Days),
        calendar,
        ql.Following,
        ql.Following,
        ql.DateGeneration.Backward,
        False,
    )
    helper = ql.FixedRateBondHelper(
        ql.QuoteHandle(ql.SimpleQuote(clean_price)),
        0,
        face_value,
        schedule,
        [coupon_rate],
        ql.Actual360(),
        ql.Following,
        face_value,
        _ql_date(issue_date),
        calendar,
        ql.Period(),
        calendar,
        ql.Unadjusted,
        False,
        ql.BondPrice.Clean,
    )
    return BootstrapInstrument(
        unique_identifier=str(row["unique_identifier"]),
        family="M_BONOS",
        valuation_date=valuation_date,
        maturity_date=maturity_date,
        helper=helper,
    )


def build_quantlib_bootstrap_instruments(df: pd.DataFrame) -> list[BootstrapInstrument]:
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
            instruments.append(build_cetes_zero_coupon_helper(row))
        elif (security_type, issuer) == ("M", "BONOS"):
            instruments.append(build_m_bono_fixed_rate_helper(row))
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

    instruments = build_quantlib_bootstrap_instruments(selected)
    families = {instrument.family for instrument in instruments}
    if "CETES" not in families or "M_BONOS" not in families:
        raise MexicanGovernmentBondCurveError(
            "Insufficient bootstrap instruments: expected at least one CETES helper "
            "and one M Bonos helper."
        )

    curve = _bootstrap_discount_curve(valuation_date, instruments)
    curve_points = _export_zero_rate_points(curve, valuation_date)
    time_index = derive_vector_time_index(valuation_date)
    return pd.DataFrame(
        [
            {
                "time_index": time_index,
                "curve_identifier": curve_identifier,
                "curve": curve_points,
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


def _bootstrap_discount_curve(
    valuation_date: pd.Timestamp,
    instruments: list[BootstrapInstrument],
):
    ql = _quantlib()
    ql_instruments = ql.RateHelperVector()
    for instrument in instruments:
        ql_instruments.push_back(instrument.helper)

    previous_evaluation_date = ql.Settings.instance().evaluationDate
    ql.Settings.instance().evaluationDate = _ql_date(valuation_date)
    try:
        curve = ql.PiecewiseLogLinearDiscount(
            _ql_date(valuation_date),
            ql_instruments,
            ql.Actual360(),
        )
        curve.recalculate()
        return curve
    finally:
        ql.Settings.instance().evaluationDate = previous_evaluation_date


def _export_zero_rate_points(curve: Any, valuation_date: pd.Timestamp) -> dict[int, float]:
    ql = _quantlib()
    points: dict[int, float] = {}
    valuation_ql_date = _ql_date(valuation_date)
    for pillar_date in curve.dates():
        days_to_maturity = int(pillar_date - valuation_ql_date)
        if days_to_maturity <= 0:
            continue
        zero_rate = curve.zeroRate(
            pillar_date,
            ql.Actual360(),
            ql.Compounded,
            ql.Annual,
            False,
        ).rate()
        points[days_to_maturity] = float(zero_rate)
    if not points:
        raise MexicanGovernmentBondCurveError("Bootstrapped curve produced no pillar points.")
    return points


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


def _normalise_coupon_rate(value: float) -> float:
    if value > 1:
        return value / 100
    return value


def _normalized_string_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.upper()


def _ql_date(timestamp: pd.Timestamp):
    ql = _quantlib()
    return ql.Date(timestamp.day, timestamp.month, timestamp.year)


def _mexico_calendar():
    ql = _quantlib()
    return ql.Mexico(ql.Mexico.BMV)


def _quantlib():
    try:
        import QuantLib as ql
    except ModuleNotFoundError as exc:
        raise RuntimeError("QuantLib is required for Mexican government curve bootstrap.") from exc
    return ql


__all__ = [
    "BootstrapInstrument",
    "CETES_FACE_VALUE",
    "M_BONOS_COUPON_PERIOD_DAYS",
    "M_BONOS_FACE_VALUE",
    "MexicanGovernmentBondCurveError",
    "build_cetes_zero_coupon_helper",
    "build_m_bono_fixed_rate_helper",
    "build_mxn_government_curve_frame",
    "build_mxn_government_curve_from_vector",
    "build_quantlib_bootstrap_instruments",
    "derive_vector_time_index",
    "select_mxn_government_bootstrap_instruments",
]
