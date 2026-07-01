from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests

from valmer_connectors.instruments.curve_bootstrap import (
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    VALMER_CURVE_QUOTE_SIDE,
    VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION,
)

VALMER_TIIE_IRS_MXN_URL = VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION.metadata_json[
    "source_url"
]
VALMER_USD_SOFR_IRS_URL = VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION.metadata_json[
    "source_url"
]
VALMER_BENCHMARK_PAGE_URL = "https://www.valmer.com.mx/en/"
VALMER_BENCHMARK_DATE_URL = "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do"
VALMER_BROWSER_USER_AGENT = "Mozilla/5.0"
VALMER_TIIE_IRS_MXN_COLUMNS = ["instrument_identifier", "quote"]
VALMER_USD_SOFR_IRS_COLUMNS = ["instrument_identifier", "quote"]
VALMER_TIIE_DOMESTIC_OIS_SUFFIX = ".MXN.FTIIE.1D/28D.BANXICO"
VALMER_TIIE_CROSS_CURRENCY_SUFFIX = ".MXN.FTIIE.1D/USD.SOFR.1D.SOFR"
VALMER_USD_SOFR_OIS_SUFFIX = ".USD.SOFR.1D/1Y.SOFR"
VALMER_USD_FEDFUNDS_OIS_SUFFIX = ".USD.FEDFUNDS.1D/1Y.FEDFUNDS1"
VALMER_USD_FEDFUNDS_SOFR_BASIS_SUFFIX = ".FEDFUNDS.1D/SOFR.1D.SOFR"
VALMER_TIIE_IRS_SOURCE_FILE = "IRS_MXN_CURVE.csv"
VALMER_USD_SOFR_IRS_SOURCE_FILE = "IRS_USD_CURVE.csv"
VALMER_BENCHMARK_DATE_NAME = "Indices_Benchmarks"
VALMER_TIIE_IMPLIED_FRONT_DAYS = (1,)
VALMER_USD_SOFR_IMPLIED_FRONT_DAYS = (1,)
VALMER_TIIE_PAYMENT_FREQUENCY = "EveryFourthWeek"
VALMER_USD_SOFR_PAYMENT_FREQUENCY = "Annual"
VALMER_TENOR_PATTERN = re.compile(r"^(?P<value>[1-9]\d*)(?P<unit>[DWMY])$")
VALMER_USD_SOFR_FUTURE_PATTERN = re.compile(
    r"^Future\.USD\.CME\.CME (?P<contract_code>SR[13]) "
    r"(?P<contract_type>EOM|IMM)\.(?P<month>[A-Z]{3})\.(?P<year>\d{2})$"
)
VALMER_MONTH_TOKENS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


class ValmerTiieCurveError(ValueError):
    """Raised when Valmer IRS MXN rows cannot build the TIIE curve."""


class ValmerUsdSofrCurveError(ValueError):
    """Raised when Valmer IRS USD rows cannot build the SOFR curve."""


@dataclass(frozen=True)
class ValmerIrsMxnQuote:
    instrument_identifier: str
    tenor: str
    quote_decimal: float
    source_quote: float


@dataclass(frozen=True)
class ValmerTiieOisHelper:
    quote: ValmerIrsMxnQuote
    helper: Any


@dataclass(frozen=True)
class ValmerUsdSofrFutureQuote:
    instrument_identifier: str
    contract_code: str
    reference_month: str
    reference_year: int
    reference_frequency: str
    source_price: float
    implied_rate_decimal: float


@dataclass(frozen=True)
class ValmerUsdSofrOisQuote:
    instrument_identifier: str
    tenor: str
    quote_decimal: float
    source_quote: float


@dataclass(frozen=True)
class ValmerUsdSofrHelper:
    quote: ValmerUsdSofrFutureQuote | ValmerUsdSofrOisQuote
    helper: Any
    helper_type: str


def read_tiie_irs_mxn_csv(content: bytes) -> pd.DataFrame:
    """Read the Valmer IRS MXN benchmark CSV as raw source quotes."""

    frame = pd.read_csv(
        io.BytesIO(content),
        header=None,
        names=VALMER_TIIE_IRS_MXN_COLUMNS,
        sep=",",
        engine="c",
        encoding="latin1",
        dtype=str,
    )
    frame = frame.dropna(how="all").copy()
    for column in VALMER_TIIE_IRS_MXN_COLUMNS:
        frame[column] = frame[column].astype(str).str.strip()
    return frame


def read_usd_sofr_irs_csv(content: bytes) -> pd.DataFrame:
    """Read the Valmer IRS USD benchmark CSV as raw source quotes."""

    frame = pd.read_csv(
        io.BytesIO(content),
        header=None,
        names=VALMER_USD_SOFR_IRS_COLUMNS,
        sep=",",
        engine="c",
        encoding="latin1",
        dtype=str,
    )
    frame = frame.dropna(how="all").copy()
    for column in VALMER_USD_SOFR_IRS_COLUMNS:
        frame[column] = frame[column].astype(str).str.strip()
    return frame


def classify_tiie_irs_mxn_row(instrument_identifier: str) -> str:
    if instrument_identifier.startswith("FX.USD.MXN"):
        return "fx"
    if instrument_identifier.startswith("Swap.") and instrument_identifier.endswith(
        VALMER_TIIE_DOMESTIC_OIS_SUFFIX
    ):
        return "domestic_ois"
    if instrument_identifier.startswith("Swap.") and instrument_identifier.endswith(
        VALMER_TIIE_CROSS_CURRENCY_SUFFIX
    ):
        return "cross_currency"
    return "unsupported"


def classify_usd_sofr_irs_row(instrument_identifier: str) -> str:
    if VALMER_USD_SOFR_FUTURE_PATTERN.match(instrument_identifier):
        return "sofr_future"
    if instrument_identifier.startswith("Swap.") and instrument_identifier.endswith(
        VALMER_USD_SOFR_OIS_SUFFIX
    ):
        return "sofr_ois"
    if instrument_identifier.startswith("Swap.") and instrument_identifier.endswith(
        VALMER_USD_FEDFUNDS_OIS_SUFFIX
    ):
        return "fedfunds_ois"
    if (
        instrument_identifier.startswith("Swap.USD.")
        and instrument_identifier.endswith(VALMER_USD_FEDFUNDS_SOFR_BASIS_SUFFIX)
    ):
        return "fedfunds_sofr_basis"
    return "unsupported"


def parse_valmer_benchmark_date(
    content: bytes | str,
    *,
    error_class: type[ValueError] = ValmerTiieCurveError,
) -> pd.Timestamp:
    """Parse the Valmer homepage AJAX benchmark-date response."""

    text = content.decode("utf-8") if isinstance(content, bytes) else str(content)
    text = text.strip()
    if text.startswith("for(;;);"):
        text = text.removeprefix("for(;;);").strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise error_class("Unable to parse Valmer benchmark date response.") from exc

    records = payload.get("respuesta")
    if not isinstance(records, list):
        raise error_class("Valmer benchmark date response missing respuesta list.")

    for record in records:
        if not isinstance(record, dict):
            continue
        if record.get("nombre") == VALMER_BENCHMARK_DATE_NAME:
            return _parse_valuation_date(record.get("fecha"), error_class=error_class)
        if record.get("descripcion") == "Indices y Benchmarks":
            return _parse_valuation_date(record.get("fecha"), error_class=error_class)

    raise error_class("Valmer benchmark date response missing Indices_Benchmarks date.")


def fetch_valmer_benchmark_date_content() -> bytes:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": VALMER_BROWSER_USER_AGENT,
            "Referer": VALMER_BENCHMARK_PAGE_URL,
        }
    )
    page_response = session.get(VALMER_BENCHMARK_PAGE_URL, timeout=30)
    page_response.raise_for_status()
    date_response = session.post(
        VALMER_BENCHMARK_DATE_URL,
        data={"rand": "0"},
        timeout=30,
    )
    date_response.raise_for_status()
    return date_response.content


def build_tiie_irs_mxn_curve_frame_from_sources(
    *,
    curve_content: bytes,
    benchmark_date_content: bytes | str,
    curve_identifier: str,
) -> pd.DataFrame:
    return build_tiie_irs_mxn_curve_frame(
        curve_content,
        curve_identifier=curve_identifier,
        valuation_date=parse_valmer_benchmark_date(benchmark_date_content),
    )


def build_usd_sofr_curve_frame_from_sources(
    *,
    curve_content: bytes,
    benchmark_date_content: bytes | str,
    curve_identifier: str,
) -> pd.DataFrame:
    return build_usd_sofr_curve_frame(
        curve_content,
        curve_identifier=curve_identifier,
        valuation_date=parse_valmer_benchmark_date(
            benchmark_date_content,
            error_class=ValmerUsdSofrCurveError,
        ),
    )


def build_tiie_irs_mxn_curve_frame(
    content: bytes,
    *,
    curve_identifier: str,
    valuation_date: Any | None = None,
    overnight_rate: float | None = None,
) -> pd.DataFrame:
    if valuation_date is None:
        raise ValmerTiieCurveError(
            "IRS_MXN_CURVE.csv has no valuation-date column; pass valuation_date explicitly."
        )

    valuation_ts = _parse_valuation_date(valuation_date)
    source_frame = read_tiie_irs_mxn_csv(content)
    domestic_quotes = _select_domestic_tiie_ois_quotes(source_frame)

    ql = _quantlib()
    previous_evaluation_date = ql.Settings.instance().evaluationDate
    ql.Settings.instance().evaluationDate = _ql_date(valuation_ts)
    try:
        helpers = _build_tiie_ois_helpers(domestic_quotes)
        ql_helpers = _build_rate_helper_vector(helpers, overnight_rate=overnight_rate)
        curve = _bootstrap_tiie_discount_curve(valuation_ts, ql_helpers)
        curve_points = _export_tiie_zero_rate_points(curve, valuation_ts)
        key_nodes = _build_tiie_key_nodes(helpers)
    finally:
        ql.Settings.instance().evaluationDate = previous_evaluation_date

    return pd.DataFrame(
        [
            {
                "time_index": valuation_ts,
                "curve_identifier": curve_identifier,
                "curve": curve_points,
                "key_nodes": key_nodes,
            }
        ]
    ).set_index(["time_index", "curve_identifier"])


def build_tiie_discount_curve_from_key_nodes(
    key_nodes: list[dict[str, Any]],
    *,
    valuation_date: Any,
    overnight_rate: float | None = None,
):
    """Rebuild the Valmer TIIE OIS discount curve from stored source key nodes."""

    valuation_ts = _parse_valuation_date(valuation_date)
    ql = _quantlib()
    previous_evaluation_date = ql.Settings.instance().evaluationDate
    ql.Settings.instance().evaluationDate = _ql_date(valuation_ts)
    try:
        helpers = _build_tiie_ois_helpers_from_key_nodes(key_nodes)
        ql_helpers = _build_rate_helper_vector(helpers, overnight_rate=overnight_rate)
        curve = _bootstrap_tiie_discount_curve(valuation_ts, ql_helpers)
        curve.enableExtrapolation()
        return curve
    finally:
        ql.Settings.instance().evaluationDate = previous_evaluation_date


def build_usd_sofr_curve_frame(
    content: bytes,
    *,
    curve_identifier: str,
    valuation_date: Any | None = None,
) -> pd.DataFrame:
    if valuation_date is None:
        raise ValmerUsdSofrCurveError(
            "IRS_USD_CURVE.csv has no valuation-date column; pass valuation_date explicitly."
        )

    valuation_ts = _parse_valuation_date(
        valuation_date,
        error_class=ValmerUsdSofrCurveError,
        source_name="Valmer USD SOFR",
    )
    source_frame = read_usd_sofr_irs_csv(content)
    future_quotes, ois_quotes = _select_usd_sofr_quotes(source_frame)

    ql = _quantlib()
    previous_evaluation_date = ql.Settings.instance().evaluationDate
    ql.Settings.instance().evaluationDate = _ql_date(valuation_ts)
    try:
        helpers = _build_usd_sofr_helpers(
            future_quotes,
            ois_quotes,
            valuation_ts=valuation_ts,
        )
        ql_helpers = _build_usd_sofr_rate_helper_vector(helpers)
        curve = _bootstrap_usd_sofr_discount_curve(valuation_ts, ql_helpers)
        curve_points = _export_usd_sofr_zero_rate_points(curve, valuation_ts)
        key_nodes = _build_usd_sofr_key_nodes(helpers)
    finally:
        ql.Settings.instance().evaluationDate = previous_evaluation_date

    return pd.DataFrame(
        [
            {
                "time_index": valuation_ts,
                "curve_identifier": curve_identifier,
                "curve": curve_points,
                "key_nodes": key_nodes,
            }
        ]
    ).set_index(["time_index", "curve_identifier"])


def build_tiie_irs_mxn_valmer(
    *,
    update_statistics,
    curve_identifier: str,
    base_node_curve_points=None,
) -> pd.DataFrame:
    _ = base_node_curve_points
    valuation_date = parse_valmer_benchmark_date(fetch_valmer_benchmark_date_content())
    last_update = _last_curve_update_time(update_statistics, curve_identifier)
    if last_update is not None and valuation_date <= last_update:
        return _empty_curve_frame()

    curve_response = requests.get(VALMER_TIIE_IRS_MXN_URL, timeout=30)
    curve_response.raise_for_status()
    return build_tiie_irs_mxn_curve_frame(
        curve_response.content,
        curve_identifier=curve_identifier,
        valuation_date=valuation_date,
    )


def build_usd_sofr_valmer(
    *,
    update_statistics,
    curve_identifier: str,
    base_node_curve_points=None,
) -> pd.DataFrame:
    _ = base_node_curve_points
    valuation_date = parse_valmer_benchmark_date(
        fetch_valmer_benchmark_date_content(),
        error_class=ValmerUsdSofrCurveError,
    )
    last_update = _last_curve_update_time(
        update_statistics,
        curve_identifier,
        error_class=ValmerUsdSofrCurveError,
    )
    if last_update is not None and valuation_date <= last_update:
        return _empty_curve_frame()

    curve_response = requests.get(VALMER_USD_SOFR_IRS_URL, timeout=30)
    curve_response.raise_for_status()
    return build_usd_sofr_curve_frame(
        curve_response.content,
        curve_identifier=curve_identifier,
        valuation_date=valuation_date,
    )


def _last_curve_update_time(
    update_statistics,
    curve_identifier: str,
    *,
    error_class: type[ValueError] = ValmerTiieCurveError,
) -> pd.Timestamp | None:
    getter = getattr(update_statistics, "get_last_update_for_identity", None)
    if not callable(getter):
        return None
    last_update = getter(curve_identifier)
    if last_update is None:
        return None
    return _parse_valuation_date(last_update, error_class=error_class)


def _empty_curve_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "time_index",
            "curve_identifier",
            "curve",
            "key_nodes",
        ]
    ).set_index(["time_index", "curve_identifier"])


def _select_domestic_tiie_ois_quotes(source_frame: pd.DataFrame) -> list[ValmerIrsMxnQuote]:
    working = source_frame.copy()
    working["source_family"] = working["instrument_identifier"].map(
        classify_tiie_irs_mxn_row
    )
    selected = working.loc[working["source_family"].eq("domestic_ois")].copy()
    if selected.empty:
        raise ValmerTiieCurveError(
            "IRS_MXN_CURVE.csv contained no domestic FTIIE OIS rows."
        )

    quotes = []
    seen_tenors: set[str] = set()
    for row in selected.itertuples(index=False):
        identifier = str(row.instrument_identifier)
        tenor = _extract_swap_tenor(identifier)
        if tenor in seen_tenors:
            raise ValmerTiieCurveError(f"Duplicate domestic FTIIE OIS tenor {tenor}.")
        seen_tenors.add(tenor)
        source_quote = _parse_float(row.quote, field_name=f"{identifier} quote")
        quotes.append(
            ValmerIrsMxnQuote(
                instrument_identifier=identifier,
                tenor=tenor,
                source_quote=source_quote,
                quote_decimal=source_quote / 100,
            )
        )
    return quotes


def _select_usd_sofr_quotes(
    source_frame: pd.DataFrame,
) -> tuple[list[ValmerUsdSofrFutureQuote], list[ValmerUsdSofrOisQuote]]:
    working = source_frame.copy()
    working["source_family"] = working["instrument_identifier"].map(
        classify_usd_sofr_irs_row
    )
    future_rows = working.loc[working["source_family"].eq("sofr_future")].copy()
    ois_rows = working.loc[working["source_family"].eq("sofr_ois")].copy()
    if future_rows.empty:
        raise ValmerUsdSofrCurveError("IRS_USD_CURVE.csv contained no SOFR futures rows.")
    if ois_rows.empty:
        raise ValmerUsdSofrCurveError("IRS_USD_CURVE.csv contained no SOFR OIS swap rows.")

    future_quotes = []
    seen_futures: set[str] = set()
    for row in future_rows.itertuples(index=False):
        identifier = str(row.instrument_identifier)
        if identifier in seen_futures:
            raise ValmerUsdSofrCurveError(f"Duplicate SOFR future {identifier}.")
        seen_futures.add(identifier)
        future_quotes.append(_parse_usd_sofr_future_quote(identifier, row.quote))

    ois_quotes = []
    seen_tenors: set[str] = set()
    for row in ois_rows.itertuples(index=False):
        identifier = str(row.instrument_identifier)
        tenor = _extract_swap_tenor(
            identifier,
            error_class=ValmerUsdSofrCurveError,
        )
        if tenor in seen_tenors:
            raise ValmerUsdSofrCurveError(f"Duplicate USD SOFR OIS tenor {tenor}.")
        seen_tenors.add(tenor)
        source_quote = _parse_float(
            row.quote,
            field_name=f"{identifier} quote",
            error_class=ValmerUsdSofrCurveError,
        )
        ois_quotes.append(
            ValmerUsdSofrOisQuote(
                instrument_identifier=identifier,
                tenor=tenor,
                source_quote=source_quote,
                quote_decimal=source_quote / 100,
            )
        )
    return future_quotes, ois_quotes


def _parse_usd_sofr_future_quote(
    instrument_identifier: str,
    quote: Any,
) -> ValmerUsdSofrFutureQuote:
    match = VALMER_USD_SOFR_FUTURE_PATTERN.match(instrument_identifier)
    if match is None:
        raise ValmerUsdSofrCurveError(
            f"Invalid Valmer USD SOFR future identifier {instrument_identifier!r}."
        )
    contract_code = match.group("contract_code")
    reference_month = match.group("month")
    reference_year = 2000 + int(match.group("year"))
    source_price = _parse_float(
        quote,
        field_name=f"{instrument_identifier} price",
        error_class=ValmerUsdSofrCurveError,
    )
    frequency = "Monthly" if contract_code == "SR1" else "Quarterly"
    return ValmerUsdSofrFutureQuote(
        instrument_identifier=instrument_identifier,
        contract_code=contract_code,
        reference_month=reference_month,
        reference_year=reference_year,
        reference_frequency=frequency,
        source_price=source_price,
        implied_rate_decimal=(100 - source_price) / 100,
    )


def _extract_swap_tenor(
    instrument_identifier: str,
    *,
    error_class: type[ValueError] = ValmerTiieCurveError,
) -> str:
    parts = instrument_identifier.split(".")
    if len(parts) < 2 or parts[0] != "Swap":
        raise error_class(f"Invalid Valmer swap identifier {instrument_identifier!r}.")
    tenor = parts[1]
    _parse_tenor_components(tenor, error_class=error_class)
    return tenor


def _parse_tenor_components(
    tenor: str,
    *,
    error_class: type[ValueError] = ValmerTiieCurveError,
) -> tuple[int, str]:
    match = VALMER_TENOR_PATTERN.match(tenor)
    if match is None:
        raise error_class(f"Unsupported Valmer tenor {tenor!r}.")
    return int(match.group("value")), match.group("unit")


def _build_tiie_ois_helpers(quotes: list[ValmerIrsMxnQuote]) -> list[ValmerTiieOisHelper]:
    ql = _quantlib()
    index = _ftiiie_overnight_index()
    helpers = []
    for quote in quotes:
        period = _ql_period_from_tenor(quote.tenor)
        helper = ql.OISRateHelper(
            1,
            period,
            quote.quote_decimal,
            index,
            ql.YieldTermStructureHandle(),
            False,
            0,
            ql.ModifiedFollowing,
            ql.EveryFourthWeek,
            _mexico_calendar(),
            ql.Period(0, ql.Days),
            0.0,
            ql.Pillar.LastRelevantDate,
            ql.Date(),
            ql.RateAveraging.Compound,
            False,
            ql.EveryFourthWeek,
            _mexico_calendar(),
        )
        helpers.append(ValmerTiieOisHelper(quote=quote, helper=helper))
    return helpers


def _build_tiie_ois_helpers_from_key_nodes(
    key_nodes: list[dict[str, Any]],
) -> list[ValmerTiieOisHelper]:
    quotes: list[ValmerIrsMxnQuote] = []
    for node in key_nodes:
        helper_type = str(node.get("helper_type") or "").strip().lower()
        instrument_type = str(node.get("instrument_type") or "").strip().lower()
        if helper_type not in {"ois_rate_helper", "overnight_indexed_swap_helper"} and (
            instrument_type != "overnight_indexed_swap"
        ):
            continue
        tenor = str(node.get("tenor") or "").strip().upper()
        if not tenor:
            raise ValmerTiieCurveError(f"TIIE key node is missing tenor: {node!r}.")
        _parse_tenor_components(tenor)
        quote = _key_node_decimal_rate(node, field_name=f"{tenor} quote")
        source_quote = node.get("source_quote")
        if source_quote in (None, ""):
            source_quote = quote * 100.0
        quotes.append(
            ValmerIrsMxnQuote(
                instrument_identifier=str(node.get("asset_identifier") or f"Swap.{tenor}"),
                tenor=tenor,
                quote_decimal=quote,
                source_quote=float(source_quote),
            )
        )
    if not quotes:
        raise ValmerTiieCurveError("TIIE key nodes contained no OIS rate helpers.")
    quotes.sort(key=lambda item: _tenor_sort_key(item.tenor))
    return _build_tiie_ois_helpers(quotes)


def _build_rate_helper_vector(
    helpers: list[ValmerTiieOisHelper],
    *,
    overnight_rate: float | None,
):
    ql = _quantlib()
    ql_helpers = ql.RateHelperVector()
    if overnight_rate is not None:
        ql_helpers.push_back(_build_overnight_deposit_helper(overnight_rate))
    for item in helpers:
        ql_helpers.push_back(item.helper)
    return ql_helpers


def _build_usd_sofr_helpers(
    future_quotes: list[ValmerUsdSofrFutureQuote],
    ois_quotes: list[ValmerUsdSofrOisQuote],
    *,
    valuation_ts: pd.Timestamp,
) -> list[ValmerUsdSofrHelper]:
    ql = _quantlib()
    valuation_date = _ql_date(valuation_ts)
    helpers: list[ValmerUsdSofrHelper] = []
    for quote in future_quotes:
        helper = _build_sofr_future_helper(quote)
        if helper.earliestDate() < valuation_date:
            continue
        helpers.append(
            ValmerUsdSofrHelper(
                quote=quote,
                helper=helper,
                helper_type="sofr_future_rate_helper",
            )
        )
    for quote in ois_quotes:
        helpers.append(
            ValmerUsdSofrHelper(
                quote=quote,
                helper=_build_sofr_ois_helper(quote),
                helper_type="ois_rate_helper",
            )
        )
    if not any(item.helper_type == "sofr_future_rate_helper" for item in helpers):
        raise ValmerUsdSofrCurveError(
            "IRS_USD_CURVE.csv contained no usable SOFR futures for the valuation date."
        )
    if not any(item.helper_type == "ois_rate_helper" for item in helpers):
        raise ValmerUsdSofrCurveError("IRS_USD_CURVE.csv contained no SOFR OIS helpers.")
    return helpers


def _build_sofr_future_helper(quote: ValmerUsdSofrFutureQuote):
    ql = _quantlib()
    frequency = ql.Monthly if quote.reference_frequency == "Monthly" else ql.Quarterly
    return ql.SofrFutureRateHelper(
        quote.source_price,
        _ql_month_from_token(quote.reference_month),
        quote.reference_year,
        frequency,
        0.0,
        ql.Pillar.LastRelevantDate,
    )


def _build_sofr_ois_helper(quote: ValmerUsdSofrOisQuote):
    ql = _quantlib()
    sofr = ql.Sofr()
    return ql.OISRateHelper(
        2,
        _ql_period_from_tenor(quote.tenor),
        quote.quote_decimal,
        sofr,
        ql.YieldTermStructureHandle(),
        False,
        0,
        ql.ModifiedFollowing,
        ql.Annual,
        sofr.fixingCalendar(),
        ql.Period(0, ql.Days),
        0.0,
        ql.Pillar.LastRelevantDate,
        ql.Date(),
        ql.RateAveraging.Compound,
        False,
        ql.Annual,
        sofr.fixingCalendar(),
    )


def _build_usd_sofr_rate_helper_vector(helpers: list[ValmerUsdSofrHelper]):
    ql = _quantlib()
    ql_helpers = ql.RateHelperVector()
    for item in helpers:
        ql_helpers.push_back(item.helper)
    return ql_helpers


def _build_overnight_deposit_helper(overnight_rate: float):
    ql = _quantlib()
    return ql.DepositRateHelper(
        overnight_rate,
        ql.Period(1, ql.Days),
        0,
        _mexico_calendar(),
        ql.ModifiedFollowing,
        False,
        ql.Actual360(),
    )


def _bootstrap_tiie_discount_curve(valuation_date: pd.Timestamp, helpers):
    ql = _quantlib()
    previous_evaluation_date = ql.Settings.instance().evaluationDate
    ql.Settings.instance().evaluationDate = _ql_date(valuation_date)
    try:
        curve = ql.PiecewiseLogLinearDiscount(
            _ql_date(valuation_date),
            helpers,
            ql.Actual360(),
        )
        curve.recalculate()
        return curve
    finally:
        ql.Settings.instance().evaluationDate = previous_evaluation_date


def _bootstrap_usd_sofr_discount_curve(valuation_date: pd.Timestamp, helpers):
    ql = _quantlib()
    curve = ql.PiecewiseLogLinearDiscount(
        _ql_date(valuation_date),
        helpers,
        ql.Actual360(),
    )
    curve.enableExtrapolation()
    curve.recalculate()
    return curve


def _export_tiie_zero_rate_points(curve: Any, valuation_date: pd.Timestamp) -> dict[int, float]:
    ql = _quantlib()
    valuation_ql_date = _ql_date(valuation_date)
    dates_by_days = {
        days: valuation_ql_date + days for days in VALMER_TIIE_IMPLIED_FRONT_DAYS
    }
    for date in curve.dates():
        days_to_maturity = int(date - valuation_ql_date)
        if days_to_maturity > 0:
            dates_by_days[days_to_maturity] = date

    points: dict[int, float] = {}
    for days_to_maturity, pillar_date in sorted(dates_by_days.items()):
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
        raise ValmerTiieCurveError("Bootstrapped TIIE curve produced no pillar points.")
    return points


def _export_usd_sofr_zero_rate_points(
    curve: Any,
    valuation_date: pd.Timestamp,
) -> dict[int, float]:
    ql = _quantlib()
    valuation_ql_date = _ql_date(valuation_date)
    dates_by_days = {
        days: valuation_ql_date + days for days in VALMER_USD_SOFR_IMPLIED_FRONT_DAYS
    }
    for date in curve.dates():
        days_to_maturity = int(date - valuation_ql_date)
        if days_to_maturity > 0:
            dates_by_days[days_to_maturity] = date

    points: dict[int, float] = {}
    for days_to_maturity, pillar_date in sorted(dates_by_days.items()):
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
        raise ValmerUsdSofrCurveError("Bootstrapped USD SOFR curve produced no pillar points.")
    return points


def _build_tiie_key_nodes(helpers: list[ValmerTiieOisHelper]) -> list[dict[str, Any]]:
    return [_build_tiie_ois_key_node(item) for item in helpers]


def _build_usd_sofr_key_nodes(helpers: list[ValmerUsdSofrHelper]) -> list[dict[str, Any]]:
    nodes = []
    for item in helpers:
        if isinstance(item.quote, ValmerUsdSofrFutureQuote):
            nodes.append(_build_usd_sofr_future_key_node(item))
        else:
            nodes.append(_build_usd_sofr_ois_key_node(item))
    return nodes


def _build_tiie_ois_key_node(item: ValmerTiieOisHelper) -> dict[str, Any]:
    return {
        "maturity_date": _ql_date_to_iso(item.helper.maturityDate()),
        "asset_identifier": item.quote.instrument_identifier,
        "instrument_type": "overnight_indexed_swap",
        "helper_type": "ois_rate_helper",
        "quote": item.quote.quote_decimal,
        "quote_type": "par_swap_rate",
        "quote_unit": "decimal",
        "quote_side": VALMER_CURVE_QUOTE_SIDE,
        "quote_source": VALMER_TIIE_IRS_SOURCE_FILE,
        "source_quote": item.quote.source_quote,
        "source_quote_unit": "percent",
        "tenor": item.quote.tenor,
        "floating_index": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        "fixed_payment_frequency": VALMER_TIIE_PAYMENT_FREQUENCY,
        "day_counter": "Actual360",
        "earliest_date": _ql_date_to_iso(item.helper.earliestDate()),
        "pillar_date": _ql_date_to_iso(item.helper.pillarDate()),
    }


def _build_usd_sofr_future_key_node(item: ValmerUsdSofrHelper) -> dict[str, Any]:
    quote = item.quote
    if not isinstance(quote, ValmerUsdSofrFutureQuote):
        raise TypeError("USD SOFR future key node requires a future quote.")
    return {
        "maturity_date": _ql_date_to_iso(item.helper.maturityDate()),
        "asset_identifier": quote.instrument_identifier,
        "instrument_type": "sofr_future",
        "helper_type": item.helper_type,
        "quote": quote.source_price,
        "quote_type": "futures_price",
        "quote_unit": "price",
        "quote_side": VALMER_CURVE_QUOTE_SIDE,
        "quote_source": VALMER_USD_SOFR_IRS_SOURCE_FILE,
        "implied_rate": quote.implied_rate_decimal,
        "implied_rate_unit": "decimal",
        "contract_code": quote.contract_code,
        "reference_month": quote.reference_month,
        "reference_year": quote.reference_year,
        "reference_frequency": quote.reference_frequency,
        "earliest_date": _ql_date_to_iso(item.helper.earliestDate()),
        "pillar_date": _ql_date_to_iso(item.helper.pillarDate()),
    }


def _build_usd_sofr_ois_key_node(item: ValmerUsdSofrHelper) -> dict[str, Any]:
    quote = item.quote
    if not isinstance(quote, ValmerUsdSofrOisQuote):
        raise TypeError("USD SOFR OIS key node requires an OIS quote.")
    return {
        "maturity_date": _ql_date_to_iso(item.helper.maturityDate()),
        "asset_identifier": quote.instrument_identifier,
        "instrument_type": "overnight_indexed_swap",
        "helper_type": item.helper_type,
        "quote": quote.quote_decimal,
        "quote_type": "par_swap_rate",
        "quote_unit": "decimal",
        "quote_side": VALMER_CURVE_QUOTE_SIDE,
        "quote_source": VALMER_USD_SOFR_IRS_SOURCE_FILE,
        "source_quote": quote.source_quote,
        "source_quote_unit": "percent",
        "tenor": quote.tenor,
        "floating_index": USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        "fixed_payment_frequency": VALMER_USD_SOFR_PAYMENT_FREQUENCY,
        "day_counter": "Actual360",
        "earliest_date": _ql_date_to_iso(item.helper.earliestDate()),
        "pillar_date": _ql_date_to_iso(item.helper.pillarDate()),
    }


def _ql_period_from_tenor(tenor: str):
    ql = _quantlib()
    value, unit = _parse_tenor_components(tenor)
    units = {"D": ql.Days, "W": ql.Weeks, "M": ql.Months, "Y": ql.Years}
    return ql.Period(value, units[unit])


def _parse_valuation_date(
    value: Any,
    *,
    error_class: type[ValueError] = ValmerTiieCurveError,
    source_name: str = "Valmer TIIE",
) -> pd.Timestamp:
    if value is None or pd.isna(value):
        raise error_class(f"Missing {source_name} valuation date.")
    if isinstance(value, str) and "/" in value:
        parsed = pd.to_datetime(value, format="%d/%m/%Y")
    else:
        parsed = pd.to_datetime(value)
    timestamp = pd.Timestamp(parsed).normalize()
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _parse_float(
    value: Any,
    *,
    field_name: str,
    error_class: type[ValueError] = ValmerTiieCurveError,
) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise error_class(f"Invalid numeric {field_name}: {value!r}.") from exc
    if parsed <= 0:
        raise error_class(f"{field_name} must be positive.")
    return parsed


def _key_node_decimal_rate(node: dict[str, Any], *, field_name: str) -> float:
    value = node.get("quote")
    unit = str(node.get("quote_unit") or "").strip().lower()
    if value in (None, ""):
        value = node.get("yield")
        unit = str(node.get("yield_unit") or "").strip().lower()
    parsed = _parse_float(value, field_name=field_name)
    if unit in {"decimal", "decimals"}:
        return parsed
    if unit in {"percent", "percentage"}:
        return parsed / 100.0
    raise ValmerTiieCurveError(
        f"TIIE key node {field_name} has unsupported rate unit {unit!r}."
    )


def _tenor_sort_key(tenor: str) -> tuple[int, int]:
    value, unit = _parse_tenor_components(tenor)
    order = {"D": 1, "W": 7, "M": 30, "Y": 365}
    return value * order[unit], value


def _ql_month_from_token(token: str) -> int:
    ql = _quantlib()
    month_number = VALMER_MONTH_TOKENS.get(token)
    if month_number is None:
        raise ValmerUsdSofrCurveError(f"Unsupported SOFR futures month token {token!r}.")
    months = {
        1: ql.January,
        2: ql.February,
        3: ql.March,
        4: ql.April,
        5: ql.May,
        6: ql.June,
        7: ql.July,
        8: ql.August,
        9: ql.September,
        10: ql.October,
        11: ql.November,
        12: ql.December,
    }
    return months[month_number]


def _ftiiie_overnight_index():
    ql = _quantlib()
    return ql.OvernightIndex(
        "FTIIE",
        1,
        ql.MXNCurrency(),
        _mexico_calendar(),
        ql.Actual360(),
        ql.YieldTermStructureHandle(),
    )


def _mexico_calendar():
    ql = _quantlib()
    return ql.Mexico()


def _ql_date(value: pd.Timestamp):
    ql = _quantlib()
    timestamp = pd.Timestamp(value)
    return ql.Date(timestamp.day, timestamp.month, timestamp.year)


def _ql_date_to_iso(value) -> str:
    return f"{value.year():04d}-{int(value.month()):02d}-{value.dayOfMonth():02d}"


def _quantlib():
    import QuantLib as ql

    return ql
