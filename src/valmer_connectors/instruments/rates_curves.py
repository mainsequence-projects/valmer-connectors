from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests
from msm_pricing.pricing_engine.curves import (
    CurveObservationExportConfig,
    build_rate_helpers,
    export_curve_observation_nodes,
    helper_specs_from_key_nodes,
    reconstruct_curve_term_structure_from_key_nodes,
)

from valmer_connectors.instruments.curve_bootstrap import (
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    VALMER_CURVE_QUOTE_SIDE,
    VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION,
)
from valmer_connectors.instruments.curve_reconstruction import (
    resolve_valmer_overnight_index,
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
VALMER_TIIE_CALENDAR_CODE = {"name": "Mexico"}
VALMER_USD_SOFR_CALENDAR_CODE = {"name": "UnitedStates", "market": 6}
VALMER_CURVE_EXPORT_CONFIG = CurveObservationExportConfig(
    quote_convention="zero_rate",
    rate_unit="decimal",
    day_counter_code="Actual360",
    compounding="compounded",
    compounding_frequency="annual",
)
VALMER_TENOR_PATTERN = re.compile(r"^(?P<value>[1-9]\d*)(?P<unit>[DWMY])$")
VALMER_USD_SOFR_FUTURE_PATTERN = re.compile(
    r"^Future\.USD\.CME\.CME (?P<contract_code>SR[13]) "
    r"(?P<contract_type>EOM|IMM)\.(?P<month>[A-Z]{3})\.(?P<year>\d{2})$"
)


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

    key_nodes = _build_tiie_key_nodes(domestic_quotes)
    _enrich_key_nodes_with_helper_dates(
        key_nodes,
        valuation_ts=valuation_ts,
        error_class=ValmerTiieCurveError,
    )
    runtime_key_nodes = list(key_nodes)
    if overnight_rate is not None:
        runtime_key_nodes.insert(0, _build_tiie_overnight_deposit_key_node(overnight_rate))
    curve = _reconstruct_valmer_curve_term_structure(
        runtime_key_nodes,
        valuation_ts=valuation_ts,
        error_class=ValmerTiieCurveError,
    )
    curve_points = _export_zero_rate_points(
        curve,
        valuation_ts=valuation_ts,
        node_days=VALMER_TIIE_IMPLIED_FRONT_DAYS,
        error_class=ValmerTiieCurveError,
    )

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

    candidate_key_nodes = _build_usd_sofr_key_nodes(future_quotes, ois_quotes)
    _enrich_key_nodes_with_helper_dates(
        candidate_key_nodes,
        valuation_ts=valuation_ts,
        error_class=ValmerUsdSofrCurveError,
    )
    key_nodes = _filter_usable_usd_sofr_key_nodes(
        candidate_key_nodes,
        valuation_ts=valuation_ts,
    )
    curve = _reconstruct_valmer_curve_term_structure(
        key_nodes,
        valuation_ts=valuation_ts,
        error_class=ValmerUsdSofrCurveError,
    )
    curve_points = _export_zero_rate_points(
        curve,
        valuation_ts=valuation_ts,
        node_days=VALMER_USD_SOFR_IMPLIED_FRONT_DAYS,
        error_class=ValmerUsdSofrCurveError,
    )

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


def _build_tiie_key_nodes(quotes: list[ValmerIrsMxnQuote]) -> list[dict[str, Any]]:
    return [_build_tiie_ois_key_node(quote) for quote in quotes]


def _build_usd_sofr_key_nodes(
    future_quotes: list[ValmerUsdSofrFutureQuote],
    ois_quotes: list[ValmerUsdSofrOisQuote],
) -> list[dict[str, Any]]:
    nodes = [_build_usd_sofr_future_key_node(quote) for quote in future_quotes]
    nodes.extend(_build_usd_sofr_ois_key_node(quote) for quote in ois_quotes)
    return nodes


def _build_tiie_ois_key_node(quote: ValmerIrsMxnQuote) -> dict[str, Any]:
    return {
        "asset_identifier": quote.instrument_identifier,
        "instrument_type": "overnight_indexed_swap",
        "helper_type": "ois_rate_helper",
        "quote": quote.quote_decimal,
        "quote_type": "par_swap_rate",
        "quote_unit": "decimal",
        "quote_side": VALMER_CURVE_QUOTE_SIDE,
        "quote_source": VALMER_TIIE_IRS_SOURCE_FILE,
        "source_quote": quote.source_quote,
        "source_quote_unit": "percent",
        "tenor": quote.tenor,
        "settlement_days": 1,
        "floating_index": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        "telescopic_value_dates": False,
        "payment_lag": 0,
        "payment_convention": "ModifiedFollowing",
        "payment_frequency": VALMER_TIIE_PAYMENT_FREQUENCY,
        "payment_calendar_code": dict(VALMER_TIIE_CALENDAR_CODE),
        "forward_start": "0D",
        "overnight_spread": 0.0,
        "pillar": "LastRelevantDate",
        "averaging_method": "Compound",
        "end_of_month": False,
        "fixed_payment_frequency": VALMER_TIIE_PAYMENT_FREQUENCY,
        "fixed_calendar_code": dict(VALMER_TIIE_CALENDAR_CODE),
        "day_counter": "Actual360",
        "day_counter_code": "Actual360",
        "date_generation_convention": "ModifiedFollowing",
    }


def _build_tiie_overnight_deposit_key_node(overnight_rate: float) -> dict[str, Any]:
    return {
        "asset_identifier": "TIIE_OVERNIGHT_DEPOSIT_1D",
        "instrument_type": "overnight_deposit",
        "helper_type": "overnight_deposit_helper",
        "quote": overnight_rate,
        "quote_type": "deposit_rate",
        "quote_unit": "decimal",
        "quote_side": VALMER_CURVE_QUOTE_SIDE,
        "quote_source": VALMER_TIIE_IRS_SOURCE_FILE,
        "tenor": "1D",
        "fixing_days": 0,
        "calendar_code": dict(VALMER_TIIE_CALENDAR_CODE),
        "business_day_convention": "ModifiedFollowing",
        "end_of_month": False,
        "day_counter_code": "Actual360",
    }


def _build_usd_sofr_future_key_node(quote: ValmerUsdSofrFutureQuote) -> dict[str, Any]:
    return {
        "asset_identifier": quote.instrument_identifier,
        "instrument_type": "sofr_future",
        "helper_type": "sofr_future_rate_helper",
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
        "future_family": "sofr",
        "convexity_adjustment": 0.0,
        "pillar": "LastRelevantDate",
        "floating_index": USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    }


def _build_usd_sofr_ois_key_node(quote: ValmerUsdSofrOisQuote) -> dict[str, Any]:
    return {
        "asset_identifier": quote.instrument_identifier,
        "instrument_type": "overnight_indexed_swap",
        "helper_type": "ois_rate_helper",
        "quote": quote.quote_decimal,
        "quote_type": "par_swap_rate",
        "quote_unit": "decimal",
        "quote_side": VALMER_CURVE_QUOTE_SIDE,
        "quote_source": VALMER_USD_SOFR_IRS_SOURCE_FILE,
        "source_quote": quote.source_quote,
        "source_quote_unit": "percent",
        "tenor": quote.tenor,
        "settlement_days": 2,
        "floating_index": USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        "telescopic_value_dates": False,
        "payment_lag": 0,
        "payment_convention": "ModifiedFollowing",
        "payment_frequency": VALMER_USD_SOFR_PAYMENT_FREQUENCY,
        "payment_calendar_code": dict(VALMER_USD_SOFR_CALENDAR_CODE),
        "forward_start": "0D",
        "overnight_spread": 0.0,
        "pillar": "LastRelevantDate",
        "averaging_method": "Compound",
        "end_of_month": False,
        "fixed_payment_frequency": VALMER_USD_SOFR_PAYMENT_FREQUENCY,
        "fixed_calendar_code": dict(VALMER_USD_SOFR_CALENDAR_CODE),
        "day_counter": "Actual360",
        "day_counter_code": "Actual360",
        "date_generation_convention": "ModifiedFollowing",
    }


def _enrich_key_nodes_with_helper_dates(
    key_nodes: list[dict[str, Any]],
    *,
    valuation_ts: pd.Timestamp,
    error_class: type[ValueError],
) -> None:
    ql = _quantlib()
    valuation_date = _ql_date(valuation_ts)
    previous_evaluation_date = ql.Settings.instance().evaluationDate
    ql.Settings.instance().evaluationDate = valuation_date
    try:
        specs = helper_specs_from_key_nodes(
            key_nodes,
            overnight_index_resolver=resolve_valmer_overnight_index,
        )
        helpers = build_rate_helpers(specs)
        for node, helper in zip(key_nodes, helpers, strict=True):
            node["maturity_date"] = _ql_date_to_iso(helper.maturityDate())
            node["earliest_date"] = _ql_date_to_iso(helper.earliestDate())
            node["pillar_date"] = _ql_date_to_iso(helper.pillarDate())
    except Exception as exc:
        raise error_class("Unable to build Valmer curve helpers from key nodes.") from exc
    finally:
        ql.Settings.instance().evaluationDate = previous_evaluation_date


def _filter_usable_usd_sofr_key_nodes(
    key_nodes: list[dict[str, Any]],
    *,
    valuation_ts: pd.Timestamp,
) -> list[dict[str, Any]]:
    valuation_date = valuation_ts.date()
    filtered = []
    for node in key_nodes:
        if node.get("instrument_type") == "sofr_future":
            earliest = pd.Timestamp(str(node["earliest_date"])).date()
            if earliest < valuation_date:
                continue
        filtered.append(node)

    if not any(node.get("instrument_type") == "sofr_future" for node in filtered):
        raise ValmerUsdSofrCurveError(
            "IRS_USD_CURVE.csv contained no usable SOFR futures for the valuation date."
        )
    if not any(node.get("instrument_type") == "overnight_indexed_swap" for node in filtered):
        raise ValmerUsdSofrCurveError("IRS_USD_CURVE.csv contained no SOFR OIS helpers.")
    return filtered


def _reconstruct_valmer_curve_term_structure(
    key_nodes: list[dict[str, Any]],
    *,
    valuation_ts: pd.Timestamp,
    error_class: type[ValueError],
):
    ql = _quantlib()
    try:
        return reconstruct_curve_term_structure_from_key_nodes(
            key_nodes,
            valuation_date=_ql_date(valuation_ts),
            day_counter=ql.Actual360(),
            bootstrap_method="piecewise_log_linear_discount",
            extrapolation=True,
            overnight_index_resolver=resolve_valmer_overnight_index,
        )
    except Exception as exc:
        raise error_class("Unable to reconstruct Valmer curve from key nodes.") from exc


def _export_zero_rate_points(
    curve: Any,
    *,
    valuation_ts: pd.Timestamp,
    node_days: tuple[int, ...],
    error_class: type[ValueError],
) -> dict[int, float]:
    try:
        nodes = export_curve_observation_nodes(
            curve,
            valuation_date=_ql_date(valuation_ts),
            node_days=node_days,
            include_pillar_dates=True,
            config=VALMER_CURVE_EXPORT_CONFIG,
        )
    except Exception as exc:
        raise error_class("Bootstrapped Valmer curve produced no exportable points.") from exc

    points = {
        int(node["days_to_maturity"]): float(node["zero"])
        for node in nodes
        if int(node["days_to_maturity"]) > 0 and node.get("zero") is not None
    }
    if not points:
        raise error_class("Bootstrapped Valmer curve produced no pillar points.")
    return points


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


def _ql_date(value: pd.Timestamp):
    ql = _quantlib()
    timestamp = pd.Timestamp(value)
    return ql.Date(timestamp.day, timestamp.month, timestamp.year)


def _ql_date_to_iso(value) -> str:
    return f"{value.year():04d}-{int(value.month()):02d}-{value.dayOfMonth():02d}"


def _quantlib():
    import QuantLib as ql

    return ql
