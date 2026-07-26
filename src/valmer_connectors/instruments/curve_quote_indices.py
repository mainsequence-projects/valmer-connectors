from __future__ import annotations

import re

VALMER_CURVE_QUOTE_INDEX_PREFIX = "VALMER_CURVE_QUOTE"
VALMER_TIIE_DOMESTIC_OIS_SUFFIX = ".MXN.FTIIE.1D/28D.BANXICO"
VALMER_TIIE_CROSS_CURRENCY_SUFFIX = ".MXN.FTIIE.1D/USD.SOFR.1D.SOFR"
VALMER_USD_SOFR_OIS_SUFFIX = ".USD.SOFR.1D/1Y.SOFR"
VALMER_USD_FEDFUNDS_OIS_SUFFIX = ".USD.FEDFUNDS.1D/1Y.FEDFUNDS1"
VALMER_USD_FEDFUNDS_SOFR_BASIS_SUFFIX = ".FEDFUNDS.1D/SOFR.1D.SOFR"
VALMER_USD_SOFR_FUTURE_PATTERN = re.compile(
    r"^Future\.USD\.CME\.CME (?P<contract_code>SR[13]) "
    r"(?P<contract_type>EOM|IMM)\.(?P<month>[A-Z]{3})\.(?P<year>\d{2})$"
)
VALMER_TENOR_PATTERN = re.compile(r"^[1-9]\d*[DWMY]$")

_MONTH_NUMBER_BY_CODE = {
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


class ValmerCurveQuoteIndexIdentifierError(ValueError):
    """Raised when a raw Valmer curve quote has no canonical Index identity."""


def curve_quote_index_identifier_from_source(source_instrument_identifier: str) -> str:
    """Map one immutable Valmer source row name to its canonical quote Index."""

    source_identifier = str(source_instrument_identifier).strip()
    if not source_identifier:
        raise ValmerCurveQuoteIndexIdentifierError(
            "Valmer curve quote source identifier must be non-empty."
        )

    if source_identifier == "FX.USD.MXN":
        return f"{VALMER_CURVE_QUOTE_INDEX_PREFIX}.USDMXN_SPOT.MID"
    if source_identifier.startswith("FX.USD.MXN."):
        source_tenor = source_identifier.removeprefix("FX.USD.MXN.")
        return _tenor_identifier("USDMXN_FORWARD", source_tenor)

    future_match = VALMER_USD_SOFR_FUTURE_PATTERN.fullmatch(source_identifier)
    if future_match is not None:
        month_code = future_match.group("month")
        month_number = _MONTH_NUMBER_BY_CODE[month_code]
        reference_year = 2000 + int(future_match.group("year"))
        return (
            f"{VALMER_CURVE_QUOTE_INDEX_PREFIX}.SOFR_FUTURE."
            f"{future_match.group('contract_code')}.{reference_year}_{month_number:02d}."
            f"{future_match.group('contract_type')}.MID"
        )

    if not source_identifier.startswith("Swap."):
        raise _unsupported_source(source_identifier)

    parts = source_identifier.split(".")
    if source_identifier.endswith(VALMER_TIIE_DOMESTIC_OIS_SUFFIX):
        return _tenor_identifier("TIIE_OIS", _swap_tenor(parts, source_identifier))
    if source_identifier.endswith(VALMER_TIIE_CROSS_CURRENCY_SUFFIX):
        return _tenor_identifier(
            "TIIE_SOFR_XCCY_BASIS",
            _swap_tenor(parts, source_identifier),
        )
    if source_identifier.endswith(VALMER_USD_SOFR_OIS_SUFFIX):
        return _tenor_identifier("SOFR_OIS", _swap_tenor(parts, source_identifier))
    if source_identifier.endswith(VALMER_USD_FEDFUNDS_OIS_SUFFIX):
        return _tenor_identifier("FEDFUNDS_OIS", _swap_tenor(parts, source_identifier))
    if (
        source_identifier.startswith("Swap.USD.")
        and source_identifier.endswith(VALMER_USD_FEDFUNDS_SOFR_BASIS_SUFFIX)
    ):
        if len(parts) < 3:
            raise _unsupported_source(source_identifier)
        return _tenor_identifier("FEDFUNDS_SOFR_BASIS", parts[2])

    raise _unsupported_source(source_identifier)


def curve_quote_source_reference(source_instrument_identifier: str) -> dict[str, str]:
    """Return the typed ms-markets source reference for a Valmer quote row."""

    return {
        "type": "index",
        "identifier": curve_quote_index_identifier_from_source(
            source_instrument_identifier
        ),
    }


def _swap_tenor(parts: list[str], source_identifier: str) -> str:
    if len(parts) < 2:
        raise _unsupported_source(source_identifier)
    return parts[1]


def _tenor_identifier(source_family: str, source_tenor: str) -> str:
    normalized_tenor = str(source_tenor).strip().upper()
    is_fx_overnight = source_family == "USDMXN_FORWARD" and normalized_tenor in {
        "ON",
        "TN",
    }
    if not is_fx_overnight and VALMER_TENOR_PATTERN.fullmatch(normalized_tenor) is None:
        raise ValmerCurveQuoteIndexIdentifierError(
            f"Unsupported Valmer curve quote tenor {source_tenor!r}."
        )
    return f"{VALMER_CURVE_QUOTE_INDEX_PREFIX}.{source_family}.{normalized_tenor}.MID"


def _unsupported_source(source_identifier: str) -> ValmerCurveQuoteIndexIdentifierError:
    return ValmerCurveQuoteIndexIdentifierError(
        f"Unsupported Valmer curve quote source identifier {source_identifier!r}."
    )


__all__ = [
    "VALMER_CURVE_QUOTE_INDEX_PREFIX",
    "ValmerCurveQuoteIndexIdentifierError",
    "curve_quote_index_identifier_from_source",
    "curve_quote_source_reference",
]
