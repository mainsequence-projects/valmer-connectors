"""Persist complete Valmer curve-quote snapshots as canonical daily Index values."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any, ClassVar

import pandas as pd
import requests
from msm.data_nodes.indices import IndexDataNodeConfiguration
from pydantic import Field, field_validator

from valmer_connectors.data_nodes.canonical_index_values import (
    DailyIndexValuesNode,
    canonical_index_value_row,
    empty_index_values_frame,
    normalize_index_value_rows,
)
from valmer_connectors.instruments.curve_quote_indices import (
    curve_quote_index_identifier_from_source,
)
from valmer_connectors.instruments.rates_curves import (
    VALMER_TIIE_IRS_MXN_URL,
    VALMER_TIIE_IRS_SOURCE_FILE,
    VALMER_USD_SOFR_FUTURE_PATTERN,
    VALMER_USD_SOFR_IRS_SOURCE_FILE,
    VALMER_USD_SOFR_IRS_URL,
    classify_tiie_irs_mxn_row,
    classify_usd_sofr_irs_row,
    fetch_valmer_benchmark_date_content,
    parse_valmer_benchmark_date,
    read_tiie_irs_mxn_csv,
    read_usd_sofr_irs_csv,
)

VALMER_PROVIDER = "Valmer"
VALMER_QUOTE_SIDE = "mid"
MXN_QUOTE_FAMILIES = (
    "fx_spot",
    "fx_forward",
    "tiie_sofr_xccy_basis",
    "tiie_ois",
)
USD_QUOTE_FAMILIES = (
    "sofr_future",
    "sofr_ois",
    "fedfunds_ois",
    "fedfunds_sofr_basis",
)
INTEREST_RATE_FAMILIES = frozenset(
    {
        "tiie_ois",
        "sofr_ois",
        "fedfunds_ois",
        "fedfunds_sofr_basis",
        "tiie_sofr_xccy_basis",
    }
)


class ValmerCurveQuoteError(RuntimeError):
    """Raised when a Valmer quote snapshot cannot be published completely."""


class ValmerCurveQuoteIndexConfiguration(IndexDataNodeConfiguration):
    """Hashed scope for one immutable Valmer quote-file producer."""

    source_file: str = Field(
        ...,
        description="Immutable Valmer source-file key consumed by this producer.",
        examples=[VALMER_TIIE_IRS_SOURCE_FILE],
    )
    quote_side: str = Field(
        default=VALMER_QUOTE_SIDE,
        description="Quote side represented by every Index identity in this producer.",
        examples=["mid"],
    )
    source_families: tuple[str, ...] = Field(
        ...,
        min_length=1,
        description="Complete source-family set this producer must persist.",
        examples=[list(MXN_QUOTE_FAMILIES)],
    )

    @field_validator("source_file", "quote_side")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Valmer quote configuration strings cannot be empty.")
        return normalized

    @field_validator("source_families")
    @classmethod
    def _families_are_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(str(item).strip() for item in value)
        if any(not item for item in normalized):
            raise ValueError("source_families cannot contain empty values.")
        if len(normalized) != len(set(normalized)):
            raise ValueError("source_families cannot contain duplicates.")
        return normalized


def normalize_valmer_mxn_quote_snapshot(
    source_frame: pd.DataFrame,
    *,
    valuation_date: Any,
    source_file: str = VALMER_TIIE_IRS_SOURCE_FILE,
    quote_side: str = VALMER_QUOTE_SIDE,
) -> pd.DataFrame:
    """Normalize all 34 recognized IRS_MXN_CURVE rows without helper filtering."""

    rows: list[dict[str, Any]] = []
    for source_row in source_frame.itertuples(index=False):
        identifier = str(source_row.instrument_identifier).strip()
        broad_family = classify_tiie_irs_mxn_row(identifier)
        if broad_family == "unsupported":
            raise ValmerCurveQuoteError(f"Unsupported Valmer MXN quote row {identifier!r}.")
        if broad_family == "fx":
            family = "fx_spot" if identifier == "FX.USD.MXN" else "fx_forward"
        elif broad_family == "domestic_ois":
            family = "tiie_ois"
        else:
            family = "tiie_sofr_xccy_basis"
        rows.append(
            _normalize_quote_row(
                identifier=identifier,
                source_quote=source_row.quote,
                family=family,
                valuation_date=valuation_date,
                source_file=source_file,
                quote_side=quote_side,
            )
        )
    return _validate_complete_snapshot(rows, expected_families=MXN_QUOTE_FAMILIES)


def normalize_valmer_usd_quote_snapshot(
    source_frame: pd.DataFrame,
    *,
    valuation_date: Any,
    source_file: str = VALMER_USD_SOFR_IRS_SOURCE_FILE,
    quote_side: str = VALMER_QUOTE_SIDE,
) -> pd.DataFrame:
    """Normalize all 47 recognized IRS_USD_CURVE rows without helper filtering."""

    rows: list[dict[str, Any]] = []
    for source_row in source_frame.itertuples(index=False):
        identifier = str(source_row.instrument_identifier).strip()
        family = classify_usd_sofr_irs_row(identifier)
        if family == "unsupported":
            raise ValmerCurveQuoteError(f"Unsupported Valmer USD quote row {identifier!r}.")
        rows.append(
            _normalize_quote_row(
                identifier=identifier,
                source_quote=source_row.quote,
                family=family,
                valuation_date=valuation_date,
                source_file=source_file,
                quote_side=quote_side,
            )
        )
    return _validate_complete_snapshot(rows, expected_families=USD_QUOTE_FAMILIES)


def upsert_valmer_curve_quote_indexes(frame: pd.DataFrame) -> dict[str, Any]:
    """Register semantic Index types and every Index identity in a quote frame."""

    from msm.api.indices import Index, IndexType
    from msm.constants import (
        INDEX_TYPE_INTEREST_RATE,
        INDEX_TYPE_INTEREST_RATE_DEFINITION,
    )

    IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())
    IndexType.upsert(
        index_type="foreign_exchange",
        display_name="Foreign Exchange",
        description="Source-published FX spot and forward-point observables.",
    )
    IndexType.upsert(
        index_type="interest_rate_future",
        display_name="Interest Rate Future",
        description="Source-published interest-rate futures contract quotes.",
    )
    upserted = {}
    for row in frame.reset_index().itertuples(index=False):
        metadata = dict(row.metadata_json)
        family = metadata["source_family"]
        if family in INTEREST_RATE_FAMILIES:
            index_type = INDEX_TYPE_INTEREST_RATE
        elif family == "sofr_future":
            index_type = "interest_rate_future"
        else:
            index_type = "foreign_exchange"
        index = Index.upsert(
            unique_identifier=row.index_identifier,
            index_type=index_type,
            display_name=_quote_display_name(row.index_identifier, family=family),
            # ADR-0037 index framework: source-published quotes are custom
            # observations; value_format is display-only. `provider` was removed
            # from IndexUpsert and now rides in metadata_json.
            calculation_method="custom",
            value_format="decimal",
            description=(
                f"Valmer {row.quote_side if hasattr(row, 'quote_side') else 'mid'} "
                f"{family.replace('_', ' ')} quote used by curve construction."
            ),
            metadata_json={
                "provider": VALMER_PROVIDER,
                "source_file": metadata["source_file"],
                "source_instrument_identifier": metadata[
                    "source_instrument_identifier"
                ],
                "source_family": family,
                "quote_side": metadata["quote_side"],
            },
        )
        upserted[index.unique_identifier] = index
    return upserted


class _ValmerCurveQuoteIndexValuesNode(DailyIndexValuesNode):
    configuration_class: ClassVar[type[ValmerCurveQuoteIndexConfiguration]] = (
        ValmerCurveQuoteIndexConfiguration
    )
    expected_source_file: ClassVar[str]
    expected_families: ClassVar[tuple[str, ...]]
    source_url: ClassVar[str]

    def __init__(
        self,
        config: ValmerCurveQuoteIndexConfiguration,
        *,
        hash_namespace: str | None = None,
    ) -> None:
        if config.source_file != self.expected_source_file:
            raise ValmerCurveQuoteError(
                f"{self.__class__.__name__} requires source_file "
                f"{self.expected_source_file!r}."
            )
        if tuple(config.source_families) != self.expected_families:
            raise ValmerCurveQuoteError(
                f"{self.__class__.__name__} requires complete source_families "
                f"{self.expected_families!r}."
            )
        self.quote_config = config
        self._source_content: bytes | None = None
        self._benchmark_date_content: bytes | str | None = None
        super().__init__(config=config, hash_namespace=hash_namespace)

    def set_source(
        self,
        *,
        source_content: bytes,
        benchmark_date_content: bytes | str,
    ) -> _ValmerCurveQuoteIndexValuesNode:
        """Inject an exact source snapshot for deterministic tests or backfills."""

        self._source_content = source_content
        self._benchmark_date_content = benchmark_date_content
        return self

    def _load_source(self) -> tuple[bytes, bytes | str]:
        if self._source_content is not None and self._benchmark_date_content is not None:
            return self._source_content, self._benchmark_date_content
        benchmark = fetch_valmer_benchmark_date_content()
        response = requests.get(self.source_url, timeout=30)
        response.raise_for_status()
        return response.content, benchmark

    def normalize_source(self, source_content: bytes, valuation_date: Any) -> pd.DataFrame:
        raise NotImplementedError

    def update(self) -> pd.DataFrame:
        source_content, benchmark_content = self._load_source()
        valuation_date = parse_valmer_benchmark_date(
            benchmark_content,
            error_class=ValmerCurveQuoteError,
        )
        frame = self.normalize_source(source_content, valuation_date)
        upsert_valmer_curve_quote_indexes(frame)
        rows = []
        getter = getattr(self.update_statistics, "get_last_update_for_identity", None)
        for row in frame.reset_index().to_dict(orient="records"):
            last_update = getter(row["index_identifier"]) if callable(getter) else None
            if last_update is None or pd.Timestamp(row["time_index"]) > pd.Timestamp(
                last_update
            ):
                rows.append(row)
        return normalize_index_value_rows(rows) if rows else empty_index_values_frame()


class ValmerIrsMxnIndexValuesNode(_ValmerCurveQuoteIndexValuesNode):
    """Publish all IRS_MXN_CURVE observations before any MXN curve build."""

    expected_source_file = VALMER_TIIE_IRS_SOURCE_FILE
    expected_families = MXN_QUOTE_FAMILIES
    source_url = VALMER_TIIE_IRS_MXN_URL

    def normalize_source(self, source_content: bytes, valuation_date: Any) -> pd.DataFrame:
        return normalize_valmer_mxn_quote_snapshot(
            read_tiie_irs_mxn_csv(source_content),
            valuation_date=valuation_date,
            source_file=self.quote_config.source_file,
            quote_side=self.quote_config.quote_side,
        )


class ValmerIrsUsdIndexValuesNode(_ValmerCurveQuoteIndexValuesNode):
    """Publish all IRS_USD_CURVE observations before any USD curve build."""

    expected_source_file = VALMER_USD_SOFR_IRS_SOURCE_FILE
    expected_families = USD_QUOTE_FAMILIES
    source_url = VALMER_USD_SOFR_IRS_URL

    def normalize_source(self, source_content: bytes, valuation_date: Any) -> pd.DataFrame:
        return normalize_valmer_usd_quote_snapshot(
            read_usd_sofr_irs_csv(source_content),
            valuation_date=valuation_date,
            source_file=self.quote_config.source_file,
            quote_side=self.quote_config.quote_side,
        )


def mxn_quote_config() -> ValmerCurveQuoteIndexConfiguration:
    return ValmerCurveQuoteIndexConfiguration(
        source_file=VALMER_TIIE_IRS_SOURCE_FILE,
        quote_side=VALMER_QUOTE_SIDE,
        source_families=MXN_QUOTE_FAMILIES,
    )


def usd_quote_config() -> ValmerCurveQuoteIndexConfiguration:
    return ValmerCurveQuoteIndexConfiguration(
        source_file=VALMER_USD_SOFR_IRS_SOURCE_FILE,
        quote_side=VALMER_QUOTE_SIDE,
        source_families=USD_QUOTE_FAMILIES,
    )


def run_valmer_irs_mxn_quote_update() -> None:
    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    bootstrap_runtime(seed_static_rows=False)
    ValmerIrsMxnIndexValuesNode(mxn_quote_config()).run(force_update=True)


def run_valmer_irs_usd_quote_update() -> None:
    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    bootstrap_runtime(seed_static_rows=False)
    ValmerIrsUsdIndexValuesNode(usd_quote_config()).run(force_update=True)


def _normalize_quote_row(
    *,
    identifier: str,
    source_quote: Any,
    family: str,
    valuation_date: Any,
    source_file: str,
    quote_side: str,
) -> dict[str, Any]:
    try:
        numeric_quote = float(source_quote)
    except (TypeError, ValueError) as exc:
        raise ValmerCurveQuoteError(
            f"Invalid quote {source_quote!r} for {identifier!r}."
        ) from exc
    if not math.isfinite(numeric_quote):
        raise ValmerCurveQuoteError(f"Non-finite quote for {identifier!r}.")

    metadata: dict[str, Any] = {
        "provider": VALMER_PROVIDER,
        "source_file": source_file,
        "source_instrument_identifier": identifier,
        "source_family": family,
        "quote_side": quote_side,
        "source_quote": numeric_quote,
    }
    source_tenor = _source_tenor(identifier, family=family)
    if source_tenor is not None:
        metadata["source_tenor"] = source_tenor

    if family == "sofr_future":
        match = VALMER_USD_SOFR_FUTURE_PATTERN.fullmatch(identifier)
        if match is None:
            raise ValmerCurveQuoteError(f"Invalid SOFR future {identifier!r}.")
        value = numeric_quote
        metadata.update(
            {
                "quote_type": "futures_price",
                "quote_unit": "price",
                "source_quote_unit": "price",
                "contract_code": match.group("contract_code"),
                "contract_type": match.group("contract_type"),
                "reference_month": match.group("month"),
                "reference_year": 2000 + int(match.group("year")),
                "reference_frequency": (
                    "Monthly" if match.group("contract_code") == "SR1" else "Quarterly"
                ),
                "implied_rate_decimal": (100.0 - numeric_quote) / 100.0,
            }
        )
    elif family == "fx_spot":
        value = numeric_quote
        metadata.update(
            {
                "quote_type": "fx_spot",
                "quote_unit": "mxn_per_usd",
                "source_quote_unit": "mxn_per_usd",
                "base_currency": "USD",
                "quote_currency": "MXN",
            }
        )
    elif family == "fx_forward":
        value = numeric_quote / 10000.0
        metadata.update(
            {
                "quote_type": "forward_points",
                "quote_unit": "mxn_per_usd",
                "source_quote_unit": "points",
                "point_scale": 10000,
                "base_currency": "USD",
                "quote_currency": "MXN",
            }
        )
    else:
        value = numeric_quote / 100.0
        quote_type = "basis_spread" if "basis" in family else "par_swap_rate"
        metadata.update(
            {
                "quote_type": quote_type,
                "quote_unit": "decimal",
                "source_quote_unit": "percent",
            }
        )
        if family == "tiie_sofr_xccy_basis" and source_tenor in {"182M", "364M"}:
            metadata["normalized_helper_tenor"] = {
                "182M": "15Y",
                "364M": "30Y",
            }[source_tenor]
        if family == "fedfunds_sofr_basis":
            metadata["base_leg"] = "FEDFUNDS"
            metadata["quote_leg"] = "SOFR"
        elif family == "tiie_sofr_xccy_basis":
            metadata["base_leg"] = "USD_SOFR"
            metadata["quote_leg"] = "MXN_TIIE"

    return canonical_index_value_row(
        time_index=pd.Timestamp(valuation_date).normalize(),
        index_identifier=curve_quote_index_identifier_from_source(identifier),
        value=value,
        observation_status="ready",
        source_as_of=pd.Timestamp(valuation_date).normalize(),
        metadata_json=metadata,
    )


def _validate_complete_snapshot(
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_families: tuple[str, ...],
) -> pd.DataFrame:
    materialized = list(rows)
    frame = normalize_index_value_rows(materialized)
    flat = frame.reset_index()
    actual_families = {
        str(metadata["source_family"]) for metadata in flat["metadata_json"]
    }
    if actual_families != set(expected_families):
        raise ValmerCurveQuoteError(
            f"Valmer quote snapshot families {sorted(actual_families)!r} do not match "
            f"required families {sorted(expected_families)!r}."
        )
    if flat[["time_index", "index_identifier"]].duplicated().any():
        raise ValmerCurveQuoteError("Valmer quote snapshot contains duplicate Index rows.")
    return frame


def _source_tenor(identifier: str, *, family: str) -> str | None:
    if family == "fx_forward":
        return identifier.removeprefix("FX.USD.MXN.")
    if family == "fedfunds_sofr_basis":
        return identifier.split(".")[2]
    if family in INTEREST_RATE_FAMILIES:
        return identifier.split(".")[1]
    return None


def _quote_display_name(index_identifier: str, *, family: str) -> str:
    semantic_tail = index_identifier.removeprefix("VALMER_CURVE_QUOTE.").removesuffix(
        ".MID"
    )
    return f"Valmer {family.replace('_', ' ').title()} {semantic_tail} mid"


__all__ = [
    "MXN_QUOTE_FAMILIES",
    "USD_QUOTE_FAMILIES",
    "ValmerCurveQuoteError",
    "ValmerCurveQuoteIndexConfiguration",
    "ValmerIrsMxnIndexValuesNode",
    "ValmerIrsUsdIndexValuesNode",
    "mxn_quote_config",
    "normalize_valmer_mxn_quote_snapshot",
    "normalize_valmer_usd_quote_snapshot",
    "run_valmer_irs_mxn_quote_update",
    "run_valmer_irs_usd_quote_update",
    "upsert_valmer_curve_quote_indexes",
    "usd_quote_config",
]
