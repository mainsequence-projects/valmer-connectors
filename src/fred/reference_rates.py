"""FRED-backed Treasury and Federal Funds target observations."""

from __future__ import annotations

import datetime as dt
import math
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from fred.client import FredApiError, FredClient
from fred.settings import (
    FED_FUNDS_TARGET_UPPER_INDEX_IDENTIFIER,
    FRED_API_KEY_SECRET_NAME,
    FRED_REFERENCE_RATE_INDEX_IDENTIFIERS,
    US_TREASURY_CMT_2Y_INDEX_IDENTIFIER,
    US_TREASURY_CMT_5Y_INDEX_IDENTIFIER,
    US_TREASURY_CMT_10Y_INDEX_IDENTIFIER,
    US_TREASURY_CMT_30Y_INDEX_IDENTIFIER,
)
from valmer_connectors.data_nodes.reference_rate_observations import (
    ReferenceRateIndexDefinition,
    ReferenceRateObservationConfiguration,
    ReferenceRateObservationsNode,
    empty_reference_rate_frame,
    normalize_reference_rate_rows,
    resolve_reference_rate_update_window,
    upsert_reference_rate_indexes,
)


class FredReferenceRateError(RuntimeError):
    """Raised when accepted FRED series metadata or observations are invalid."""


@dataclass(frozen=True)
class FredReferenceRateDefinition:
    """Accepted FRED series mapping and metadata-validation terms."""

    index: ReferenceRateIndexDefinition
    required_title_terms: tuple[str, ...]

    @property
    def index_identifier(self) -> str:
        return self.index.unique_identifier

    @property
    def series_id(self) -> str:
        return self.index.source_series_id


def _treasury_definition(
    *,
    index_identifier: str,
    series_id: str,
    tenor_years: int,
) -> FredReferenceRateDefinition:
    return FredReferenceRateDefinition(
        index=ReferenceRateIndexDefinition(
            unique_identifier=index_identifier,
            source_series_id=series_id,
            display_name=f"US Treasury {tenor_years}Y constant maturity",
            description=(
                f"Daily {tenor_years}-year US Treasury constant-maturity yield "
                "retrieved through FRED for cross-market analytics."
            ),
            provider="FRED",
            currency="USD",
            country="US",
            observation_type="treasury_constant_maturity_yield",
            tenor_months=tenor_years * 12,
            source_agency="Board of Governors of the Federal Reserve System",
        ),
        required_title_terms=("TREASURY", f"{tenor_years} YEAR", "CONSTANT MATURITY"),
    )


DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS: tuple[FredReferenceRateDefinition, ...] = (
    _treasury_definition(
        index_identifier=US_TREASURY_CMT_2Y_INDEX_IDENTIFIER,
        series_id="DGS2",
        tenor_years=2,
    ),
    _treasury_definition(
        index_identifier=US_TREASURY_CMT_5Y_INDEX_IDENTIFIER,
        series_id="DGS5",
        tenor_years=5,
    ),
    _treasury_definition(
        index_identifier=US_TREASURY_CMT_10Y_INDEX_IDENTIFIER,
        series_id="DGS10",
        tenor_years=10,
    ),
    _treasury_definition(
        index_identifier=US_TREASURY_CMT_30Y_INDEX_IDENTIFIER,
        series_id="DGS30",
        tenor_years=30,
    ),
    FredReferenceRateDefinition(
        index=ReferenceRateIndexDefinition(
            unique_identifier=FED_FUNDS_TARGET_UPPER_INDEX_IDENTIFIER,
            source_series_id="DFEDTARU",
            display_name="Federal Funds target range upper limit",
            description=(
                "Daily upper limit of the Federal Funds target range retrieved "
                "through FRED for policy-rate analytics."
            ),
            provider="FRED",
            currency="USD",
            country="US",
            observation_type="policy_target_upper_limit",
            source_agency="Board of Governors of the Federal Reserve System",
        ),
        required_title_terms=("FEDERAL FUNDS", "TARGET RANGE", "UPPER LIMIT"),
    ),
)


def resolve_fred_api_key(
    *,
    secret_name: str = FRED_API_KEY_SECRET_NAME,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve the FRED API key from the environment, then Main Sequence Secret."""

    env = environ or os.environ
    api_key = (env.get(secret_name) or "").strip()
    if api_key:
        return api_key

    try:
        from mainsequence.client import Secret

        secret = Secret.get(name=secret_name)
        secret_value = secret.value
        secret_uid = getattr(secret, "uid", None)
        if secret_value is None and secret_uid:
            secret_value = Secret.get_by_uid(secret_uid).value
        api_key = (
            secret_value.get_secret_value()
            if hasattr(secret_value, "get_secret_value")
            else str(secret_value or "")
        ).strip()
    except Exception as exc:
        raise FredReferenceRateError(
            f"Main Sequence Secret {secret_name!r} is required for FRED access."
        ) from exc

    if not api_key:
        raise FredReferenceRateError(f"Main Sequence Secret {secret_name!r} is empty.")
    return api_key


def definitions_by_index_identifier(
    definitions: Iterable[FredReferenceRateDefinition] = (
        DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS
    ),
) -> dict[str, FredReferenceRateDefinition]:
    items = tuple(definitions)
    resolved = {item.index_identifier: item for item in items}
    if len(resolved) != len(items):
        raise FredReferenceRateError("FRED definitions contain duplicate index identifiers.")
    return resolved


def select_fred_reference_rate_definitions(
    index_identifiers: Iterable[str] | None = None,
    *,
    definitions: Iterable[FredReferenceRateDefinition] = (
        DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS
    ),
) -> tuple[FredReferenceRateDefinition, ...]:
    by_index = definitions_by_index_identifier(definitions)
    if index_identifiers is None:
        return tuple(by_index.values())
    selected: list[FredReferenceRateDefinition] = []
    for index_identifier in index_identifiers:
        try:
            selected.append(by_index[index_identifier])
        except KeyError as exc:
            raise FredReferenceRateError(
                f"Unsupported FRED reference-rate index {index_identifier!r}."
            ) from exc
    return tuple(selected)


def validate_fred_series_metadata(
    metadata_payloads: Iterable[Mapping[str, Any]],
    definitions: Iterable[FredReferenceRateDefinition],
) -> dict[str, Mapping[str, Any]]:
    """Validate identity, title, frequency, unit, and seasonal-adjustment metadata."""

    payload_by_id = {str(item.get("id") or "").strip(): item for item in metadata_payloads}
    validated: dict[str, Mapping[str, Any]] = {}
    for definition in definitions:
        payload = payload_by_id.get(definition.series_id)
        if payload is None:
            raise FredReferenceRateError(
                f"FRED metadata did not include series {definition.series_id!r}."
            )
        title = str(payload.get("title") or "").strip()
        normalized_title = _normalize_text(title)
        missing_terms = [
            term
            for term in definition.required_title_terms
            if _normalize_text(term) not in normalized_title
        ]
        if missing_terms:
            raise FredReferenceRateError(
                f"FRED series {definition.series_id!r} title {title!r} does not match "
                f"{definition.index_identifier!r}; missing terms {missing_terms!r}."
            )

        frequency = _normalize_text(
            str(payload.get("frequency") or payload.get("frequency_short") or "")
        )
        if frequency not in {"D", "DAILY"} and "DAILY" not in frequency:
            raise FredReferenceRateError(
                f"FRED series {definition.series_id!r} must have daily frequency."
            )
        units = _normalize_text(str(payload.get("units") or ""))
        units_short = str(payload.get("units_short") or "").strip()
        if units != "PERCENT" and units_short != "%":
            raise FredReferenceRateError(
                f"FRED series {definition.series_id!r} must use percent units."
            )
        seasonal = _normalize_text(
            str(
                payload.get("seasonal_adjustment")
                or payload.get("seasonal_adjustment_short")
                or ""
            )
        )
        if seasonal not in {"NSA", "NOT SEASONALLY ADJUSTED"}:
            raise FredReferenceRateError(
                f"FRED series {definition.series_id!r} must be not seasonally adjusted."
            )
        validated[definition.index_identifier] = payload
    return validated


def normalize_fred_observations(
    observations: Iterable[Mapping[str, Any]],
    *,
    index_identifier: str,
) -> pd.DataFrame:
    """Normalize FRED percentage observations without filling missing dates."""

    rows: list[dict[str, Any]] = []
    for observation in observations:
        raw_value = observation.get("value")
        source_value = "" if raw_value is None else str(raw_value).strip()
        if source_value == ".":
            continue
        try:
            numeric_value = float(source_value)
        except (TypeError, ValueError) as exc:
            raise FredReferenceRateError(
                f"FRED observation for {index_identifier!r} has invalid value "
                f"{source_value!r}."
            ) from exc
        if not math.isfinite(numeric_value):
            raise FredReferenceRateError(
                f"FRED observation for {index_identifier!r} is not finite."
            )
        rows.append(
            {
                "time_index": observation.get("date"),
                "index_identifier": index_identifier,
                "rate": numeric_value / 100.0,
            }
        )
    return normalize_reference_rate_rows(rows) if rows else empty_reference_rate_frame()


class FredReferenceRatesNode(ReferenceRateObservationsNode):
    """Publish accepted FRED Treasury yields and the Fed target upper limit."""

    def __init__(
        self,
        config: ReferenceRateObservationConfiguration,
        *,
        hash_namespace: str | None = None,
    ) -> None:
        self.client: FredClient | None = None
        self.validate_metadata = True
        self.definitions = DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS
        super().__init__(config=config, hash_namespace=hash_namespace)

    def set_source(
        self,
        *,
        client: FredClient,
        definitions: Sequence[FredReferenceRateDefinition] | None = None,
        validate_metadata: bool = True,
    ) -> FredReferenceRatesNode:
        self.client = client
        self.definitions = tuple(definitions or DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS)
        self.validate_metadata = validate_metadata
        return self

    def _selected_definitions(self) -> tuple[FredReferenceRateDefinition, ...]:
        return select_fred_reference_rate_definitions(
            self.reference_rate_config.index_unique_identifiers,
            definitions=self.definitions,
        )

    def prepare_source(self) -> None:
        if self.client is None:
            raise FredReferenceRateError("FredReferenceRatesNode requires a configured client.")
        if not self.validate_metadata:
            return
        selected = self._selected_definitions()
        metadata = [self.client.fetch_series_metadata(item.series_id) for item in selected]
        validate_fred_series_metadata(metadata, selected)

    def build_reference_rate_frame(
        self,
        *,
        update_statistics: Any,
        index_identifier: str,
    ) -> pd.DataFrame:
        if self.client is None:
            raise FredReferenceRateError("FredReferenceRatesNode requires a configured client.")
        definition = definitions_by_index_identifier(self.definitions).get(index_identifier)
        if definition is None:
            raise FredReferenceRateError(
                f"Unsupported FRED reference-rate index {index_identifier!r}."
            )
        window = resolve_reference_rate_update_window(
            update_statistics=update_statistics,
            config=self.reference_rate_config,
            index_identifier=index_identifier,
            runtime_end=self.runtime_end,
        )
        if window is None:
            return empty_reference_rate_frame()
        observations = self.client.fetch_series_observations(
            definition.series_id,
            start_date=window.start_date,
            end_date=window.end_date,
        )
        return normalize_fred_observations(
            observations,
            index_identifier=index_identifier,
        )


def run_fred_reference_rates_update(
    *,
    index_identifiers: Iterable[str] | None = None,
    api_key: str | None = None,
    api_key_secret_name: str = FRED_API_KEY_SECRET_NAME,
    validate_metadata: bool = True,
    bootstrap_lookback_days: int = 90,
    backfill_start: dt.datetime | str | None = None,
    backfill_end: dt.datetime | str | None = None,
    runtime_end: dt.date | dt.datetime | str | pd.Timestamp | None = None,
    hash_namespace: str | None = None,
    require_hash_namespace: bool = False,
    force_update: bool = True,
) -> None:
    """Attach runtime state, register FRED indexes, and execute the producer."""

    if require_hash_namespace and not hash_namespace:
        raise FredReferenceRateError(
            "The first shared-backend smoke run requires an explicit hash namespace."
        )
    selected = select_fred_reference_rate_definitions(index_identifiers)

    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    bootstrap_runtime(seed_static_rows=False)
    upsert_reference_rate_indexes([item.index for item in selected])
    client = FredClient(
        api_key=api_key or resolve_fred_api_key(secret_name=api_key_secret_name)
    )
    config = ReferenceRateObservationConfiguration(
        index_unique_identifiers=[item.index_identifier for item in selected],
        bootstrap_lookback_days=bootstrap_lookback_days,
        offset_start=backfill_start,
        backfill_end=backfill_end,
    )
    node = FredReferenceRatesNode(
        config,
        hash_namespace=hash_namespace,
    ).set_source(
        client=client,
        definitions=selected,
        validate_metadata=validate_metadata,
    )
    node.set_runtime_end(runtime_end).run(force_update=force_update)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", value.upper()).strip()


__all__ = [
    "DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS",
    "FRED_REFERENCE_RATE_INDEX_IDENTIFIERS",
    "FredApiError",
    "FredClient",
    "FredReferenceRateDefinition",
    "FredReferenceRateError",
    "FredReferenceRatesNode",
    "normalize_fred_observations",
    "resolve_fred_api_key",
    "run_fred_reference_rates_update",
    "select_fred_reference_rate_definitions",
    "validate_fred_series_metadata",
]
