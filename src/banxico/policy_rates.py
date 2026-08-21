"""Banxico SIE policy-target observations for cross-market analytics."""

from __future__ import annotations

import datetime as dt
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from banxico.fixings import BanxicoFixingError, resolve_banxico_token
from banxico.settings import (
    BANXICO_POLICY_TARGET_INDEX_IDENTIFIER,
    BANXICO_TOKEN_SECRET_NAME,
)
from banxico.sie import BanxicoSieClient
from valmer_connectors.data_nodes.canonical_index_values import (
    ReferenceRateIndexDefinition,
    ReferenceRateIndexValuesNode,
    ReferenceRateObservationConfiguration,
    canonical_index_value_row,
    empty_index_values_frame,
    normalize_index_value_rows,
    resolve_reference_rate_update_window,
    upsert_reference_rate_indexes,
)


class BanxicoPolicyRateError(RuntimeError):
    """Raised when Banxico policy-rate metadata or observations are invalid."""


@dataclass(frozen=True)
class BanxicoPolicyRateDefinition:
    """Accepted Banxico SIE policy-target mapping."""

    index: ReferenceRateIndexDefinition
    required_title_terms: tuple[str, ...]

    @property
    def index_identifier(self) -> str:
        return self.index.unique_identifier

    @property
    def series_id(self) -> str:
        return self.index.source_series_id


BANXICO_POLICY_TARGET_DEFINITION = BanxicoPolicyRateDefinition(
    index=ReferenceRateIndexDefinition(
        unique_identifier=BANXICO_POLICY_TARGET_INDEX_IDENTIFIER,
        source_series_id="SF61745",
        display_name="Banco de Mexico policy target",
        description=(
            "Banco de Mexico overnight interbank policy target published by SIE "
            "for policy-rate and cross-market analytics."
        ),
        provider="Banco de Mexico",
        currency="MXN",
        country="MX",
        observation_type="policy_target",
    ),
    required_title_terms=("OBJETIVO", "TASA"),
)

DEFAULT_BANXICO_POLICY_RATE_DEFINITIONS = (BANXICO_POLICY_TARGET_DEFINITION,)
_MISSING_BANXICO_VALUES = {"", "N/E", "N/D", "N.A.", "NA", "-"}


def definitions_by_index_identifier(
    definitions: Iterable[BanxicoPolicyRateDefinition] = (
        DEFAULT_BANXICO_POLICY_RATE_DEFINITIONS
    ),
) -> dict[str, BanxicoPolicyRateDefinition]:
    items = tuple(definitions)
    resolved = {item.index_identifier: item for item in items}
    if len(resolved) != len(items):
        raise BanxicoPolicyRateError(
            "Banxico policy definitions contain duplicate index identifiers."
        )
    return resolved


def select_banxico_policy_rate_definitions(
    index_identifiers: Iterable[str] | None = None,
    *,
    definitions: Iterable[BanxicoPolicyRateDefinition] = (
        DEFAULT_BANXICO_POLICY_RATE_DEFINITIONS
    ),
) -> tuple[BanxicoPolicyRateDefinition, ...]:
    by_index = definitions_by_index_identifier(definitions)
    if index_identifiers is None:
        return tuple(by_index.values())
    selected: list[BanxicoPolicyRateDefinition] = []
    for index_identifier in index_identifiers:
        try:
            selected.append(by_index[index_identifier])
        except KeyError as exc:
            raise BanxicoPolicyRateError(
                f"Unsupported Banxico policy-rate index {index_identifier!r}."
            ) from exc
    return tuple(selected)


def validate_banxico_policy_metadata(
    metadata_payloads: Iterable[Mapping[str, Any]],
    definitions: Iterable[BanxicoPolicyRateDefinition],
) -> dict[str, Mapping[str, Any]]:
    """Validate SF61745 identity and policy-target title terms."""

    payload_by_id = {
        str(item.get("idSerie") or item.get("idserie") or "").strip(): item
        for item in metadata_payloads
    }
    validated: dict[str, Mapping[str, Any]] = {}
    for definition in definitions:
        payload = payload_by_id.get(definition.series_id)
        if payload is None:
            raise BanxicoPolicyRateError(
                f"Banxico metadata did not include series {definition.series_id!r}."
            )
        title = str(payload.get("titulo") or "").strip()
        normalized_title = _normalize_text(title)
        missing_terms = [
            term
            for term in definition.required_title_terms
            if _normalize_text(term) not in normalized_title
        ]
        if missing_terms:
            raise BanxicoPolicyRateError(
                f"Banxico series {definition.series_id!r} title {title!r} does not "
                f"match {definition.index_identifier!r}; missing terms {missing_terms!r}."
            )
        validated[definition.index_identifier] = payload
    return validated


def normalize_banxico_policy_observations(
    series_payloads: Iterable[Mapping[str, Any]],
    *,
    series_id_to_index_identifier: Mapping[str, str],
    definitions: Iterable[BanxicoPolicyRateDefinition] = (
        DEFAULT_BANXICO_POLICY_RATE_DEFINITIONS
    ),
) -> pd.DataFrame:
    """Normalize Banxico percentage observations to the shared storage contract."""

    rows: list[dict[str, Any]] = []
    definition_by_index = definitions_by_index_identifier(definitions)
    for series_payload in series_payloads:
        series_id = str(
            series_payload.get("idSerie") or series_payload.get("idserie") or ""
        ).strip()
        try:
            index_identifier = series_id_to_index_identifier[series_id]
        except KeyError as exc:
            raise BanxicoPolicyRateError(
                f"Unexpected Banxico policy series id {series_id!r}."
            ) from exc
        data = series_payload.get("datos") or []
        if not isinstance(data, list):
            raise BanxicoPolicyRateError(
                f"Banxico SIE series {series_id!r} datos must be a list."
            )
        for item in data:
            if not isinstance(item, Mapping):
                continue
            raw_value = item.get("dato")
            source_value = "" if raw_value is None else str(raw_value).strip()
            if source_value.upper() in _MISSING_BANXICO_VALUES:
                continue
            normalized_value = source_value.replace(",", "")
            try:
                numeric_value = float(normalized_value)
            except ValueError as exc:
                raise BanxicoPolicyRateError(
                    f"Banxico policy observation has invalid value {source_value!r}."
                ) from exc
            if not math.isfinite(numeric_value):
                raise BanxicoPolicyRateError(
                    "Banxico policy observation must be finite."
                )
            definition = definition_by_index.get(index_identifier)
            if definition is None:
                raise BanxicoPolicyRateError(
                    f"Missing Banxico definition for Index {index_identifier!r}."
                )
            rows.append(
                canonical_index_value_row(
                    time_index=pd.to_datetime(
                        item.get("fecha"),
                        format="%d/%m/%Y",
                        utc=True,
                        errors="raise",
                    ),
                    index_identifier=index_identifier,
                    value=numeric_value / 100.0,
                    metadata_json=definition.index.observation_metadata(
                        source_quote=numeric_value
                    ),
                )
            )
    return normalize_index_value_rows(rows) if rows else empty_index_values_frame()


class BanxicoPolicyRatesNode(ReferenceRateIndexValuesNode):
    """Publish the accepted Banco de Mexico policy-target series."""

    def __init__(
        self,
        config: ReferenceRateObservationConfiguration,
        *,
        hash_namespace: str | None = None,
    ) -> None:
        self.client: BanxicoSieClient | None = None
        self.validate_metadata = True
        self.definitions = DEFAULT_BANXICO_POLICY_RATE_DEFINITIONS
        super().__init__(config=config, hash_namespace=hash_namespace)

    def set_source(
        self,
        *,
        client: BanxicoSieClient,
        definitions: Sequence[BanxicoPolicyRateDefinition] | None = None,
        validate_metadata: bool = True,
    ) -> BanxicoPolicyRatesNode:
        self.client = client
        self.definitions = tuple(
            definitions or DEFAULT_BANXICO_POLICY_RATE_DEFINITIONS
        )
        self.validate_metadata = validate_metadata
        return self

    def _selected_definitions(self) -> tuple[BanxicoPolicyRateDefinition, ...]:
        return select_banxico_policy_rate_definitions(
            self.reference_rate_config.index_unique_identifiers,
            definitions=self.definitions,
        )

    def prepare_source(self) -> None:
        if self.client is None:
            raise BanxicoPolicyRateError(
                "BanxicoPolicyRatesNode requires a configured client."
            )
        if not self.validate_metadata:
            return
        selected = self._selected_definitions()
        metadata = self.client.fetch_series_metadata(item.series_id for item in selected)
        validate_banxico_policy_metadata(metadata, selected)

    def build_reference_rate_frame(
        self,
        *,
        update_statistics: Any,
        index_identifier: str,
    ) -> pd.DataFrame:
        if self.client is None:
            raise BanxicoPolicyRateError(
                "BanxicoPolicyRatesNode requires a configured client."
            )
        definition = definitions_by_index_identifier(self.definitions).get(index_identifier)
        if definition is None:
            raise BanxicoPolicyRateError(
                f"Unsupported Banxico policy-rate index {index_identifier!r}."
            )
        window = resolve_reference_rate_update_window(
            update_statistics=update_statistics,
            config=self.reference_rate_config,
            index_identifier=index_identifier,
            runtime_end=self.runtime_end,
        )
        if window is None:
            return empty_index_values_frame()
        payloads = self.client.fetch_series_data(
            [definition.series_id],
            start_date=window.start_date,
            end_date=window.end_date,
        )
        return normalize_banxico_policy_observations(
            payloads,
            series_id_to_index_identifier={
                definition.series_id: definition.index_identifier
            },
            definitions=self.definitions,
        )


def run_banxico_policy_rates_update(
    *,
    index_identifiers: Iterable[str] | None = None,
    token: str | None = None,
    token_secret_name: str = BANXICO_TOKEN_SECRET_NAME,
    validate_metadata: bool = True,
    runtime_end: dt.date | dt.datetime | str | pd.Timestamp | None = None,
    hash_namespace: str | None = None,
    force_update: bool = True,
) -> None:
    """Attach runtime state, register the policy Index, and execute the producer."""

    selected = select_banxico_policy_rate_definitions(index_identifiers)

    from valmer_connectors.instruments.bootstrap import bootstrap_runtime

    bootstrap_runtime(seed_static_rows=False)
    upsert_reference_rate_indexes([item.index for item in selected])
    if token is None:
        try:
            token = resolve_banxico_token(secret_name=token_secret_name)
        except BanxicoFixingError as exc:
            raise BanxicoPolicyRateError(str(exc)) from exc
    client = BanxicoSieClient(token=token)
    config = ReferenceRateObservationConfiguration(
        index_unique_identifiers=[item.index_identifier for item in selected],
    )
    node = BanxicoPolicyRatesNode(
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
    "BANXICO_POLICY_TARGET_DEFINITION",
    "BanxicoPolicyRateDefinition",
    "BanxicoPolicyRateError",
    "BanxicoPolicyRatesNode",
    "normalize_banxico_policy_observations",
    "run_banxico_policy_rates_update",
    "select_banxico_policy_rate_definitions",
    "validate_banxico_policy_metadata",
]
