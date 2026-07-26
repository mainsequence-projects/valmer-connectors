"""Canonical daily Index-value storage and source-observation helpers."""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

import pandas as pd
from msm.data_nodes.indices import (
    IndexDataNodeConfiguration,
    IndexValuesDataNode,
    configured_index_values_storage,
)
from pydantic import Field, field_validator, model_validator

DailyIndexValuesStorage = configured_index_values_storage(cadence="1d")


class IndexObservationError(RuntimeError):
    """Raised when an external Index observation violates the canonical contract."""


class ReferenceRateObservationConfiguration(IndexDataNodeConfiguration):
    """Hashed updater scope for one source-specific reference-rate producer."""

    initial_history_years: ClassVar[int] = 5

    index_unique_identifiers: list[str] = Field(
        ...,
        min_length=1,
        description="Canonical Index identifiers updated by this source producer.",
        examples=[["US_TREASURY_CMT_2Y", "US_TREASURY_CMT_10Y"]],
    )

    @field_validator("index_unique_identifiers")
    @classmethod
    def _validate_index_unique_identifiers(cls, value: list[str]) -> list[str]:
        normalized = [str(item).strip() for item in value]
        if any(not item for item in normalized):
            raise ValueError("index_unique_identifiers cannot contain empty values.")
        if len(normalized) != len(set(normalized)):
            raise ValueError("index_unique_identifiers cannot contain duplicates.")
        return normalized

    @model_validator(mode="after")
    def _reject_manual_backfill_scope(self) -> ReferenceRateObservationConfiguration:
        if self.offset_start is not None:
            raise ValueError(
                "offset_start is not supported: reference-rate DataNodes load five "
                "calendar years on their first normal run and then update incrementally."
            )
        return self


@dataclass(frozen=True)
class ReferenceRateIndexDefinition:
    """Canonical Index identity and bounded source metadata for one external rate."""

    unique_identifier: str
    source_series_id: str
    display_name: str
    description: str
    provider: str
    currency: str
    country: str
    observation_type: str
    tenor_months: int | None = None
    source_agency: str | None = None

    def to_index_payload(self) -> dict[str, Any]:
        from msm.constants import INDEX_TYPE_INTEREST_RATE

        metadata_json: dict[str, Any] = {
            "source_series_id": self.source_series_id,
            "currency": self.currency,
            "country": self.country,
            "source_unit": "percent",
            "observation_type": self.observation_type,
        }
        if self.tenor_months is not None:
            metadata_json["tenor_months"] = self.tenor_months
        if self.source_agency is not None:
            metadata_json["source_agency"] = self.source_agency
        return {
            "unique_identifier": self.unique_identifier,
            "index_type": INDEX_TYPE_INTEREST_RATE,
            "display_name": self.display_name,
            "description": self.description,
            "provider": self.provider,
            "metadata_json": metadata_json,
        }

    def observation_metadata(self, *, source_quote: float) -> dict[str, Any]:
        metadata = {
            "provider": self.provider,
            "source_series_id": self.source_series_id,
            "source_quote": float(source_quote),
            "source_quote_unit": "percent",
            "currency": self.currency,
            "country": self.country,
            "observation_type": self.observation_type,
        }
        if self.tenor_months is not None:
            metadata["tenor_months"] = self.tenor_months
        return metadata


@dataclass(frozen=True)
class ReferenceRateUpdateWindow:
    """Inclusive source request window for one canonical Index identity."""

    start_date: dt.date
    end_date: dt.date


def upsert_reference_rate_indexes(
    definitions: Sequence[ReferenceRateIndexDefinition],
) -> dict[str, Any]:
    """Ensure the interest-rate type and canonical Index rows exist."""

    from msm.api.indices import Index, IndexType
    from msm.constants import INDEX_TYPE_INTEREST_RATE_DEFINITION

    IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())
    upserted: dict[str, Any] = {}
    for definition in definitions:
        index = Index.upsert(definition.to_index_payload())
        upserted[index.unique_identifier] = index
    return upserted


def resolve_reference_rate_update_window(
    *,
    update_statistics: Any,
    config: ReferenceRateObservationConfiguration,
    index_identifier: str,
    runtime_end: dt.date | dt.datetime | str | pd.Timestamp | None = None,
) -> ReferenceRateUpdateWindow | None:
    """Resolve one identity's five-year initial or normal incremental window."""

    resolved_end = _normalize_date(
        runtime_end
        if runtime_end is not None
        else pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)
    )
    last_update = None
    if update_statistics is not None:
        getter = getattr(update_statistics, "get_last_update_for_identity", None)
        if callable(getter):
            last_update = getter(index_identifier)
        else:
            index_progress = getattr(update_statistics, "index_progress", None) or {}
            last_update = index_progress.get(index_identifier)

    if last_update is None:
        resolved_start = (
            pd.Timestamp(resolved_end)
            - pd.DateOffset(years=config.initial_history_years)
            + pd.Timedelta(days=1)
        ).date()
    else:
        resolved_start = _normalize_date(last_update) + dt.timedelta(days=1)
    if resolved_start > resolved_end:
        return None
    return ReferenceRateUpdateWindow(start_date=resolved_start, end_date=resolved_end)


def canonical_index_value_row(
    *,
    time_index: Any,
    index_identifier: str,
    value: float,
    unit: str,
    source_as_of: Any | None = None,
    observation_status: str = "ready",
    metadata_json: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one plain/source-published canonical Index observation."""

    numeric_value = float(value)
    if not math.isfinite(numeric_value):
        raise IndexObservationError("Canonical Index values must be finite.")
    timestamp = pd.Timestamp(time_index)
    timestamp = (
        timestamp.tz_localize("UTC")
        if timestamp.tzinfo is None
        else timestamp.tz_convert("UTC")
    )
    resolved_source_as_of = timestamp if source_as_of is None else pd.Timestamp(source_as_of)
    resolved_source_as_of = (
        resolved_source_as_of.tz_localize("UTC")
        if resolved_source_as_of.tzinfo is None
        else resolved_source_as_of.tz_convert("UTC")
    )
    return {
        "time_index": timestamp,
        "index_identifier": str(index_identifier),
        "value": numeric_value,
        "unit": str(unit),
        "definition_uid": None,
        "observation_status": observation_status,
        "source_as_of": resolved_source_as_of,
        "metadata_json": dict(metadata_json) if metadata_json is not None else None,
    }


def normalize_index_value_rows(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Normalize rows against the one canonical daily Index-values contract."""

    frame = pd.DataFrame(list(rows))
    if frame.empty:
        return empty_index_values_frame()
    return DailyIndexValuesNode.validate_frame(frame)


def empty_index_values_frame() -> pd.DataFrame:
    """Return an empty canonical daily Index-values builder frame."""

    return pd.DataFrame(
        columns=[
            "time_index",
            "index_identifier",
            "value",
            "unit",
            "definition_uid",
            "observation_status",
            "source_as_of",
            "metadata_json",
        ]
    )


class DailyIndexValuesNode(IndexValuesDataNode):
    """Base producer for canonical daily Index observations."""

    @classmethod
    def _required_storage_table(cls):
        return DailyIndexValuesStorage


class ReferenceRateIndexValuesNode(DailyIndexValuesNode):
    """Base producer combining independently resolved reference-rate series."""

    configuration_class: ClassVar[type[ReferenceRateObservationConfiguration]] = (
        ReferenceRateObservationConfiguration
    )

    def __init__(
        self,
        config: ReferenceRateObservationConfiguration,
        *,
        hash_namespace: str | None = None,
    ) -> None:
        self.reference_rate_config = config
        self.runtime_end: dt.date | dt.datetime | str | pd.Timestamp | None = None
        super().__init__(config=config, hash_namespace=hash_namespace)

    def set_runtime_end(
        self,
        runtime_end: dt.date | dt.datetime | str | pd.Timestamp | None,
    ) -> ReferenceRateIndexValuesNode:
        self.runtime_end = runtime_end
        return self

    def prepare_source(self) -> None:
        """Validate and prepare the source once before per-index requests."""

    def build_reference_rate_frame(
        self,
        *,
        update_statistics: Any,
        index_identifier: str,
    ) -> pd.DataFrame:
        raise NotImplementedError

    def update(self) -> pd.DataFrame:
        self.prepare_source()
        frames: list[pd.DataFrame] = []
        for index_identifier in self.reference_rate_config.index_unique_identifiers:
            frame = self.build_reference_rate_frame(
                update_statistics=self.update_statistics,
                index_identifier=index_identifier,
            )
            if not frame.empty:
                frames.append(frame.reset_index())
        if not frames:
            return empty_index_values_frame()
        return normalize_index_value_rows(
            pd.concat(frames, axis=0, ignore_index=True).to_dict(orient="records")
        )


def _normalize_date(value: dt.date | dt.datetime | str | pd.Timestamp) -> dt.date:
    timestamp = pd.Timestamp(value)
    timestamp = (
        timestamp.tz_localize("UTC")
        if timestamp.tzinfo is None
        else timestamp.tz_convert("UTC")
    )
    return timestamp.normalize().date()


__all__ = [
    "DailyIndexValuesNode",
    "DailyIndexValuesStorage",
    "IndexObservationError",
    "ReferenceRateIndexDefinition",
    "ReferenceRateIndexValuesNode",
    "ReferenceRateObservationConfiguration",
    "ReferenceRateUpdateWindow",
    "canonical_index_value_row",
    "empty_index_values_frame",
    "normalize_index_value_rows",
    "resolve_reference_rate_update_window",
    "upsert_reference_rate_indexes",
]
