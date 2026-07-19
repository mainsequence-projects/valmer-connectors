"""Shared storage and DataNode contracts for external reference-rate observations."""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

import pandas as pd
from msm.base import MarketsBase
from msm.data_nodes.indices import IndexDataNodeConfiguration, IndexTimestampedDataNode
from msm.models.indices import IndexTable
from msm.settings import INDEX_IDENTIFIER_DIMENSION
from pydantic import Field, field_validator, model_validator
from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from valmer_connectors.markets import ValmerMarketsTimeIndexMetaTableMixin


class ReferenceRateObservationError(RuntimeError):
    """Raised when a reference-rate configuration or source frame is invalid."""


class ReferenceRateObservationsStorage(ValmerMarketsTimeIndexMetaTableMixin, MarketsBase):
    """Daily external rates used for spread, policy-rate, and diagnostic analytics."""

    __metatable_identifier__: ClassVar[str] = "reference_rate_observations"
    __metatable_description__ = (
        "Daily external reference-rate observations keyed by UTC observation date "
        "and canonical Index identifier for cross-market spread, policy-rate, and "
        "diagnostic analytics."
    )
    __metatable_extra_hash_components__: ClassVar[dict[str, Any]] = {
        "storage_name": "reference_rate_observations",
    }
    __time_index_name__: ClassVar[str] = "time_index"
    __index_names__: ClassVar[list[str]] = ["time_index", INDEX_IDENTIFIER_DIMENSION]
    __cadence__: ClassVar[str] = "1d"

    time_index: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": (
                "Source observation date normalized to a nanosecond-resolution UTC timestamp."
            ),
        },
    )
    index_identifier: Mapped[str] = mapped_column(
        String(255),
        ForeignKey(
            f"{IndexTable.__table__.fullname}.unique_identifier",
            ondelete="RESTRICT",
        ),
        nullable=False,
        info={
            "label": "Index Identifier",
            "description": (
                "Canonical IndexTable.unique_identifier for the observed reference rate."
            ),
        },
    )
    rate: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Reference Rate",
            "description": (
                "Observed annualized rate in decimal form after normalizing the source "
                "percentage exactly once."
            ),
        },
    )


class ReferenceRateObservationConfiguration(IndexDataNodeConfiguration):
    """Hashed updater scope for one source-specific reference-rate producer."""

    index_unique_identifiers: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "Canonical Index unique identifiers updated by this source-specific "
            "reference-rate producer."
        ),
        examples=[["US_TREASURY_CMT_2Y", "US_TREASURY_CMT_10Y"]],
    )
    bootstrap_lookback_days: int = Field(
        default=90,
        ge=1,
        description=(
            "Fixed calendar-day lookback used only when storage has no progress "
            "for an index and no bounded backfill range is configured."
        ),
        examples=[90],
    )
    backfill_end: dt.datetime | None = Field(
        default=None,
        description="Inclusive UTC end of an explicit bounded backfill.",
        examples=["2026-04-18T00:00:00Z"],
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
    def _validate_backfill_bounds(self) -> ReferenceRateObservationConfiguration:
        has_start = self.offset_start is not None
        has_end = self.backfill_end is not None
        if has_start != has_end:
            raise ValueError("offset_start and backfill_end must be provided together.")
        if not has_start:
            return self

        assert self.offset_start is not None
        assert self.backfill_end is not None
        if self.offset_start.utcoffset() is None or self.backfill_end.utcoffset() is None:
            raise ValueError("offset_start and backfill_end must be timezone-aware.")
        self.offset_start = self.offset_start.astimezone(dt.UTC)
        self.backfill_end = self.backfill_end.astimezone(dt.UTC)
        if self.offset_start > self.backfill_end:
            raise ValueError("offset_start must be less than or equal to backfill_end.")
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
    """Resolve one identity's bounded backfill or normal incremental window."""

    if config.offset_start is not None:
        assert config.backfill_end is not None
        return ReferenceRateUpdateWindow(
            start_date=_normalize_date(config.offset_start),
            end_date=_normalize_date(config.backfill_end),
        )

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
        resolved_start = resolved_end - dt.timedelta(days=config.bootstrap_lookback_days - 1)
    else:
        resolved_start = _normalize_date(last_update) + dt.timedelta(days=1)
    if resolved_start > resolved_end:
        return None
    return ReferenceRateUpdateWindow(start_date=resolved_start, end_date=resolved_end)


def normalize_reference_rate_rows(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Normalize source rows to the shared storage frame contract."""

    frame = pd.DataFrame(list(rows))
    if frame.empty:
        return empty_reference_rate_frame()
    required = {"time_index", "index_identifier", "rate"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ReferenceRateObservationError(
            f"Reference-rate source rows are missing columns: {missing!r}."
        )
    if frame["rate"].isna().any():
        raise ReferenceRateObservationError("Reference-rate rows cannot contain null rates.")
    frame["rate"] = frame["rate"].astype(float)
    if not frame["rate"].map(math.isfinite).all():
        raise ReferenceRateObservationError("Reference-rate rows must contain finite rates.")
    return ReferenceRateObservationsNode.validate_frame(frame)


def empty_reference_rate_frame() -> pd.DataFrame:
    """Return an empty frame with the shared source-builder columns."""

    return pd.DataFrame(columns=["time_index", "index_identifier", "rate"])


class ReferenceRateObservationsNode(IndexTimestampedDataNode):
    """Base producer that combines independently resolved per-index source frames."""

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

    @classmethod
    def _required_storage_table(cls) -> type[ReferenceRateObservationsStorage]:
        return ReferenceRateObservationsStorage

    def set_runtime_end(
        self,
        runtime_end: dt.date | dt.datetime | str | pd.Timestamp | None,
    ) -> ReferenceRateObservationsNode:
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
                frames.append(frame)
        if not frames:
            return empty_reference_rate_frame()
        return normalize_reference_rate_rows(
            pd.concat(frames, axis=0).reset_index().to_dict(orient="records")
        )


def _normalize_date(value: dt.date | dt.datetime | str | pd.Timestamp) -> dt.date:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.normalize().date()


__all__ = [
    "ReferenceRateIndexDefinition",
    "ReferenceRateObservationConfiguration",
    "ReferenceRateObservationError",
    "ReferenceRateObservationsNode",
    "ReferenceRateObservationsStorage",
    "ReferenceRateUpdateWindow",
    "empty_reference_rate_frame",
    "normalize_reference_rate_rows",
    "resolve_reference_rate_update_window",
    "upsert_reference_rate_indexes",
]
