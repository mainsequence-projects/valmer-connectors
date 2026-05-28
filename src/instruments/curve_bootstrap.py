from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

MEXICO_MARKET = "MX"


@dataclass(frozen=True)
class MexicanReferenceIndexDefinition:
    """Static Mexican reference-index seed data for core ms-markets Index rows."""

    unique_identifier: str
    display_name: str
    description: str
    index_family: str
    tenor_days: int
    metadata_json: Mapping[str, Any] = field(default_factory=dict)
    provider: str | None = None

    def to_index_payload(self) -> dict[str, Any]:
        metadata_json = {
            "market": MEXICO_MARKET,
            "index_family": self.index_family,
            "tenor_days": self.tenor_days,
        }
        metadata_json.update(dict(self.metadata_json))

        return {
            "unique_identifier": self.unique_identifier,
            "display_name": self.display_name,
            "description": self.description,
            "provider": self.provider,
            "metadata_json": metadata_json,
        }


MEXICAN_REFERENCE_INDEX_DEFINITIONS: tuple[MexicanReferenceIndexDefinition, ...] = (
    MexicanReferenceIndexDefinition(
        unique_identifier="TIIE_OVERNIGHT",
        display_name="TIIE overnight",
        description="Mexican overnight TIIE reference rate.",
        index_family="TIIE",
        tenor_days=1,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier="TIIE_28",
        display_name="TIIE 28D",
        description="Mexican 28-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=28,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier="TIIE_91",
        display_name="TIIE 91D",
        description="Mexican 91-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=91,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier="TIIE_182",
        display_name="TIIE 182D",
        description="Mexican 182-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=182,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier="CETE_28",
        display_name="CETE 28D",
        description="Mexican 28-day CETE reference index.",
        index_family="CETE",
        tenor_days=28,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier="CETE_91",
        display_name="CETE 91D",
        description="Mexican 91-day CETE reference index.",
        index_family="CETE",
        tenor_days=91,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier="CETE_182",
        display_name="CETE 182D",
        description="Mexican 182-day CETE reference index.",
        index_family="CETE",
        tenor_days=182,
    ),
)


def mexican_reference_index_payloads(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
) -> tuple[dict[str, Any], ...]:
    return tuple(definition.to_index_payload() for definition in definitions)


def create_mexican_reference_index_schemas(**schema_kwargs: Any):
    """Create the core Index MetaTable schema needed by Mexican reference rates."""

    from msm.api.indices import Index

    return Index.create_schemas(**schema_kwargs)


def upsert_mexican_reference_indexes(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    *,
    create_schemas: bool = True,
    **schema_kwargs: Any,
) -> dict[str, Any]:
    """Upsert the core Index rows required by Mexican curve and fixing code."""

    from msm.api.indices import Index

    if create_schemas:
        create_mexican_reference_index_schemas(**schema_kwargs)

    upserted = {}
    for payload in mexican_reference_index_payloads(definitions):
        index = Index.upsert(payload)
        upserted[index.unique_identifier] = index
    return upserted


def bootstrap_valmer_curve_indexes(**schema_kwargs: Any) -> dict[str, Any]:
    """Bootstrap the reference-index identities used by Valmer curve pricing."""

    return upsert_mexican_reference_indexes(**schema_kwargs)


__all__ = [
    "MEXICO_MARKET",
    "MEXICAN_REFERENCE_INDEX_DEFINITIONS",
    "MexicanReferenceIndexDefinition",
    "bootstrap_valmer_curve_indexes",
    "create_mexican_reference_index_schemas",
    "mexican_reference_index_payloads",
    "upsert_mexican_reference_indexes",
]
