from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

MEXICO_MARKET = "MX"
MEXICAN_MARKET_SOURCE = "mexico"

TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER = "TIIE_OVERNIGHT"
TIIE_28_INDEX_UNIQUE_IDENTIFIER = "TIIE_28"
TIIE_91_INDEX_UNIQUE_IDENTIFIER = "TIIE_91"
TIIE_182_INDEX_UNIQUE_IDENTIFIER = "TIIE_182"
CETE_28_INDEX_UNIQUE_IDENTIFIER = "CETE_28"
CETE_91_INDEX_UNIQUE_IDENTIFIER = "CETE_91"
CETE_182_INDEX_UNIQUE_IDENTIFIER = "CETE_182"

VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER = "VALMER_TIIE_28"
VALMER_SOURCE = "valmer"


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
        from msm.constants import INDEX_TYPE_INTEREST_RATE

        return {
            "unique_identifier": self.unique_identifier,
            "index_type": INDEX_TYPE_INTEREST_RATE,
            "display_name": self.display_name,
            "description": self.description,
            "provider": self.provider,
            "metadata_json": dict(self.metadata_json) or None,
        }


MEXICAN_REFERENCE_INDEX_DEFINITIONS: tuple[MexicanReferenceIndexDefinition, ...] = (
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE overnight",
        description="Mexican overnight TIIE reference rate.",
        index_family="TIIE",
        tenor_days=1,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE 28D",
        description="Mexican 28-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=28,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE 91D",
        description="Mexican 91-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=91,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_182_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE 182D",
        description="Mexican 182-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=182,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=CETE_28_INDEX_UNIQUE_IDENTIFIER,
        display_name="CETE 28D",
        description="Mexican 28-day CETE reference index.",
        index_family="CETE",
        tenor_days=28,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=CETE_91_INDEX_UNIQUE_IDENTIFIER,
        display_name="CETE 91D",
        description="Mexican 91-day CETE reference index.",
        index_family="CETE",
        tenor_days=91,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=CETE_182_INDEX_UNIQUE_IDENTIFIER,
        display_name="CETE 182D",
        description="Mexican 182-day CETE reference index.",
        index_family="CETE",
        tenor_days=182,
    ),
)


@dataclass(frozen=True)
class MexicanIndexConventionDefinition:
    index_unique_identifier: str
    index_family: str
    tenor_days: int
    settlement_days: int
    business_day_convention: str
    day_counter_code: str = "Actual360"
    fixing_calendar_code: str = "Mexico-BMV"
    currency_code: str = "MXN"
    end_of_month: bool = False
    source: str = MEXICAN_MARKET_SOURCE
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_convention_payload(self, *, index_uid: Any) -> dict[str, Any]:
        return {
            "index_uid": index_uid,
            "index_family": self.index_family,
            "convention_dump": {
                "currency_code": self.currency_code,
                "day_counter_code": self.day_counter_code,
                "fixing_calendar_code": self.fixing_calendar_code,
                "period": f"{self.tenor_days}D",
                "settlement_days": self.settlement_days,
                "business_day_convention": self.business_day_convention,
                "end_of_month": self.end_of_month,
            },
            "source": self.source,
            "metadata_json": dict(self.metadata_json) or None,
        }


MEXICAN_INDEX_CONVENTION_DEFINITIONS: tuple[MexicanIndexConventionDefinition, ...] = (
    MexicanIndexConventionDefinition(
        index_unique_identifier=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        index_family="TIIE",
        tenor_days=1,
        settlement_days=1,
        business_day_convention="ModifiedFollowing",
    ),
    MexicanIndexConventionDefinition(
        index_unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        index_family="TIIE",
        tenor_days=28,
        settlement_days=1,
        business_day_convention="ModifiedFollowing",
    ),
    MexicanIndexConventionDefinition(
        index_unique_identifier=TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        index_family="TIIE",
        tenor_days=91,
        settlement_days=1,
        business_day_convention="ModifiedFollowing",
    ),
    MexicanIndexConventionDefinition(
        index_unique_identifier=TIIE_182_INDEX_UNIQUE_IDENTIFIER,
        index_family="TIIE",
        tenor_days=182,
        settlement_days=1,
        business_day_convention="ModifiedFollowing",
    ),
    MexicanIndexConventionDefinition(
        index_unique_identifier=CETE_28_INDEX_UNIQUE_IDENTIFIER,
        index_family="CETE",
        tenor_days=28,
        settlement_days=1,
        business_day_convention="Following",
    ),
    MexicanIndexConventionDefinition(
        index_unique_identifier=CETE_91_INDEX_UNIQUE_IDENTIFIER,
        index_family="CETE",
        tenor_days=91,
        settlement_days=1,
        business_day_convention="Following",
    ),
    MexicanIndexConventionDefinition(
        index_unique_identifier=CETE_182_INDEX_UNIQUE_IDENTIFIER,
        index_family="CETE",
        tenor_days=182,
        settlement_days=1,
        business_day_convention="Following",
    ),
)


@dataclass(frozen=True)
class ValmerCurveDefinition:
    unique_identifier: str
    display_name: str
    curve_type: str
    index_unique_identifier: str
    interpolation_method: str
    compounding: str
    source: str
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_curve_payload(self, *, index_uid: Any) -> dict[str, Any]:
        return {
            "unique_identifier": self.unique_identifier,
            "display_name": self.display_name,
            "curve_type": self.curve_type,
            "index_uid": index_uid,
            "interpolation_method": self.interpolation_method,
            "compounding": self.compounding,
            "source": self.source,
            "metadata_json": dict(self.metadata_json) or None,
        }


VALMER_TIIE_28_CURVE_DEFINITION = ValmerCurveDefinition(
    unique_identifier=VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
    display_name="Valmer TIIE 28 zero curve",
    curve_type="discount",
    index_unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    interpolation_method="log_linear_discount",
    compounding="compounded_annual",
    source=VALMER_SOURCE,
    metadata_json={
        "market": MEXICO_MARKET,
        "source_file": "MEXDERSWAP_IRSTIIEPR.csv",
        "source_url": "https://valmer.com.mx/VAL/Web_Benchmarks/MEXDERSWAP_IRSTIIEPR.csv",
    },
)


def mexican_reference_index_payloads(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
) -> tuple[dict[str, Any], ...]:
    return tuple(definition.to_index_payload() for definition in definitions)


def create_valmer_curve_pricing_schemas(**schema_kwargs: Any):
    """Create the current markets and pricing schemas needed by the curve bootstrap."""

    import msm
    from msm_pricing.bootstrap import create_pricing_schemas

    msm.start_engine(models=["AssetType", "Asset", "IndexType", "Index"], **schema_kwargs)
    return create_pricing_schemas(**schema_kwargs)


def upsert_interest_rate_index_type() -> Any:
    from msm.api.indices import IndexType
    from msm.constants import INDEX_TYPE_INTEREST_RATE_DEFINITION

    return IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())


def upsert_mexican_reference_indexes(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    *,
    create_schemas: bool = True,
    **schema_kwargs: Any,
) -> dict[str, Any]:
    """Upsert the core Index rows required by Mexican curve and fixing code."""

    from msm.api.indices import Index

    if create_schemas:
        create_valmer_curve_pricing_schemas(**schema_kwargs)

    upsert_interest_rate_index_type()
    upserted = {}
    for payload in mexican_reference_index_payloads(definitions):
        index = Index.upsert(payload)
        upserted[index.unique_identifier] = index
    return upserted


def upsert_mexican_index_convention_details(
    definitions: Sequence[MexicanIndexConventionDefinition] = (
        MEXICAN_INDEX_CONVENTION_DEFINITIONS
    ),
    *,
    indexes: Mapping[str, Any] | None = None,
    create_schemas: bool = True,
    **schema_kwargs: Any,
) -> dict[str, Any]:
    from msm_pricing.api import IndexConventionDetails

    resolved_indexes = indexes or upsert_mexican_reference_indexes(
        create_schemas=create_schemas,
        **schema_kwargs,
    )
    upserted = {}
    for definition in definitions:
        index = resolved_indexes[definition.index_unique_identifier]
        detail = IndexConventionDetails.upsert(
            definition.to_convention_payload(index_uid=index.uid)
        )
        upserted[definition.index_unique_identifier] = detail
    return upserted


def upsert_valmer_tiie_curve(
    definition: ValmerCurveDefinition = VALMER_TIIE_28_CURVE_DEFINITION,
    *,
    indexes: Mapping[str, Any] | None = None,
    create_schemas: bool = True,
    **schema_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    resolved_indexes = indexes or upsert_mexican_reference_indexes(
        create_schemas=create_schemas,
        **schema_kwargs,
    )
    upsert_mexican_index_convention_details(
        indexes=resolved_indexes,
        create_schemas=False,
    )
    index = resolved_indexes[definition.index_unique_identifier]
    return Curve.upsert(definition.to_curve_payload(index_uid=index.uid))


def bootstrap_valmer_curve_pricing(
    *,
    create_schemas: bool = True,
    **schema_kwargs: Any,
) -> dict[str, Any]:
    """Bootstrap the MetaTable rows required by Valmer curve publication."""

    if create_schemas:
        create_valmer_curve_pricing_schemas(**schema_kwargs)

    index_type = upsert_interest_rate_index_type()
    indexes = upsert_mexican_reference_indexes(create_schemas=False)
    conventions = upsert_mexican_index_convention_details(
        indexes=indexes,
        create_schemas=False,
    )
    curve = upsert_valmer_tiie_curve(
        indexes=indexes,
        create_schemas=False,
    )
    return {
        "index_type": index_type,
        "indexes": indexes,
        "index_conventions": conventions,
        "curves": {curve.unique_identifier: curve},
    }


def bootstrap_valmer_curve_indexes(**schema_kwargs: Any) -> dict[str, Any]:
    """Bootstrap the reference-index identities used by Valmer curve pricing."""

    return upsert_mexican_reference_indexes(**schema_kwargs)


__all__ = [
    "MEXICO_MARKET",
    "MEXICAN_INDEX_CONVENTION_DEFINITIONS",
    "MEXICAN_MARKET_SOURCE",
    "MEXICAN_REFERENCE_INDEX_DEFINITIONS",
    "VALMER_SOURCE",
    "VALMER_TIIE_28_CURVE_DEFINITION",
    "VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER",
    "CETE_182_INDEX_UNIQUE_IDENTIFIER",
    "CETE_28_INDEX_UNIQUE_IDENTIFIER",
    "CETE_91_INDEX_UNIQUE_IDENTIFIER",
    "MexicanReferenceIndexDefinition",
    "MexicanIndexConventionDefinition",
    "TIIE_182_INDEX_UNIQUE_IDENTIFIER",
    "TIIE_28_INDEX_UNIQUE_IDENTIFIER",
    "TIIE_91_INDEX_UNIQUE_IDENTIFIER",
    "TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER",
    "ValmerCurveDefinition",
    "bootstrap_valmer_curve_pricing",
    "bootstrap_valmer_curve_indexes",
    "create_valmer_curve_pricing_schemas",
    "mexican_reference_index_payloads",
    "upsert_interest_rate_index_type",
    "upsert_mexican_index_convention_details",
    "upsert_mexican_reference_indexes",
    "upsert_valmer_tiie_curve",
]
