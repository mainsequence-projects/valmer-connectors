from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

MEXICO_MARKET = "MX"
MEXICAN_MARKET_SOURCE = "mexico"
BANCO_DE_MEXICO_PROVIDER = "Banco de Mexico"

TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER = "TIIE_OVERNIGHT"
TIIE_28_INDEX_UNIQUE_IDENTIFIER = "TIIE_28"
TIIE_91_INDEX_UNIQUE_IDENTIFIER = "TIIE_91"
TIIE_182_INDEX_UNIQUE_IDENTIFIER = "TIIE_182"
CETE_28_INDEX_UNIQUE_IDENTIFIER = "CETE_28"
CETE_91_INDEX_UNIQUE_IDENTIFIER = "CETE_91"
CETE_182_INDEX_UNIQUE_IDENTIFIER = "CETE_182"
MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER = "MXN_GOVERNMENT_BOND"

VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER = "VALMER_TIIE_28"
VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER = "VALMER_MXN_GOVERNMENT_BOND"
VALMER_SOURCE = "valmer"
VALMER_DISCOUNT_CURVES_CADENCE = "1d"


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
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE 28D",
        description="Mexican 28-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=28,
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE 91D",
        description="Mexican 91-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=91,
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=TIIE_182_INDEX_UNIQUE_IDENTIFIER,
        display_name="TIIE 182D",
        description="Mexican 182-day TIIE reference rate.",
        index_family="TIIE",
        tenor_days=182,
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=CETE_28_INDEX_UNIQUE_IDENTIFIER,
        display_name="CETE 28D",
        description="Mexican 28-day CETE reference index.",
        index_family="CETE",
        tenor_days=28,
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=CETE_91_INDEX_UNIQUE_IDENTIFIER,
        display_name="CETE 91D",
        description="Mexican 91-day CETE reference index.",
        index_family="CETE",
        tenor_days=91,
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=CETE_182_INDEX_UNIQUE_IDENTIFIER,
        display_name="CETE 182D",
        description="Mexican 182-day CETE reference index.",
        index_family="CETE",
        tenor_days=182,
        provider=BANCO_DE_MEXICO_PROVIDER,
    ),
    MexicanReferenceIndexDefinition(
        unique_identifier=MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER,
        display_name="MXN government bond benchmark",
        description="Mexican government MXN bond benchmark used for Valmer curve pricing.",
        index_family="MXN_GOVERNMENT_BOND",
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
    calendar_code: str | None = None
    currency_code: str = "MXN"
    end_of_month: bool = False
    coupon_period_days: int | None = None
    date_generation_rule: str | None = None
    source: str = MEXICAN_MARKET_SOURCE
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_convention_payload(self, *, index_uid: Any) -> dict[str, Any]:
        convention_dump = {
            "currency_code": self.currency_code,
            "day_counter_code": self.day_counter_code,
            "fixing_calendar_code": self.fixing_calendar_code,
            "period": f"{self.tenor_days}D",
            "settlement_days": self.settlement_days,
            "business_day_convention": self.business_day_convention,
            "end_of_month": self.end_of_month,
        }
        if self.calendar_code is not None:
            convention_dump["calendar_code"] = self.calendar_code
        if self.coupon_period_days is not None:
            convention_dump["coupon_period_days"] = self.coupon_period_days
        if self.date_generation_rule is not None:
            convention_dump["date_generation_rule"] = self.date_generation_rule
        return {
            "index_uid": index_uid,
            "index_family": self.index_family,
            "convention_dump": convention_dump,
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
    MexicanIndexConventionDefinition(
        index_unique_identifier=MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER,
        index_family="MXN_GOVERNMENT_BOND",
        tenor_days=182,
        settlement_days=0,
        business_day_convention="Following",
        fixing_calendar_code="Mexico-BMV",
        calendar_code="Mexico/BMV",
        coupon_period_days=182,
        date_generation_rule="Backward",
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

VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION = ValmerCurveDefinition(
    unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    display_name="Valmer MXN government bond discount curve",
    curve_type="discount",
    index_unique_identifier=MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER,
    interpolation_method="log_linear_discount",
    compounding="compounded_annual",
    source=VALMER_SOURCE,
    metadata_json={
        "market": MEXICO_MARKET,
        "source_file": "Vector Analitico",
        "instrument_families": ["BI_CETES", "M_BONOS"],
    },
)

VALMER_CURVE_DEFINITIONS: tuple[ValmerCurveDefinition, ...] = (
    VALMER_TIIE_28_CURVE_DEFINITION,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION,
)


def mexican_reference_index_payloads(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
) -> tuple[dict[str, Any], ...]:
    return tuple(definition.to_index_payload() for definition in definitions)


def attach_valmer_curve_pricing_runtime(
    *,
    markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
):
    """Attach markets/pricing runtime objects needed by Valmer curve bootstrap."""

    import msm
    from msm_pricing.bootstrap import attach_pricing_schemas

    configure_valmer_discount_curves_cadence()
    models = ["AssetType", "Asset", "IndexType", "Index"]
    if markets_models is None:
        from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable

        markets_models = [ValmerAssetDetailsTable]
    if markets_models is not None:
        models.extend(markets_models)
    msm.start_engine(models=models, **runtime_kwargs)
    return attach_pricing_schemas(
        models=valmer_pricing_runtime_models(),
        **runtime_kwargs,
    )


def valmer_pricing_runtime_models() -> list[type[Any]]:
    """Pricing MetaTables required by Valmer instrument and curve workflows."""

    from msm.models import AssetTable, IndexTable, IndexTypeTable
    from msm_pricing.data_nodes.pricing_details.storage import AssetPricingDetailsStorage
    from msm_pricing.models.curves import CurveTable
    from msm_pricing.models.index_convention_details import IndexConventionDetailsTable
    from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

    return [
        AssetTable,
        IndexTypeTable,
        IndexTable,
        IndexConventionDetailsTable,
        CurveTable,
        AssetCurrentPricingDetailsTable,
        AssetPricingDetailsStorage,
    ]


def configure_valmer_discount_curves_cadence(
    cadence: str = VALMER_DISCOUNT_CURVES_CADENCE,
) -> type[Any]:
    """Set the imported core discount-curve storage cadence for Valmer daily curves."""

    from msm_pricing.data_nodes import DiscountCurvesNode

    storage_table = DiscountCurvesNode._required_storage_table()
    storage_table.__cadence__ = cadence
    return storage_table


def create_valmer_curve_pricing_schemas(
    *,
    markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
):
    """Compatibility wrapper for the old schema-oriented helper name."""

    return attach_valmer_curve_pricing_runtime(
        markets_models=markets_models,
        **runtime_kwargs,
    )


def upsert_interest_rate_index_type() -> Any:
    from msm.api.indices import IndexType
    from msm.constants import INDEX_TYPE_INTEREST_RATE_DEFINITION

    return IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())


def upsert_mexican_reference_indexes(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    """Upsert the core Index rows required by Mexican curve and fixing code."""

    from msm.api.indices import Index

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(**runtime_kwargs)

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
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    from msm_pricing.api import IndexConventionDetails

    if create_schemas is not None:
        attach_runtime = create_schemas
    resolved_indexes = indexes or upsert_mexican_reference_indexes(
        attach_runtime=attach_runtime,
        **runtime_kwargs,
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
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    if create_schemas is not None:
        attach_runtime = create_schemas
    resolved_indexes = indexes or upsert_mexican_reference_indexes(
        attach_runtime=attach_runtime,
        **runtime_kwargs,
    )
    upsert_mexican_index_convention_details(
        indexes=resolved_indexes,
        attach_runtime=False,
    )
    index = resolved_indexes[definition.index_unique_identifier]
    return Curve.upsert(definition.to_curve_payload(index_uid=index.uid))


def upsert_valmer_mxn_government_bond_curve(
    definition: ValmerCurveDefinition = VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION,
    *,
    indexes: Mapping[str, Any] | None = None,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    if create_schemas is not None:
        attach_runtime = create_schemas
    resolved_indexes = indexes or upsert_mexican_reference_indexes(
        attach_runtime=attach_runtime,
        **runtime_kwargs,
    )
    upsert_mexican_index_convention_details(
        indexes=resolved_indexes,
        attach_runtime=False,
    )
    index = resolved_indexes[definition.index_unique_identifier]
    return Curve.upsert(definition.to_curve_payload(index_uid=index.uid))


def bootstrap_valmer_curve_pricing(
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    """Bootstrap the MetaTable rows required by Valmer curve publication."""

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(
            markets_models=markets_models,
            **runtime_kwargs,
        )

    index_type = upsert_interest_rate_index_type()
    indexes = upsert_mexican_reference_indexes(attach_runtime=False)
    conventions = upsert_mexican_index_convention_details(
        indexes=indexes,
        attach_runtime=False,
    )
    curves = {
        curve.unique_identifier: curve
        for curve in (
            upsert_valmer_tiie_curve(indexes=indexes, attach_runtime=False),
            upsert_valmer_mxn_government_bond_curve(indexes=indexes, attach_runtime=False),
        )
    }
    return {
        "index_type": index_type,
        "indexes": indexes,
        "index_conventions": conventions,
        "curves": curves,
    }


def bootstrap_valmer_curve_indexes(**runtime_kwargs: Any) -> dict[str, Any]:
    """Bootstrap the reference-index identities used by Valmer curve pricing."""

    return upsert_mexican_reference_indexes(**runtime_kwargs)


__all__ = [
    "BANCO_DE_MEXICO_PROVIDER",
    "MEXICO_MARKET",
    "MEXICAN_INDEX_CONVENTION_DEFINITIONS",
    "MEXICAN_MARKET_SOURCE",
    "MEXICAN_REFERENCE_INDEX_DEFINITIONS",
    "MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER",
    "VALMER_DISCOUNT_CURVES_CADENCE",
    "VALMER_CURVE_DEFINITIONS",
    "VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION",
    "VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER",
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
    "attach_valmer_curve_pricing_runtime",
    "bootstrap_valmer_curve_pricing",
    "bootstrap_valmer_curve_indexes",
    "configure_valmer_discount_curves_cadence",
    "create_valmer_curve_pricing_schemas",
    "mexican_reference_index_payloads",
    "upsert_interest_rate_index_type",
    "upsert_mexican_index_convention_details",
    "upsert_mexican_reference_indexes",
    "upsert_valmer_mxn_government_bond_curve",
    "upsert_valmer_tiie_curve",
]
