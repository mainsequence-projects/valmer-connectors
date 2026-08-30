from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

MEXICO_MARKET = "MX"
UNITED_STATES_MARKET = "US"
MEXICAN_MARKET_SOURCE = "mexico"
UNITED_STATES_MARKET_SOURCE = "united_states"
BANCO_DE_MEXICO_PROVIDER = "Banco de Mexico"
FEDERAL_RESERVE_BANK_OF_NEW_YORK_PROVIDER = "Federal Reserve Bank of New York"

TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER = "TIIE_OVERNIGHT"
TIIE_28_INDEX_UNIQUE_IDENTIFIER = "TIIE_28"
TIIE_91_INDEX_UNIQUE_IDENTIFIER = "TIIE_91"
TIIE_182_INDEX_UNIQUE_IDENTIFIER = "TIIE_182"
CETE_28_INDEX_UNIQUE_IDENTIFIER = "CETE_28"
CETE_91_INDEX_UNIQUE_IDENTIFIER = "CETE_91"
CETE_182_INDEX_UNIQUE_IDENTIFIER = "CETE_182"
USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER = "USD_SOFR_OVERNIGHT"

VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER = "VALMER_TIIE_OVERNIGHT"
VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER = "VALMER_MXN_GOVERNMENT_BOND"
VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER = "VALMER_USD_SOFR_OVERNIGHT"
VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER = (
    "VALMER_MXN_USD_COLLATERAL_DISCOUNT"
)
VALMER_SOURCE = "valmer"
VALMER_DISCOUNT_CURVES_CADENCE = "1d"
VALMER_CURVE_QUOTE_SIDE = "mid"
MEXICO_BMV_QUANTLIB_CALENDAR = {"name": "Mexico"}


def _json_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return dict(value)
    return value


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


@dataclass(frozen=True)
class UsdReferenceIndexDefinition:
    """Static USD reference-index seed data for core ms-markets Index rows."""

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
)


USD_REFERENCE_INDEX_DEFINITIONS: tuple[UsdReferenceIndexDefinition, ...] = (
    UsdReferenceIndexDefinition(
        unique_identifier=USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        display_name="USD SOFR overnight",
        description="US dollar Secured Overnight Financing Rate reference index.",
        index_family="SOFR",
        tenor_days=1,
        provider=FEDERAL_RESERVE_BANK_OF_NEW_YORK_PROVIDER,
        metadata_json={"market": UNITED_STATES_MARKET},
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
    fixing_calendar_code: Mapping[str, Any] | str = field(
        default_factory=lambda: dict(MEXICO_BMV_QUANTLIB_CALENDAR)
    )
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
            "fixing_calendar_code": _json_payload(self.fixing_calendar_code),
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


@dataclass(frozen=True)
class UsdIndexConventionDefinition:
    index_unique_identifier: str
    index_family: str
    tenor_days: int
    settlement_days: int
    business_day_convention: str
    day_counter_code: str = "Actual360"
    fixing_calendar_code: str = "UnitedStates"
    calendar_code: str | None = "UnitedStates"
    currency_code: str = "USD"
    end_of_month: bool = False
    source: str = UNITED_STATES_MARKET_SOURCE
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
            "fixings_unique_identifier": self.index_unique_identifier,
        }
        if self.calendar_code is not None:
            convention_dump["calendar_code"] = self.calendar_code
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
)


USD_INDEX_CONVENTION_DEFINITIONS: tuple[UsdIndexConventionDefinition, ...] = (
    UsdIndexConventionDefinition(
        index_unique_identifier=USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        index_family="SOFR",
        tenor_days=1,
        settlement_days=0,
        business_day_convention="ModifiedFollowing",
        metadata_json={"market": UNITED_STATES_MARKET},
    ),
)


@dataclass(frozen=True)
class ValmerCurveDefinition:
    unique_identifier: str
    display_name: str
    curve_type: str
    currency_code: str
    quote_side: str
    interpolation_method: str
    compounding: str
    source: str
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_curve_payload(self) -> dict[str, Any]:
        return {
            "unique_identifier": self.unique_identifier,
            "display_name": self.display_name,
            "curve_type": self.curve_type,
            "currency_code": self.currency_code,
            "quote_side": self.quote_side,
            "interpolation_method": self.interpolation_method,
            "compounding": self.compounding,
            "source": self.source,
            "metadata_json": dict(self.metadata_json) or None,
        }


@dataclass(frozen=True)
class ValmerCurveBuildingDetailsDefinition:
    curve_unique_identifier: str
    builder_type: str = "zero_rate_curve"
    quote_convention: str = "zero_rate"
    rate_unit: str = "decimal"
    day_counter_code: str = "Actual360"
    calendar_code: str = "Mexico"
    interpolation_method: str = "log_linear_discount"
    compounding: str = "compounded_annual"
    compounding_frequency: str | None = None
    extrapolation_policy: str = "enabled"
    bootstrap_method: str | None = None
    builder_payload: Mapping[str, Any] = field(default_factory=dict)
    source: str = VALMER_SOURCE
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_building_details_payload(self, *, curve_uid: Any) -> dict[str, Any]:
        return {
            "curve_uid": curve_uid,
            "builder_type": self.builder_type,
            "quote_convention": self.quote_convention,
            "rate_unit": self.rate_unit,
            "day_counter_code": self.day_counter_code,
            "calendar_code": self.calendar_code,
            "interpolation_method": self.interpolation_method,
            "compounding": self.compounding,
            "compounding_frequency": self.compounding_frequency,
            "extrapolation_policy": self.extrapolation_policy,
            "bootstrap_method": self.bootstrap_method,
            "builder_payload": dict(self.builder_payload) or None,
            "source": self.source,
            "metadata_json": dict(self.metadata_json) or None,
        }


@dataclass(frozen=True)
class ValmerIndexCurveBindingDefinition:
    role_key: str
    index_unique_identifier: str
    curve_unique_identifier: str
    quote_side: str = VALMER_CURVE_QUOTE_SIDE
    source: str = VALMER_SOURCE
    priority: int = 0
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_index_curve_selection_payload(
        self,
        *,
        market_data_set_uid: Any,
        index_uid: Any,
        curve_uid: Any,
    ) -> dict[str, Any]:
        return {
            "market_data_set_uid": market_data_set_uid,
            "role_key": self.role_key,
            "index_uid": index_uid,
            "quote_side": self.quote_side,
            "curve_uid": curve_uid,
            "source": self.source,
            "priority": self.priority,
            "metadata_json": dict(self.metadata_json) or None,
        }


@dataclass(frozen=True)
class ValmerCurveBindingDefinition:
    role_key: str
    selector_type: str
    selector_key: str
    curve_unique_identifier: str
    quote_side: str = VALMER_CURVE_QUOTE_SIDE
    source: str = VALMER_SOURCE
    priority: int = 0
    metadata_json: Mapping[str, Any] = field(default_factory=dict)

    def to_curve_binding_payload(
        self,
        *,
        market_data_set_uid: Any,
        curve_uid: Any,
    ) -> dict[str, Any]:
        return {
            "market_data_set_uid": market_data_set_uid,
            "role_key": self.role_key,
            "selector_type": self.selector_type,
            "selector_key": self.selector_key,
            "quote_side": self.quote_side,
            "curve_uid": curve_uid,
            "source": self.source,
            "priority": self.priority,
            "metadata_json": dict(self.metadata_json) or None,
        }


@dataclass(frozen=True)
class ValmerDeprecatedIndexCurveBindingDefinition:
    role_key: str
    index_unique_identifier: str
    curve_unique_identifier: str
    quote_side: str = VALMER_CURVE_QUOTE_SIDE
    source: str = VALMER_SOURCE

    def binding_key(self, *, index_uid: Any) -> str:
        return f"{self.role_key}:index:{index_uid}:{self.quote_side}"


@dataclass(frozen=True)
class ValmerDeprecatedCurveBindingDefinition:
    role_key: str
    selector_type: str
    selector_key: str
    curve_unique_identifier: str
    quote_side: str = VALMER_CURVE_QUOTE_SIDE
    source: str = VALMER_SOURCE

    @property
    def binding_key(self) -> str:
        return (
            f"{self.role_key}:{self.selector_type}:{self.selector_key}:"
            f"{self.quote_side}"
        )


VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION = ValmerCurveDefinition(
    unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    display_name="Valmer TIIE overnight OIS curve",
    curve_type="projection",
    currency_code="MXN",
    quote_side=VALMER_CURVE_QUOTE_SIDE,
    interpolation_method="log_linear_discount",
    compounding="compounded_annual",
    source=VALMER_SOURCE,
    metadata_json={
        "market": MEXICO_MARKET,
        "source_file": "IRS_MXN_CURVE.csv",
        "source_url": "https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_MXN_CURVE.csv",
        "date_source_url": "https://www.valmer.com.mx/en/",
        "date_ajax_url": "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do",
        "included_source_family": "Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO",
        "excluded_source_families": [
            "FX.USD.MXN",
            "Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR",
        ],
    },
)

VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION = ValmerCurveDefinition(
    unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    display_name="Valmer MXN government bond discount curve",
    curve_type="discount",
    currency_code="MXN",
    quote_side=VALMER_CURVE_QUOTE_SIDE,
    interpolation_method="log_linear_discount",
    compounding="compounded_annual",
    source=VALMER_SOURCE,
    metadata_json={
        "market": MEXICO_MARKET,
        "source_file": "Vector Analitico",
        "instrument_families": ["BI_CETES", "M_BONOS"],
    },
)

VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION = ValmerCurveDefinition(
    unique_identifier=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    display_name="Valmer USD SOFR overnight OIS curve",
    curve_type="projection",
    currency_code="USD",
    quote_side=VALMER_CURVE_QUOTE_SIDE,
    interpolation_method="log_linear_discount",
    compounding="compounded_annual",
    source=VALMER_SOURCE,
    metadata_json={
        "market": UNITED_STATES_MARKET,
        "source_file": "IRS_USD_CURVE.csv",
        "source_url": "https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_USD_CURVE.csv",
        "date_source_url": "https://www.valmer.com.mx/en/",
        "date_ajax_url": "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do",
        "included_source_families": [
            "Future.USD.CME.CME SR1 EOM.<MMM>.<YY>",
            "Future.USD.CME.CME SR3 IMM.<MMM>.<YY>",
            "Swap.<tenor>.USD.SOFR.1D/1Y.SOFR",
        ],
        "excluded_source_families": [
            "Swap.<tenor>.USD.FEDFUNDS.1D/1Y.FEDFUNDS1",
            "Swap.USD.<tenor>.FEDFUNDS.1D/SOFR.1D.SOFR",
        ],
    },
)

VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_DEFINITION = ValmerCurveDefinition(
    unique_identifier=VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    display_name="Valmer MXN USD-collateralized discount curve",
    curve_type="discount",
    currency_code="MXN",
    quote_side=VALMER_CURVE_QUOTE_SIDE,
    interpolation_method="log_linear_discount",
    compounding="compounded_annual",
    source=VALMER_SOURCE,
    metadata_json={
        "market": MEXICO_MARKET,
        "source_file": "IRS_MXN_CURVE.csv",
        "source_url": "https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_MXN_CURVE.csv",
        "date_source_url": "https://www.valmer.com.mx/en/",
        "date_ajax_url": "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do",
        "fx_pair": "USD/MXN",
        "fx_quote_meaning": "MXN per 1 USD",
        "included_source_families": [
            "FX.USD.MXN.<tenor>",
            "Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR",
        ],
        "dependency_curves": [
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
            VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        ],
    },
)

VALMER_CURVE_DEFINITIONS: tuple[ValmerCurveDefinition, ...] = (
    VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION,
    VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_DEFINITION,
)

VALMER_CURVE_BUILDING_DETAILS_DEFINITIONS: tuple[
    ValmerCurveBuildingDetailsDefinition,
    ...,
] = (
    ValmerCurveBuildingDetailsDefinition(
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        builder_type="rate_helper_curve",
        quote_convention="helper_quote",
        rate_unit="helper_unit",
        bootstrap_method="piecewise_log_linear_discount",
        builder_payload={
            "key_node_schema": "CurveKeyNode",
            "helper_schema": "rate_helpers@v1",
            "source_file": "IRS_MXN_CURVE.csv",
            "source_row_pattern": "Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO",
            "date_source": "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do:Indices_Benchmarks",
            "output_quote_convention": "zero_rate",
            "output_rate_unit": "decimal",
            "output_quote_type": "zero_rate",
            "output_quote_unit": "decimal",
            "implied_front_zero_days": [1],
            "valmer_extensions": [
                "helper_type",
                "quote_source",
                "source_quote",
                "source_quote_unit",
                "tenor",
                "floating_index",
                "settlement_days",
                "payment_convention",
                "payment_frequency",
                "payment_calendar_code",
                "averaging_method",
                "end_of_month",
                "fixed_payment_frequency",
                "fixed_calendar_code",
                "day_counter",
                "day_counter_code",
                "earliest_date",
                "pillar_date",
            ],
            "instrument_rules": {
                "FTIIE_OIS": {
                    "instrument_type": "overnight_indexed_swap",
                    "helper_type": "ois_rate_helper",
                    "quote_type": "par_swap_rate",
                    "quote_unit": "decimal",
                    "source_quote_unit": "percent",
                    "floating_index": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
                    "settlement_days": 1,
                    "payment_convention": "ModifiedFollowing",
                    "payment_frequency": "EveryFourthWeek",
                    "payment_calendar_code": {"name": "Mexico"},
                    "averaging_method": "Compound",
                    "end_of_month": False,
                    "fixed_payment_frequency": "EveryFourthWeek",
                    "fixed_calendar_code": {"name": "Mexico"},
                    "day_counter_code": "Actual360",
                }
            },
        },
        metadata_json={"market": MEXICO_MARKET},
    ),
    ValmerCurveBuildingDetailsDefinition(
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
        builder_type="rate_helper_curve",
        quote_convention="helper_quote",
        rate_unit="helper_unit",
        bootstrap_method="piecewise_log_linear_discount",
        builder_payload={
            "key_node_schema": "CurveKeyNode",
            "helper_schema": "rate_helpers@v1",
            "output_quote_convention": "zero_rate",
            "output_rate_unit": "decimal",
            "output_quote_type": "zero_rate",
            "output_quote_unit": "decimal",
            "valmer_extensions": [
                "helper_type",
                "quote_source",
                "source_quote_type",
                "yield_type",
                "yield_unit",
                "yield_source",
                "dirty_price",
                "dirty_price_source",
                "accrued_interest",
                "coupon_rate",
                "coupon_period_days",
                "face_value",
                "day_counter",
            ],
            "instrument_rules": {
                "CETES": {
                    "instrument_type": "zero_coupon_bond",
                    "helper_type": "zero_coupon_bond_helper",
                    "quote_type": "clean_price",
                    "quote_unit": "price_per_face",
                    "source_quote_type": "dirty_price",
                    "yield_type": "yield_to_maturity",
                    "yield_unit": "decimal",
                },
                "M_BONOS": {
                    "instrument_type": "fixed_rate_bond",
                    "helper_type": "fixed_rate_bond_helper",
                    "quote_type": "clean_price",
                    "quote_unit": "price_per_100",
                    "source_quote_type": "clean_price",
                    "yield_type": "yield_to_maturity",
                    "yield_unit": "decimal",
                    "coupon_period_days": 182,
                },
            },
        },
        metadata_json={"market": MEXICO_MARKET},
    ),
    ValmerCurveBuildingDetailsDefinition(
        curve_unique_identifier=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        builder_type="rate_helper_curve",
        quote_convention="helper_quote",
        rate_unit="helper_unit",
        day_counter_code="Actual360",
        calendar_code="UnitedStates",
        bootstrap_method="piecewise_log_linear_discount",
        builder_payload={
            "key_node_schema": "CurveKeyNode",
            "helper_schema": "rate_helpers@v1",
            "source_file": "IRS_USD_CURVE.csv",
            "source_row_patterns": [
                "Future.USD.CME.CME SR1 EOM.<MMM>.<YY>",
                "Future.USD.CME.CME SR3 IMM.<MMM>.<YY>",
                "Swap.<tenor>.USD.SOFR.1D/1Y.SOFR",
            ],
            "excluded_source_row_patterns": [
                "Swap.<tenor>.USD.FEDFUNDS.1D/1Y.FEDFUNDS1",
                "Swap.USD.<tenor>.FEDFUNDS.1D/SOFR.1D.SOFR",
            ],
            "date_source": "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do:Indices_Benchmarks",
            "output_quote_convention": "zero_rate",
            "output_rate_unit": "decimal",
            "output_quote_type": "zero_rate",
            "output_quote_unit": "decimal",
            "implied_front_zero_days": [1],
            "active_future_policy": "exclude_futures_before_valuation_date",
            "valmer_extensions": [
                "helper_type",
                "quote_source",
                "source_quote",
                "source_quote_unit",
                "implied_rate",
                "implied_rate_unit",
                "contract_code",
                "reference_month",
                "reference_year",
                "reference_frequency",
                "future_family",
                "convexity_adjustment",
                "tenor",
                "floating_index",
                "settlement_days",
                "payment_convention",
                "payment_frequency",
                "payment_calendar_code",
                "averaging_method",
                "end_of_month",
                "fixed_payment_frequency",
                "fixed_calendar_code",
                "day_counter",
                "day_counter_code",
                "earliest_date",
                "pillar_date",
            ],
            "instrument_rules": {
                "SOFR_FUTURE": {
                    "instrument_type": "sofr_future",
                    "helper_type": "sofr_future_rate_helper",
                    "quote_type": "futures_price",
                    "quote_unit": "price",
                    "implied_rate_unit": "decimal",
                    "floating_index": USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
                    "future_family": "sofr",
                    "convexity_adjustment": 0.0,
                },
                "SOFR_OIS": {
                    "instrument_type": "overnight_indexed_swap",
                    "helper_type": "ois_rate_helper",
                    "quote_type": "par_swap_rate",
                    "quote_unit": "decimal",
                    "source_quote_unit": "percent",
                    "floating_index": USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
                    "settlement_days": 2,
                    "payment_convention": "ModifiedFollowing",
                    "payment_frequency": "Annual",
                    "payment_calendar_code": {"name": "UnitedStates", "market": 6},
                    "averaging_method": "Compound",
                    "end_of_month": False,
                    "fixed_payment_frequency": "Annual",
                    "fixed_calendar_code": {"name": "UnitedStates", "market": 6},
                    "day_counter_code": "Actual360",
                },
            },
        },
        metadata_json={"market": UNITED_STATES_MARKET},
    ),
    ValmerCurveBuildingDetailsDefinition(
        curve_unique_identifier=VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
        builder_type="rate_helper_curve",
        quote_convention="helper_quote",
        rate_unit="helper_unit",
        day_counter_code="Actual360",
        calendar_code="Mexico",
        bootstrap_method="piecewise_log_linear_discount",
        builder_payload={
            "key_node_schema": "CurveKeyNode",
            "helper_schema": "rate_helpers@v1",
            "source_file": "IRS_MXN_CURVE.csv",
            "source_row_patterns": [
                "FX.USD.MXN.<tenor>",
                "Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR",
            ],
            "date_source": "https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do:Indices_Benchmarks",
            "output_quote_convention": "zero_rate",
            "output_rate_unit": "decimal",
            "output_quote_type": "zero_rate",
            "output_quote_unit": "decimal",
            "dependency_curves": [
                VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
                VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
            ],
            "excluded_fx_tenors": ["ON", "TN"],
            "tenor_normalization": {"182M": "15Y", "364M": "30Y"},
            "instrument_rules": {
                "FX_SWAP": {
                    "instrument_type": "fx_swap",
                    "helper_type": "fx_swap_rate_helper",
                    "fx_pair": "USD/MXN",
                    "fx_base_currency": "USD",
                    "fx_quote_currency": "MXN",
                    "quote_type": "fx_forward_points",
                    "quote_unit": "mxn_per_usd",
                    "source_quote_unit": "raw_points",
                    "point_scale": 10000,
                    "fixing_days": 2,
                    "calendar_code": {
                        "name": "JointCalendar",
                        "calendars": [
                            {"name": "Mexico"},
                            {"name": "UnitedStates", "market": 0},
                        ],
                    },
                    "business_day_convention": "ModifiedFollowing",
                    "end_of_month": False,
                    "is_fx_base_currency_collateral_currency": True,
                    "collateral_curve": VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
                },
                "CONSTANT_NOTIONAL_CCS": {
                    "instrument_type": "cross_currency_basis_swap",
                    "helper_type": "const_notional_cross_currency_basis_swap_rate_helper",
                    "quote_type": "basis_spread",
                    "quote_unit": "decimal",
                    "source_quote_unit": "percent",
                    "basis_side": "USD_SOFR",
                    "basis_sign": "positive_quote_means_sofr_plus_spread",
                    "notional_style": "constant_notional",
                    "base_currency_index": USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
                    "quote_currency_index": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
                    "collateral_curve": VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
                    "fixing_days": 0,
                    "business_day_convention": "ModifiedFollowing",
                    "end_of_month": False,
                    "is_fx_base_currency_collateral_currency": True,
                    "is_basis_on_fx_base_currency_leg": True,
                    "payment_frequency": "EveryFourthWeek",
                    "payment_lag": 0,
                },
            },
        },
        metadata_json={"market": MEXICO_MARKET},
    ),
)

VALMER_INDEX_CURVE_BINDING_DEFINITIONS: tuple[
    ValmerIndexCurveBindingDefinition,
    ...,
] = (
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=TIIE_182_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=TIIE_182_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=TIIE_28_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=TIIE_91_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=TIIE_182_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=CETE_28_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=CETE_91_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=CETE_182_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=CETE_28_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=CETE_91_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=CETE_182_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=CETE_28_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=CETE_91_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="z_spread_base",
        index_unique_identifier=CETE_182_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
    ValmerIndexCurveBindingDefinition(
        role_key="projection",
        index_unique_identifier=USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
)

VALMER_CURVE_BINDING_DEFINITIONS: tuple[ValmerCurveBindingDefinition, ...] = ()

VALMER_DEPRECATED_INDEX_CURVE_BINDING_DEFINITIONS: tuple[
    ValmerDeprecatedIndexCurveBindingDefinition,
    ...,
] = (
    ValmerDeprecatedIndexCurveBindingDefinition(
        role_key="discount",
        index_unique_identifier=USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        curve_unique_identifier=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
)

VALMER_DEPRECATED_CURVE_BINDING_DEFINITIONS: tuple[
    ValmerDeprecatedCurveBindingDefinition,
    ...,
] = (
    ValmerDeprecatedCurveBindingDefinition(
        role_key="discount",
        selector_type="currency",
        selector_key="MXN",
        curve_unique_identifier=VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    ),
)


def mexican_reference_index_payloads(
    definitions: Sequence[MexicanReferenceIndexDefinition] = MEXICAN_REFERENCE_INDEX_DEFINITIONS,
) -> tuple[dict[str, Any], ...]:
    return tuple(definition.to_index_payload() for definition in definitions)


def usd_reference_index_payloads(
    definitions: Sequence[UsdReferenceIndexDefinition] = USD_REFERENCE_INDEX_DEFINITIONS,
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
    from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
    from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage
    from msm_pricing.data_nodes.pricing_details.storage import AssetPricingDetailsStorage
    from msm_pricing.models.curve_building_details import CurveBuildingDetailsTable
    from msm_pricing.models.curves import CurveTable
    from msm_pricing.models.index_convention_details import IndexConventionDetailsTable
    from msm_pricing.models.market_data_bindings import (
        PricingMarketDataSetBindingTable,
        PricingMarketDataSetCurveBindingTable,
        PricingMarketDataSetTable,
    )
    from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

    return [
        AssetTable,
        IndexTypeTable,
        IndexTable,
        IndexConventionDetailsTable,
        CurveTable,
        CurveBuildingDetailsTable,
        AssetCurrentPricingDetailsTable,
        PricingMarketDataSetTable,
        PricingMarketDataSetBindingTable,
        PricingMarketDataSetCurveBindingTable,
        DiscountCurvesStorage,
        IndexFixingsStorage,
        AssetPricingDetailsStorage,
    ]


def configure_valmer_discount_curves_cadence(
    cadence: str = VALMER_DISCOUNT_CURVES_CADENCE,
) -> type[Any]:
    """Set the imported core discount-curve storage cadence for Valmer daily curves."""

    from msm_pricing.data_nodes import DiscountCurvesNode

    output_table = DiscountCurvesNode._required_output_table()
    output_table.__cadence__ = cadence
    return output_table


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


def upsert_usd_reference_indexes(
    definitions: Sequence[UsdReferenceIndexDefinition] = USD_REFERENCE_INDEX_DEFINITIONS,
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    """Upsert the core Index rows required by USD SOFR curve and fixing code."""

    from msm.api.indices import Index

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(**runtime_kwargs)

    upsert_interest_rate_index_type()
    upserted = {}
    for payload in usd_reference_index_payloads(definitions):
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


def upsert_usd_index_convention_details(
    definitions: Sequence[UsdIndexConventionDefinition] = (
        USD_INDEX_CONVENTION_DEFINITIONS
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
    resolved_indexes = indexes or upsert_usd_reference_indexes(
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
    definition: ValmerCurveDefinition = VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION,
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(**runtime_kwargs)
    return Curve.upsert(definition.to_curve_payload())


def upsert_valmer_usd_sofr_curve(
    definition: ValmerCurveDefinition = VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION,
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(**runtime_kwargs)
    return Curve.upsert(definition.to_curve_payload())


def upsert_valmer_mxn_government_bond_curve(
    definition: ValmerCurveDefinition = VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION,
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(**runtime_kwargs)
    return Curve.upsert(definition.to_curve_payload())


def upsert_valmer_usd_mxn_xccy_curve(
    definition: ValmerCurveDefinition = VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_DEFINITION,
    *,
    attach_runtime: bool = True,
    create_schemas: bool | None = None,
    **runtime_kwargs: Any,
) -> Any:
    from msm_pricing.api import Curve

    if create_schemas is not None:
        attach_runtime = create_schemas
    if attach_runtime:
        attach_valmer_curve_pricing_runtime(**runtime_kwargs)
    return Curve.upsert(definition.to_curve_payload())


def upsert_valmer_curve_building_details(
    definitions: Sequence[ValmerCurveBuildingDetailsDefinition] = (
        VALMER_CURVE_BUILDING_DETAILS_DEFINITIONS
    ),
    *,
    curves: Mapping[str, Any],
) -> dict[str, Any]:
    from msm_pricing.api import CurveBuildingDetails

    upserted = {}
    for definition in definitions:
        curve = curves[definition.curve_unique_identifier]
        detail = CurveBuildingDetails.upsert(
            definition.to_building_details_payload(curve_uid=curve.uid)
        )
        upserted[definition.curve_unique_identifier] = detail
    return upserted


def upsert_valmer_market_data_source_bindings() -> dict[str, Any]:
    from msm_pricing.api import PricingMarketDataSet, PricingMarketDataSetBinding
    from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
    from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage
    from msm_pricing.settings import (
        PRICING_CONCEPT_DISCOUNT_CURVES,
        PRICING_CONCEPT_INTEREST_RATE_INDEX_FIXINGS,
        PRICING_MARKET_DATA_SET_DEFAULT,
    )

    market_data_set = PricingMarketDataSet.upsert(
        {
            "set_key": PRICING_MARKET_DATA_SET_DEFAULT,
            "display_name": "Default pricing market data",
            "description": "Default Valmer pricing market-data set.",
            "metadata_json": {"source": VALMER_SOURCE},
        }
    )
    discount_binding = PricingMarketDataSetBinding.upsert(
        {
            "market_data_set_uid": market_data_set.uid,
            "concept_key": PRICING_CONCEPT_DISCOUNT_CURVES,
            "data_node_uid": DiscountCurvesStorage.get_meta_table_uid(),
            "storage_table_identifier": DiscountCurvesStorage.get_identifier(),
            "source": VALMER_SOURCE,
        }
    )
    fixing_binding = PricingMarketDataSetBinding.upsert(
        {
            "market_data_set_uid": market_data_set.uid,
            "concept_key": PRICING_CONCEPT_INTEREST_RATE_INDEX_FIXINGS,
            "data_node_uid": IndexFixingsStorage.get_meta_table_uid(),
            "storage_table_identifier": IndexFixingsStorage.get_identifier(),
            "source": VALMER_SOURCE,
        }
    )
    return {
        "market_data_set": market_data_set,
        "discount_curves": discount_binding,
        "interest_rate_index_fixings": fixing_binding,
    }


def upsert_valmer_curve_bindings(
    definitions: Sequence[ValmerIndexCurveBindingDefinition] = (
        VALMER_INDEX_CURVE_BINDING_DEFINITIONS
    ),
    *,
    indexes: Mapping[str, Any],
    curves: Mapping[str, Any],
    market_data_set: Any,
) -> dict[tuple[str, str, str], Any]:
    from msm_pricing.api import PricingMarketDataSetCurveBinding

    upserted = {}
    for definition in definitions:
        index = indexes[definition.index_unique_identifier]
        curve = curves[definition.curve_unique_identifier]
        selection = PricingMarketDataSetCurveBinding.upsert_index_curve_selection(
            definition.to_index_curve_selection_payload(
                market_data_set_uid=market_data_set.uid,
                index_uid=index.uid,
                curve_uid=curve.uid,
            )
        )
        upserted[
            (
                definition.role_key,
                definition.index_unique_identifier,
                definition.quote_side,
            )
        ] = selection
    return upserted


def upsert_valmer_generic_curve_bindings(
    definitions: Sequence[ValmerCurveBindingDefinition] = (
        VALMER_CURVE_BINDING_DEFINITIONS
    ),
    *,
    curves: Mapping[str, Any],
    market_data_set: Any,
) -> dict[tuple[str, str, str, str], Any]:
    from msm_pricing.api import PricingMarketDataSetCurveBinding

    upserted = {}
    for definition in definitions:
        curve = curves[definition.curve_unique_identifier]
        binding = PricingMarketDataSetCurveBinding.upsert(
            definition.to_curve_binding_payload(
                market_data_set_uid=market_data_set.uid,
                curve_uid=curve.uid,
            )
        )
        upserted[
            (
                definition.role_key,
                definition.selector_type,
                definition.selector_key,
                definition.quote_side,
            )
        ] = binding
    return upserted


def delete_valmer_deprecated_curve_bindings(
    *,
    indexes: Mapping[str, Any],
    curves: Mapping[str, Any],
    market_data_set: Any,
    index_definitions: Sequence[ValmerDeprecatedIndexCurveBindingDefinition] = (
        VALMER_DEPRECATED_INDEX_CURVE_BINDING_DEFINITIONS
    ),
    generic_definitions: Sequence[ValmerDeprecatedCurveBindingDefinition] = (
        VALMER_DEPRECATED_CURVE_BINDING_DEFINITIONS
    ),
) -> dict[tuple[str, str, str, str], Any]:
    """Remove Valmer-owned curve bindings from superseded seed policy."""

    from msm_pricing.api import PricingMarketDataSetCurveBinding

    deleted = {}
    for definition in index_definitions:
        index = indexes[definition.index_unique_identifier]
        curve = curves[definition.curve_unique_identifier]
        binding = PricingMarketDataSetCurveBinding.get_by_set_and_binding_key(
            market_data_set_uid=market_data_set.uid,
            binding_key=definition.binding_key(index_uid=index.uid),
        )
        if (
            binding is not None
            and str(binding.curve_uid) == str(curve.uid)
            and binding.source == definition.source
        ):
            deleted[
                (
                    definition.role_key,
                    "index",
                    definition.index_unique_identifier,
                    definition.quote_side,
                )
            ] = PricingMarketDataSetCurveBinding.delete(binding.uid)

    for definition in generic_definitions:
        curve = curves[definition.curve_unique_identifier]
        binding = PricingMarketDataSetCurveBinding.get_by_set_and_binding_key(
            market_data_set_uid=market_data_set.uid,
            binding_key=definition.binding_key,
        )
        if (
            binding is not None
            and str(binding.curve_uid) == str(curve.uid)
            and binding.source == definition.source
        ):
            deleted[
                (
                    definition.role_key,
                    definition.selector_type,
                    definition.selector_key,
                    definition.quote_side,
                )
            ] = PricingMarketDataSetCurveBinding.delete(binding.uid)

    return deleted


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
    indexes.update(upsert_usd_reference_indexes(attach_runtime=False))
    conventions = upsert_mexican_index_convention_details(
        indexes=indexes,
        attach_runtime=False,
    )
    conventions.update(
        upsert_usd_index_convention_details(
            indexes=indexes,
            attach_runtime=False,
        )
    )
    curves = {
        curve.unique_identifier: curve
        for curve in (
            upsert_valmer_tiie_curve(attach_runtime=False),
            upsert_valmer_mxn_government_bond_curve(attach_runtime=False),
            upsert_valmer_usd_sofr_curve(attach_runtime=False),
            upsert_valmer_usd_mxn_xccy_curve(attach_runtime=False),
        )
    }
    curve_building_details = upsert_valmer_curve_building_details(curves=curves)
    market_data_bindings = upsert_valmer_market_data_source_bindings()
    curve_bindings = upsert_valmer_curve_bindings(
        indexes=indexes,
        curves=curves,
        market_data_set=market_data_bindings["market_data_set"],
    )
    generic_curve_bindings = upsert_valmer_generic_curve_bindings(
        curves=curves,
        market_data_set=market_data_bindings["market_data_set"],
    )
    curve_bindings.update(generic_curve_bindings)
    deprecated_curve_bindings = delete_valmer_deprecated_curve_bindings(
        indexes=indexes,
        curves=curves,
        market_data_set=market_data_bindings["market_data_set"],
    )
    return {
        "index_type": index_type,
        "indexes": indexes,
        "index_conventions": conventions,
        "curves": curves,
        "curve_building_details": curve_building_details,
        "market_data_bindings": market_data_bindings,
        "curve_bindings": curve_bindings,
        "deprecated_curve_bindings": deprecated_curve_bindings,
    }


def bootstrap_valmer_curve_indexes(**runtime_kwargs: Any) -> dict[str, Any]:
    """Bootstrap the reference-index identities used by Valmer curve pricing."""

    indexes = upsert_mexican_reference_indexes(**runtime_kwargs)
    indexes.update(upsert_usd_reference_indexes(attach_runtime=False))
    return indexes


__all__ = [
    "BANCO_DE_MEXICO_PROVIDER",
    "FEDERAL_RESERVE_BANK_OF_NEW_YORK_PROVIDER",
    "MEXICO_MARKET",
    "MEXICAN_INDEX_CONVENTION_DEFINITIONS",
    "MEXICAN_MARKET_SOURCE",
    "MEXICAN_REFERENCE_INDEX_DEFINITIONS",
    "UNITED_STATES_MARKET",
    "UNITED_STATES_MARKET_SOURCE",
    "USD_INDEX_CONVENTION_DEFINITIONS",
    "USD_REFERENCE_INDEX_DEFINITIONS",
    "USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER",
    "VALMER_CURVE_BUILDING_DETAILS_DEFINITIONS",
    "VALMER_CURVE_BINDING_DEFINITIONS",
    "VALMER_CURVE_QUOTE_SIDE",
    "VALMER_DEPRECATED_CURVE_BINDING_DEFINITIONS",
    "VALMER_DEPRECATED_INDEX_CURVE_BINDING_DEFINITIONS",
    "VALMER_DISCOUNT_CURVES_CADENCE",
    "VALMER_CURVE_DEFINITIONS",
    "VALMER_INDEX_CURVE_BINDING_DEFINITIONS",
    "VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION",
    "VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER",
    "VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_DEFINITION",
    "VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER",
    "VALMER_SOURCE",
    "VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION",
    "VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER",
    "VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION",
    "VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER",
    "CETE_182_INDEX_UNIQUE_IDENTIFIER",
    "CETE_28_INDEX_UNIQUE_IDENTIFIER",
    "CETE_91_INDEX_UNIQUE_IDENTIFIER",
    "MexicanReferenceIndexDefinition",
    "MexicanIndexConventionDefinition",
    "TIIE_182_INDEX_UNIQUE_IDENTIFIER",
    "TIIE_28_INDEX_UNIQUE_IDENTIFIER",
    "TIIE_91_INDEX_UNIQUE_IDENTIFIER",
    "TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER",
    "ValmerCurveBuildingDetailsDefinition",
    "ValmerCurveBindingDefinition",
    "ValmerCurveDefinition",
    "ValmerDeprecatedCurveBindingDefinition",
    "ValmerDeprecatedIndexCurveBindingDefinition",
    "ValmerIndexCurveBindingDefinition",
    "UsdReferenceIndexDefinition",
    "UsdIndexConventionDefinition",
    "attach_valmer_curve_pricing_runtime",
    "bootstrap_valmer_curve_pricing",
    "bootstrap_valmer_curve_indexes",
    "configure_valmer_discount_curves_cadence",
    "create_valmer_curve_pricing_schemas",
    "delete_valmer_deprecated_curve_bindings",
    "mexican_reference_index_payloads",
    "usd_reference_index_payloads",
    "upsert_interest_rate_index_type",
    "upsert_mexican_index_convention_details",
    "upsert_mexican_reference_indexes",
    "upsert_usd_index_convention_details",
    "upsert_usd_reference_indexes",
    "upsert_valmer_curve_bindings",
    "upsert_valmer_curve_building_details",
    "upsert_valmer_generic_curve_bindings",
    "upsert_valmer_market_data_source_bindings",
    "upsert_valmer_mxn_government_bond_curve",
    "upsert_valmer_tiie_curve",
    "upsert_valmer_usd_mxn_xccy_curve",
    "upsert_valmer_usd_sofr_curve",
]
