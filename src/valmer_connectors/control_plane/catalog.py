from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from msm.models import AssetTable
from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

from valmer_connectors.data_nodes.canonical_index_values import DailyIndexValuesStorage
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable


@dataclass(frozen=True)
class DataProductDefinition:
    key: str
    name: str
    description: str
    table_identifier: str
    category: str
    stale_after_hours: int
    identity_column: str | None = None
    identity_value: str | None = None
    time_indexed: bool = True


@dataclass(frozen=True)
class JobParameterDefinition:
    key: str
    input_type: Literal["boolean", "date", "text"]
    label: str
    description: str
    command_flag: str
    required: bool = False
    default: bool | str | None = None
    false_command_flag: str | None = None


@dataclass(frozen=True)
class JobActionDefinition:
    key: str
    job_name: str
    description: str
    execution_path: str
    dependencies: tuple[str, ...] = ()
    parameters: tuple[JobParameterDefinition, ...] = ()


FORCE_PRICING_DETAILS_PARAMETER = JobParameterDefinition(
    key="force_pricing_details_patch",
    input_type="boolean",
    label="Refresh pricing details",
    description=(
        "Rehydrate current pricing details for every selected Valmer pricing target."
    ),
    command_flag="--force-pricing-details-patch",
    false_command_flag="--no-force-pricing-details-patch",
    default=True,
)

BYPASS_VECTOR_CURSOR_PARAMETER = JobParameterDefinition(
    key="bypass_vector_cursor_filter",
    input_type="boolean",
    label="Reprocess current observations",
    description=(
        "Keep source observations even when vector storage already contains an equal "
        "or newer observation."
    ),
    command_flag="--bypass-vector-cursor-filter",
    false_command_flag="--no-bypass-vector-cursor-filter",
    default=False,
)

END_DATE_PARAMETER = JobParameterDefinition(
    key="end_date",
    input_type="date",
    label="Inclusive end date",
    description=(
        "Optional final provider date in YYYY-MM-DD form. Leave empty to use the "
        "normal production cutoff."
    ),
    command_flag="--end-date",
)

VECTOR_PARAMETERS = (
    FORCE_PRICING_DETAILS_PARAMETER,
    BYPASS_VECTOR_CURSOR_PARAMETER,
)


DISCOUNT_CURVE_TABLE_IDENTIFIER = DiscountCurvesStorage.__table__.name
INDEX_FIXINGS_TABLE_IDENTIFIER = IndexFixingsStorage.__table__.name
INDEX_VALUES_TABLE_IDENTIFIER = DailyIndexValuesStorage.__table__.name
CANONICAL_ASSET_TABLE_IDENTIFIER = AssetTable.__table__.name
CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER = (
    AssetCurrentPricingDetailsTable.__table__.name
)
VALMER_ASSET_DETAILS_TABLE_IDENTIFIER = ValmerAssetDetailsTable.__table__.name
VECTOR_TABLE_IDENTIFIER = ValmerVectorPricesStorage.__table__.name

VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER = "VALMER_TIIE_OVERNIGHT"
VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER = "VALMER_MXN_GOVERNMENT_BOND"
VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER = "VALMER_USD_SOFR_OVERNIGHT"
VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER = (
    "VALMER_MXN_USD_COLLATERAL_DISCOUNT"
)

DATA_PRODUCTS: tuple[DataProductDefinition, ...] = (
    DataProductDefinition(
        key="valmer-assets",
        name="Registered Valmer assets",
        description=(
            "Valmer asset-detail rows linked one-to-one to canonical Asset identities."
        ),
        table_identifier=VALMER_ASSET_DETAILS_TABLE_IDENTIFIER,
        category="registry",
        stale_after_hours=0,
        identity_column="asset_uid",
        time_indexed=False,
    ),
    DataProductDefinition(
        key="pricing-details",
        name="Current pricing details",
        description="Current rows in the persisted Asset pricing-details relation.",
        table_identifier=CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER,
        category="registry",
        stale_after_hours=0,
        identity_column="asset_uid",
        time_indexed=False,
    ),
    DataProductDefinition(
        key="valmer-vector",
        name="Valmer vector",
        description="Latest daily Valmer asset price, yield, spread, rating and risk observations.",
        table_identifier=VECTOR_TABLE_IDENTIFIER,
        category="source",
        stale_after_hours=36,
        identity_column="asset_identifier",
    ),
    DataProductDefinition(
        key="index-values",
        name="Reference rates and curve quotes",
        description="Canonical daily Index observations from FRED, Banxico and Valmer quotes.",
        table_identifier=INDEX_VALUES_TABLE_IDENTIFIER,
        category="market-data",
        stale_after_hours=48,
        identity_column="index_identifier",
    ),
    DataProductDefinition(
        key="index-fixings",
        name="Index fixings",
        description="Daily persisted interest-rate fixing observations used by pricing workflows.",
        table_identifier=INDEX_FIXINGS_TABLE_IDENTIFIER,
        category="market-data",
        stale_after_hours=48,
        identity_column="index_identifier",
    ),
    DataProductDefinition(
        key="curve-tiie",
        name="TIIE discount curve",
        description="MXN TIIE overnight discount curve built from persisted quotes and fixings.",
        table_identifier=DISCOUNT_CURVE_TABLE_IDENTIFIER,
        category="curve",
        stale_after_hours=48,
        identity_column="curve_identifier",
        identity_value=VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    DataProductDefinition(
        key="curve-sofr",
        name="SOFR discount curve",
        description="USD SOFR overnight discount curve built from persisted Valmer quotes.",
        table_identifier=DISCOUNT_CURVE_TABLE_IDENTIFIER,
        category="curve",
        stale_after_hours=48,
        identity_column="curve_identifier",
        identity_value=VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    ),
    DataProductDefinition(
        key="curve-xccy",
        name="USD/MXN collateral curve",
        description="Cross-currency curve dependent on the current TIIE and SOFR curves.",
        table_identifier=DISCOUNT_CURVE_TABLE_IDENTIFIER,
        category="curve",
        stale_after_hours=48,
        identity_column="curve_identifier",
        identity_value=VALMER_MXN_USD_COLLATERAL_DISCOUNT_CURVE_UNIQUE_IDENTIFIER,
    ),
    DataProductDefinition(
        key="curve-government",
        name="MXN government curve",
        description="Government bond discount curve built from persisted CETES and M Bonos vectors.",
        table_identifier=DISCOUNT_CURVE_TABLE_IDENTIFIER,
        category="curve",
        stale_after_hours=48,
        identity_column="curve_identifier",
        identity_value=VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    ),
)


JOB_ACTIONS: tuple[JobActionDefinition, ...] = (
    JobActionDefinition(
        key="vector-refresh",
        job_name="Valmer Vector Refresh",
        description="Import the configured Main Sequence Artifact source and publish vector observations.",
        execution_path="scripts/update_vector_valmer.py",
        parameters=VECTOR_PARAMETERS,
    ),
    JobActionDefinition(
        key="vector-onedrive-refresh",
        job_name="Valmer Vector Refresh — OneDrive Graph",
        description="Import Valmer files from the configured Microsoft Graph drive.",
        execution_path="scripts/update_vector_valmer_onedrive.py",
        parameters=VECTOR_PARAMETERS,
    ),
    JobActionDefinition(
        key="vector-metatable-refresh",
        job_name="Valmer Vector Refresh — MetaTable",
        description="Import Valmer rows from the repository-declared MetaTable source.",
        execution_path="scripts/update_vector_valmer_metatable.py",
        parameters=VECTOR_PARAMETERS,
    ),
    JobActionDefinition(
        key="irs-mxn-quotes-refresh",
        job_name="Valmer IRS MXN Quotes Refresh",
        description="Publish the current IRS MXN quote snapshot.",
        execution_path="scripts/update_valmer_irs_mxn_quotes.py",
    ),
    JobActionDefinition(
        key="irs-usd-quotes-refresh",
        job_name="Valmer IRS USD Quotes Refresh",
        description="Publish the current IRS USD and SOFR quote snapshot.",
        execution_path="scripts/update_valmer_irs_usd_quotes.py",
    ),
    JobActionDefinition(
        key="banxico-fixings-refresh",
        job_name="Banxico Fixings Refresh",
        description="Refresh supported Banxico TIIE and CETE fixings.",
        execution_path="scripts/update_banxico_fixings.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobActionDefinition(
        key="banxico-tiie-fixings-refresh",
        job_name="Banxico TIIE Fixings Refresh",
        description="Refresh only the overnight, 28-day, 91-day, and 182-day TIIE fixings.",
        execution_path="scripts/update_banxico_tiie_fixings.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobActionDefinition(
        key="fred-reference-rates-refresh",
        job_name="FRED Reference Rates Refresh",
        description="Refresh configured FRED reference-rate observations.",
        execution_path="scripts/update_fred_reference_rates.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobActionDefinition(
        key="banxico-policy-refresh",
        job_name="Banxico Policy Target Refresh",
        description="Refresh the Banco de Mexico policy target series.",
        execution_path="scripts/update_banxico_policy_rates.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobActionDefinition(
        key="tiie-curve-refresh",
        job_name="Valmer TIIE Curve Refresh",
        description="Build the TIIE curve after MXN quotes and Banxico fixings are fresh.",
        execution_path="scripts/update_valmer_tiie_curve.py",
        dependencies=("irs-mxn-quotes-refresh", "banxico-fixings-refresh"),
    ),
    JobActionDefinition(
        key="sofr-curve-refresh",
        job_name="Valmer SOFR Curve Refresh",
        description="Build the SOFR curve after USD quotes are fresh.",
        execution_path="scripts/update_valmer_usd_sofr_curve.py",
        dependencies=("irs-usd-quotes-refresh",),
    ),
    JobActionDefinition(
        key="xccy-curve-refresh",
        job_name="Valmer USD/MXN XCCY Curve Refresh",
        description="Build the cross-currency curve after TIIE and SOFR are fresh.",
        execution_path="scripts/update_valmer_usd_mxn_xccy_curve.py",
        dependencies=("tiie-curve-refresh", "sofr-curve-refresh"),
    ),
    JobActionDefinition(
        key="xccy-curve-rebuild",
        job_name="Valmer USD/MXN XCCY Curve Rebuild",
        description="Force a rebuild of the current cross-currency curve for recovery.",
        execution_path="scripts/rebuild_valmer_usd_mxn_xccy_curve.py",
        dependencies=("tiie-curve-refresh", "sofr-curve-refresh"),
    ),
    JobActionDefinition(
        key="government-curve-refresh",
        job_name="Valmer MXN Government Curve Refresh",
        description="Build the government curve after the Valmer vector is fresh.",
        execution_path="scripts/update_valmer_mxn_government_curve.py",
        dependencies=("vector-refresh",),
    ),
    JobActionDefinition(
        key="standard-pipeline-refresh",
        job_name="Valmer Standard Pipeline Refresh",
        description="Execute the complete dependency-ordered production refresh.",
        execution_path="scripts/run_control_plane_pipeline.py",
    ),
    JobActionDefinition(
        key="pipeline-verification",
        job_name="Valmer Pipeline Verification",
        description="Run the persisted-data verification suite without mutating source data.",
        execution_path="scripts/verify_current_pipeline.py",
    ),
)

JOB_ACTIONS_BY_KEY = {action.key: action for action in JOB_ACTIONS}
JOB_ACTIONS_BY_NAME = {action.job_name: action for action in JOB_ACTIONS}


PIPELINE_STAGES: tuple[dict[str, object], ...] = (
    {
        "id": "sources",
        "label": "Source producers",
        "description": "Valmer vector, IRS quotes, fixings and independent reference rates.",
        "actions": [
            "vector-refresh",
            "irs-mxn-quotes-refresh",
            "irs-usd-quotes-refresh",
            "banxico-fixings-refresh",
            "fred-reference-rates-refresh",
            "banxico-policy-refresh",
        ],
    },
    {
        "id": "primary-curves",
        "label": "Primary curves",
        "description": "TIIE, SOFR and government curves after their sources are current.",
        "actions": [
            "tiie-curve-refresh",
            "sofr-curve-refresh",
            "government-curve-refresh",
        ],
    },
    {
        "id": "cross-currency",
        "label": "Cross-currency curve",
        "description": "USD/MXN curve after both upstream discount curves are current.",
        "actions": ["xccy-curve-refresh"],
    },
    {
        "id": "pipeline-operations",
        "label": "Pipeline operations",
        "description": (
            "Run the complete dependency-ordered refresh or validate the persisted result."
        ),
        "actions": ["standard-pipeline-refresh", "pipeline-verification"],
    },
    {
        "id": "operational-variants",
        "label": "Operational variants",
        "description": (
            "Alternative production-safe source configurations and explicit recovery runs."
        ),
        "actions": [
            "vector-onedrive-refresh",
            "vector-metatable-refresh",
            "banxico-tiie-fixings-refresh",
            "xccy-curve-rebuild",
        ],
    },
)


__all__ = [
    "DATA_PRODUCTS",
    "CANONICAL_ASSET_TABLE_IDENTIFIER",
    "CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER",
    "DISCOUNT_CURVE_TABLE_IDENTIFIER",
    "INDEX_FIXINGS_TABLE_IDENTIFIER",
    "JOB_ACTIONS",
    "JOB_ACTIONS_BY_KEY",
    "JOB_ACTIONS_BY_NAME",
    "PIPELINE_STAGES",
    "DataProductDefinition",
    "JobActionDefinition",
    "JobParameterDefinition",
]
