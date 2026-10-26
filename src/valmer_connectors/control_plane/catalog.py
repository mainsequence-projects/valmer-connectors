from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from msm.models import AssetTable
from msm_pricing.models.pricing_details import AssetCurrentPricingDetailsTable

from valmer_connectors.data_nodes.canonical_index_values import DailyIndexValuesStorage
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.services.migrations import MIGRATION_JOB_EXECUTION_PATH
from valmer_connectors.storage import discount_curves_storage, index_fixings_storage


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
class JobLaunchProfile:
    key: str
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


# The storages the producers write: core Markets tables or Valmer's instances
# (VALMER_TIME_SERIES_STORAGE, read when the catalog is imported).
DISCOUNT_CURVE_TABLE_IDENTIFIER = discount_curves_storage().__table__.name
INDEX_FIXINGS_TABLE_IDENTIFIER = index_fixings_storage().__table__.name
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
        description=(
            "Registered Valmer assets with current pricing details, read through the "
            "Markets API."
        ),
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


JOB_LAUNCH_PROFILES: tuple[JobLaunchProfile, ...] = (
    JobLaunchProfile(
        key="vector-refresh",
        execution_path="scripts/update_vector_valmer.py",
        parameters=VECTOR_PARAMETERS,
    ),
    JobLaunchProfile(
        key="vector-onedrive-refresh",
        execution_path="scripts/update_vector_valmer_onedrive.py",
        parameters=VECTOR_PARAMETERS,
    ),
    JobLaunchProfile(
        key="vector-metatable-refresh",
        execution_path="scripts/update_vector_valmer_metatable.py",
        parameters=VECTOR_PARAMETERS,
    ),
    JobLaunchProfile(
        key="irs-mxn-quotes-refresh",
        execution_path="scripts/update_valmer_irs_mxn_quotes.py",
    ),
    JobLaunchProfile(
        key="irs-usd-quotes-refresh",
        execution_path="scripts/update_valmer_irs_usd_quotes.py",
    ),
    JobLaunchProfile(
        key="banxico-fixings-refresh",
        execution_path="scripts/update_banxico_fixings.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobLaunchProfile(
        key="banxico-tiie-fixings-refresh",
        execution_path="scripts/update_banxico_tiie_fixings.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobLaunchProfile(
        key="fred-reference-rates-refresh",
        execution_path="scripts/update_fred_reference_rates.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobLaunchProfile(
        key="banxico-policy-refresh",
        execution_path="scripts/update_banxico_policy_rates.py",
        parameters=(END_DATE_PARAMETER,),
    ),
    JobLaunchProfile(
        key="tiie-curve-refresh",
        execution_path="scripts/update_valmer_tiie_curve.py",
        dependencies=("irs-mxn-quotes-refresh", "banxico-fixings-refresh"),
    ),
    JobLaunchProfile(
        key="sofr-curve-refresh",
        execution_path="scripts/update_valmer_usd_sofr_curve.py",
        dependencies=("irs-usd-quotes-refresh",),
    ),
    JobLaunchProfile(
        key="xccy-curve-refresh",
        execution_path="scripts/update_valmer_usd_mxn_xccy_curve.py",
        dependencies=("tiie-curve-refresh", "sofr-curve-refresh"),
    ),
    JobLaunchProfile(
        key="xccy-curve-rebuild",
        execution_path="scripts/rebuild_valmer_usd_mxn_xccy_curve.py",
        dependencies=("tiie-curve-refresh", "sofr-curve-refresh"),
    ),
    JobLaunchProfile(
        key="government-curve-refresh",
        execution_path="scripts/update_valmer_mxn_government_curve.py",
        dependencies=("vector-refresh",),
    ),
    JobLaunchProfile(
        key="standard-pipeline-refresh",
        execution_path="scripts/run_control_plane_pipeline.py",
    ),
    JobLaunchProfile(
        key="pipeline-verification",
        execution_path="scripts/verify_current_pipeline.py",
    ),
    JobLaunchProfile(
        key="registry-seed",
        execution_path="scripts/seed_valmer_registries.py",
    ),
    JobLaunchProfile(
        key="time-series-backfill",
        execution_path="scripts/backfill_valmer_time_series.py",
    ),
)

JOB_LAUNCH_PROFILES_BY_KEY = {
    profile.key: profile for profile in JOB_LAUNCH_PROFILES
}
JOB_LAUNCH_PROFILES_BY_EXECUTION_PATH = {
    profile.execution_path: profile for profile in JOB_LAUNCH_PROFILES
}

# Jobs that only the deployment workflow runs. The migration Job applies the
# Valmer schema from the candidate image before the API rolls out; launching it
# by hand would migrate the hosted runtime outside a deployment.
DEPLOYMENT_ONLY_EXECUTION_PATHS: frozenset[str] = frozenset({MIGRATION_JOB_EXECUTION_PATH})
DEPLOYMENT_ONLY_JOB_BLOCKER = (
    "This Job runs only from the deployment workflow, before the API rolls out. "
    "It cannot be launched by hand."
)


PIPELINE_STAGES: tuple[dict[str, object], ...] = (
    {
        "id": "registries",
        "label": "Registries",
        "description": (
            "Index, convention, curve and market-data binding rows the producers "
            "reference, written through the Markets API."
        ),
        "actions": ["registry-seed"],
    },
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
    {
        "id": "storage-migration",
        "label": "Storage migration",
        "description": (
            "Copy Valmer's curves and fixings from the Markets tables into Valmer's "
            "own instances (docs/time-series-instances.md)."
        ),
        "actions": ["time-series-backfill"],
    },
)


__all__ = [
    "DATA_PRODUCTS",
    "CANONICAL_ASSET_TABLE_IDENTIFIER",
    "CURRENT_PRICING_DETAILS_TABLE_IDENTIFIER",
    "DISCOUNT_CURVE_TABLE_IDENTIFIER",
    "INDEX_FIXINGS_TABLE_IDENTIFIER",
    "JOB_LAUNCH_PROFILES",
    "JOB_LAUNCH_PROFILES_BY_EXECUTION_PATH",
    "JOB_LAUNCH_PROFILES_BY_KEY",
    "PIPELINE_STAGES",
    "DataProductDefinition",
    "JobLaunchProfile",
    "JobParameterDefinition",
]
