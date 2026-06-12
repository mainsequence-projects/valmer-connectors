import os

from valmer_connectors.instruments.curve_bootstrap import (
    CETE_182_INDEX_UNIQUE_IDENTIFIER,
    CETE_28_INDEX_UNIQUE_IDENTIFIER,
    CETE_91_INDEX_UNIQUE_IDENTIFIER,
    TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    TIIE_91_INDEX_UNIQUE_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
)

PROJECT_BUCKET_NAME = "Data Connectors"
VALMER_VECTOR_BUCKET_NAME_ENV = "VALMER_VECTOR_BUCKET_NAME"
DEFAULT_VALMER_VECTOR_BUCKET_NAME = "Hitorical Valmer Vector Analytico"
DEFAULT_VECTOR_FIRST_LOOP_COUNT = 360 // 5
VALMER_META_OPERATION_BATCH_SIZE_ENV = "VALMER_PER_PAGE"
DEFAULT_VALMER_META_OPERATION_BATCH_SIZE = 1000
MAX_VALMER_META_OPERATION_BATCH_SIZE = 1000
VALMER_ASSET_UPSERT_BATCH_SIZE_ENV = "VALMER_ASSET_UPSERT_BATCH_SIZE"
DEFAULT_VALMER_ASSET_UPSERT_BATCH_SIZE = 500
MAX_VALMER_ASSET_UPSERT_BATCH_SIZE = 500
VALMER_PRICING_DETAILS_BATCH_SIZE_ENV = "VALMER_PRICING_DETAILS_BATCH_SIZE"
DEFAULT_VALMER_PRICING_DETAILS_BATCH_SIZE = 5000
MAX_VALMER_PRICING_DETAILS_BATCH_SIZE = 5000


def resolve_valmer_vector_bucket_name(bucket_name: str | None = None) -> str:
    return bucket_name or os.environ.get(
        VALMER_VECTOR_BUCKET_NAME_ENV,
        DEFAULT_VALMER_VECTOR_BUCKET_NAME,
    )


def resolve_valmer_meta_operation_batch_size(batch_size: int | None = None) -> int:
    if batch_size is None:
        batch_size = int(
            os.environ.get(
                VALMER_META_OPERATION_BATCH_SIZE_ENV,
                DEFAULT_VALMER_META_OPERATION_BATCH_SIZE,
            )
        )
    if batch_size <= 0:
        raise ValueError("Valmer MetaTable operation batch size must be positive.")
    return min(batch_size, MAX_VALMER_META_OPERATION_BATCH_SIZE)


def resolve_valmer_asset_upsert_batch_size(batch_size: int | None = None) -> int:
    if batch_size is None:
        batch_size = int(
            os.environ.get(
                VALMER_ASSET_UPSERT_BATCH_SIZE_ENV,
                DEFAULT_VALMER_ASSET_UPSERT_BATCH_SIZE,
            )
        )
    if batch_size <= 0:
        raise ValueError("Valmer asset upsert batch size must be positive.")
    return min(batch_size, MAX_VALMER_ASSET_UPSERT_BATCH_SIZE)


def resolve_valmer_pricing_details_batch_size(batch_size: int | None = None) -> int:
    if batch_size is None:
        batch_size = int(
            os.environ.get(
                VALMER_PRICING_DETAILS_BATCH_SIZE_ENV,
                DEFAULT_VALMER_PRICING_DETAILS_BATCH_SIZE,
            )
        )
    if batch_size <= 0:
        raise ValueError("Valmer pricing-details batch size must be positive.")
    return min(batch_size, MAX_VALMER_PRICING_DETAILS_BATCH_SIZE)


BUCKET_NAME_HISTORICAL_VECTORS = resolve_valmer_vector_bucket_name()
SUBYACENTE_TO_INDEX_MAP = {
    "TIIE28": TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    "TIIE182": TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    "TIIE91": TIIE_91_INDEX_UNIQUE_IDENTIFIER,
    "TIIE28 EQUIV 182": TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    "Tasa TIIE Fondeo 1D": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    "CETE_28": CETE_28_INDEX_UNIQUE_IDENTIFIER,
    "CETE28": CETE_28_INDEX_UNIQUE_IDENTIFIER,
    "CETE182": CETE_182_INDEX_UNIQUE_IDENTIFIER,
    "Bonos M Bruta(Yield)": CETE_28_INDEX_UNIQUE_IDENTIFIER,
    "Fondeo Bancario": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    "Tasa TIIE Fondeo 1D": TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    "IRMXP-FGub-28": CETE_28_INDEX_UNIQUE_IDENTIFIER,
    "IRMXP-FGub-91": CETE_91_INDEX_UNIQUE_IDENTIFIER,
    "AAA": TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    "D1": TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    "P8-X8": CETE_182_INDEX_UNIQUE_IDENTIFIER,
    "P12-X12": CETE_182_INDEX_UNIQUE_IDENTIFIER,
    "P4-X4": CETE_91_INDEX_UNIQUE_IDENTIFIER,
}
