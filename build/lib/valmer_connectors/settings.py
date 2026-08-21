import os

from valmer_connectors.instruments.curve_bootstrap import (
    CETE_28_INDEX_UNIQUE_IDENTIFIER,
    CETE_91_INDEX_UNIQUE_IDENTIFIER,
    CETE_182_INDEX_UNIQUE_IDENTIFIER,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    TIIE_91_INDEX_UNIQUE_IDENTIFIER,
    TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
)

PROJECT_BUCKET_NAME = "Data Connectors"
VALMER_VECTOR_BUCKET_NAME_ENV = "VALMER_VECTOR_BUCKET_NAME"
VALMER_VECTOR_UPLOAD_DEBUG_PATH_ENV = "VALMER_VECTOR_UPLOAD_DEBUG_PATH"
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
VALMER_FORCE_PRICING_DETAILS_PATCH_ENV = "VALMER_FORCE_PRICING_DETAILS_PATCH"
VALMER_VECTOR_BYPASS_CURSOR_FILTER_ENV = "VALMER_VECTOR_BYPASS_CURSOR_FILTER"
VALMER_VECTOR_FILE_BATCH_SIZE_ENV = "VALMER_VECTOR_FILE_BATCH_SIZE"
DEFAULT_VALMER_VECTOR_FILE_BATCH_SIZE = 5
VALMER_VECTOR_LOCAL_COPY_CHUNK_SIZE_ENV = "VALMER_VECTOR_LOCAL_COPY_CHUNK_SIZE"
DEFAULT_VALMER_VECTOR_LOCAL_COPY_CHUNK_SIZE = 256 * 1024
VALMER_ONEDRIVE_TENANT_ID_SECRET_NAME_ENV = "VALMER_ONEDRIVE_TENANT_ID_SECRET_NAME"
VALMER_ONEDRIVE_CLIENT_ID_SECRET_NAME_ENV = "VALMER_ONEDRIVE_CLIENT_ID_SECRET_NAME"
VALMER_ONEDRIVE_CLIENT_SECRET_SECRET_NAME_ENV = "VALMER_ONEDRIVE_CLIENT_SECRET_SECRET_NAME"
DEFAULT_VALMER_ONEDRIVE_TENANT_ID_SECRET_NAME = "VALMER_ONEDRIVE_TENANT_ID"
DEFAULT_VALMER_ONEDRIVE_CLIENT_ID_SECRET_NAME = "VALMER_ONEDRIVE_CLIENT_ID"
DEFAULT_VALMER_ONEDRIVE_CLIENT_SECRET_SECRET_NAME = "VALMER_ONEDRIVE_CLIENT_SECRET"
VALMER_ONEDRIVE_DRIVE_ID_ENV = "VALMER_ONEDRIVE_DRIVE_ID"
VALMER_ONEDRIVE_FOLDER_PATH_ENV = "VALMER_ONEDRIVE_FOLDER_PATH"
VALMER_ONEDRIVE_CACHE_PATH_ENV = "VALMER_ONEDRIVE_CACHE_PATH"
DEFAULT_VALMER_ONEDRIVE_DRIVE_ID_CONSTANT_NAME = "VALMER_ONEDRIVE_DRIVE_ID"
DEFAULT_VALMER_ONEDRIVE_FOLDER_PATH = "clients/actinver/vector_de_precios_upload"
DEFAULT_VALMER_ONEDRIVE_CACHE_PATH = "/tmp/valmer-vector-cache"
VALMER_ONEDRIVE_GRAPH_PAGE_SIZE_ENV = "VALMER_ONEDRIVE_GRAPH_PAGE_SIZE"
DEFAULT_VALMER_ONEDRIVE_GRAPH_PAGE_SIZE = 999


def _resolve_bool_env(
    env_name: str,
    value: bool | None,
    *,
    default: bool = False,
) -> bool:
    if value is not None:
        return value
    raw_value = os.environ.get(env_name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    raise ValueError(f"{env_name} must be a boolean value, got {raw_value!r}.")


def resolve_valmer_vector_file_batch_size(batch_size: int | None = None) -> int:
    """How many source vector files to read + persist per run (bounds peak memory)."""
    if batch_size is None:
        batch_size = int(
            os.environ.get(
                VALMER_VECTOR_FILE_BATCH_SIZE_ENV,
                DEFAULT_VALMER_VECTOR_FILE_BATCH_SIZE,
            )
        )
    if batch_size <= 0:
        raise ValueError("Valmer vector file batch size must be positive.")
    return batch_size


def resolve_valmer_vector_local_copy_chunk_size(chunk_size: int | None = None) -> int:
    """Byte chunk size used when staging cloud-backed local vector files."""
    if chunk_size is None:
        chunk_size = int(
            os.environ.get(
                VALMER_VECTOR_LOCAL_COPY_CHUNK_SIZE_ENV,
                DEFAULT_VALMER_VECTOR_LOCAL_COPY_CHUNK_SIZE,
            )
        )
    if chunk_size <= 0:
        raise ValueError("Valmer vector local copy chunk size must be positive.")
    return chunk_size


def resolve_valmer_vector_bucket_name(bucket_name: str | None = None) -> str:
    return bucket_name or os.environ.get(
        VALMER_VECTOR_BUCKET_NAME_ENV,
        DEFAULT_VALMER_VECTOR_BUCKET_NAME,
    )


def resolve_valmer_onedrive_tenant_id_secret_name(secret_name: str | None = None) -> str:
    return secret_name or os.environ.get(
        VALMER_ONEDRIVE_TENANT_ID_SECRET_NAME_ENV,
        DEFAULT_VALMER_ONEDRIVE_TENANT_ID_SECRET_NAME,
    )


def resolve_valmer_onedrive_client_id_secret_name(secret_name: str | None = None) -> str:
    return secret_name or os.environ.get(
        VALMER_ONEDRIVE_CLIENT_ID_SECRET_NAME_ENV,
        DEFAULT_VALMER_ONEDRIVE_CLIENT_ID_SECRET_NAME,
    )


def resolve_valmer_onedrive_client_secret_secret_name(secret_name: str | None = None) -> str:
    return secret_name or os.environ.get(
        VALMER_ONEDRIVE_CLIENT_SECRET_SECRET_NAME_ENV,
        DEFAULT_VALMER_ONEDRIVE_CLIENT_SECRET_SECRET_NAME,
    )


def resolve_valmer_onedrive_folder_path(folder_path: str | None = None) -> str:
    return folder_path or os.environ.get(
        VALMER_ONEDRIVE_FOLDER_PATH_ENV,
        DEFAULT_VALMER_ONEDRIVE_FOLDER_PATH,
    )


def resolve_valmer_onedrive_cache_path(cache_path: str | None = None) -> str:
    return cache_path or os.environ.get(
        VALMER_ONEDRIVE_CACHE_PATH_ENV,
        DEFAULT_VALMER_ONEDRIVE_CACHE_PATH,
    )


def resolve_valmer_onedrive_graph_page_size(page_size: int | None = None) -> int:
    if page_size is None:
        page_size = int(
            os.environ.get(
                VALMER_ONEDRIVE_GRAPH_PAGE_SIZE_ENV,
                DEFAULT_VALMER_ONEDRIVE_GRAPH_PAGE_SIZE,
            )
        )
    if page_size <= 0:
        raise ValueError("Valmer OneDrive Graph page size must be positive.")
    return min(page_size, 999)


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


def resolve_valmer_force_pricing_details_patch(
    force: bool | None = None,
    *,
    default: bool = False,
) -> bool:
    return _resolve_bool_env(
        VALMER_FORCE_PRICING_DETAILS_PATCH_ENV,
        force,
        default=default,
    )


def resolve_valmer_vector_bypass_cursor_filter(
    bypass: bool | None = None,
    *,
    default: bool = False,
) -> bool:
    return _resolve_bool_env(
        VALMER_VECTOR_BYPASS_CURSOR_FILTER_ENV,
        bypass,
        default=default,
    )


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
