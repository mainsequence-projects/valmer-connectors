from __future__ import annotations

from msm.base import MarketsBase

from mainsequence.meta_tables.migrations import build_metatable_model_registry


def _metatable_provider_model_sources() -> list[type[MarketsBase]]:
    from valmer_connectors.data_nodes.canonical_index_values import (
        DailyIndexValuesStorage,
    )
    from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
    from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable

    return [
        ValmerAssetDetailsTable,
        ValmerVectorPricesStorage,
        DailyIndexValuesStorage,
    ]


METATABLE_PROVIDER_MODELS: tuple[type[MarketsBase], ...] = tuple(
    build_metatable_model_registry(
        _metatable_provider_model_sources(),
        base=MarketsBase,
    )
)
"""Valmer project-owned MetaTable models managed by the SDK Alembic provider."""


def metatable_provider_models() -> list[type[MarketsBase]]:
    """Return the Valmer project-owned MetaTable provider model scope."""

    return list(METATABLE_PROVIDER_MODELS)


__all__ = [
    "METATABLE_PROVIDER_MODELS",
    "metatable_provider_models",
]
