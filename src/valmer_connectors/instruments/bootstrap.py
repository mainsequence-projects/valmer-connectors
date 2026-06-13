from __future__ import annotations

from collections.abc import Sequence
from threading import RLock
from typing import Any

from valmer_connectors.instruments.curve_bootstrap import bootstrap_valmer_curve_pricing

_LOCK = RLock()
_BOOTSTRAPPED = False


def seed_static_defaults(
    *,
    extra_markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    """Attach runtime tables and seed static rows used by this project."""

    from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage

    from valmer_connectors.meta_tables.valmer_asset_details import (
        ValmerAssetDetailsTable,
        ensure_valmer_asset_detail_runtime,
    )

    markets_models = [ValmerAssetDetailsTable, ValmerVectorPricesStorage]
    if extra_markets_models is not None:
        markets_models.extend(extra_markets_models)

    result = bootstrap_valmer_curve_pricing(
        markets_models=markets_models,
        **runtime_kwargs,
    )
    result["valmer_asset_details_context"] = ensure_valmer_asset_detail_runtime(
        timeout=runtime_kwargs.get("timeout"),
    )
    return result


def seed_defaults(**runtime_kwargs: Any) -> dict[str, Any]:
    """Compatibility wrapper for the old bootstrap helper name."""

    return seed_static_defaults(**runtime_kwargs)


def bootstrap_runtime(
    *,
    override: bool = False,
    extra_markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any] | None:
    """Attach runtime tables and seed current Valmer static pricing rows.

    ``extra_markets_models`` extends the single shared markets runtime startup
    with downstream portfolio or project-local SQLAlchemy models before pricing
    schemas are attached.
    """

    global _BOOTSTRAPPED

    with _LOCK:
        if _BOOTSTRAPPED and not override:
            return None
        result = seed_static_defaults(
            extra_markets_models=extra_markets_models,
            **runtime_kwargs,
        )
        _BOOTSTRAPPED = True
        return result


def register_all(
    *,
    override: bool = False,
    extra_markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any] | None:
    """Compatibility wrapper for the old project bootstrap entry point."""

    return bootstrap_runtime(
        override=override,
        extra_markets_models=extra_markets_models,
        **runtime_kwargs,
    )


__all__ = ["bootstrap_runtime", "register_all", "seed_defaults", "seed_static_defaults"]
