from __future__ import annotations

from collections.abc import Sequence
from threading import RLock
from typing import Any

from valmer_connectors.instruments.curve_bootstrap import (
    attach_valmer_curve_pricing_runtime,
    bootstrap_valmer_curve_pricing,
)

_LOCK = RLock()
_RUNTIME_ATTACHED = False
_STATIC_DEFAULTS_SEEDED = False


def _valmer_markets_models(
    *,
    extra_markets_models: Sequence[Any] | None = None,
) -> list[Any]:
    from msm.models import IndexDatasetAvailabilityTable, IndexFormulaInputTable

    from valmer_connectors.data_nodes.canonical_index_values import (
        DailyIndexValuesStorage,
    )
    from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
    from valmer_connectors.meta_tables.valmer_asset_details import (
        ValmerAssetDetailsTable,
    )

    markets_models = [
        ValmerAssetDetailsTable,
        ValmerVectorPricesStorage,
        DailyIndexValuesStorage,
        # ADR-0037 index framework: Index row operations now require these two
        # bound tables in addition to IndexTable/IndexFormulaDefinitionTable.
        IndexDatasetAvailabilityTable,
        IndexFormulaInputTable,
    ]
    if extra_markets_models is not None:
        markets_models.extend(extra_markets_models)
    return markets_models


def attach_runtime_defaults(
    *,
    extra_markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    """Attach Valmer runtime tables without seeding static pricing rows."""

    from valmer_connectors.meta_tables.valmer_asset_details import (
        ensure_valmer_asset_detail_runtime,
    )

    result = {
        "pricing_context": attach_valmer_curve_pricing_runtime(
            markets_models=_valmer_markets_models(
                extra_markets_models=extra_markets_models,
            ),
            **runtime_kwargs,
        )
    }
    result["valmer_asset_details_context"] = ensure_valmer_asset_detail_runtime(
        timeout=runtime_kwargs.get("timeout"),
    )
    return result


def seed_static_defaults(
    *,
    extra_markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any]:
    """Attach runtime tables and seed static rows used by this project."""

    from valmer_connectors.meta_tables.valmer_asset_details import (
        ensure_valmer_asset_detail_runtime,
    )

    result = bootstrap_valmer_curve_pricing(
        markets_models=_valmer_markets_models(
            extra_markets_models=extra_markets_models,
        ),
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
    seed_static_rows: bool = True,
    extra_markets_models: Sequence[Any] | None = None,
    **runtime_kwargs: Any,
) -> dict[str, Any] | None:
    """Attach runtime tables and seed current Valmer static pricing rows.

    ``extra_markets_models`` extends the single shared markets runtime startup
    with downstream portfolio or project-local SQLAlchemy models before pricing
    schemas are attached.
    """

    global _RUNTIME_ATTACHED, _STATIC_DEFAULTS_SEEDED

    with _LOCK:
        if seed_static_rows:
            if _STATIC_DEFAULTS_SEEDED and not override:
                return None
            result = seed_static_defaults(
                extra_markets_models=extra_markets_models,
                **runtime_kwargs,
            )
            _RUNTIME_ATTACHED = True
            _STATIC_DEFAULTS_SEEDED = True
            return result

        if _RUNTIME_ATTACHED and not override:
            return None
        result = attach_runtime_defaults(
            extra_markets_models=extra_markets_models,
            **runtime_kwargs,
        )
        _RUNTIME_ATTACHED = True
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
        seed_static_rows=True,
        extra_markets_models=extra_markets_models,
        **runtime_kwargs,
    )


__all__ = [
    "attach_runtime_defaults",
    "bootstrap_runtime",
    "register_all",
    "seed_defaults",
    "seed_static_defaults",
]
