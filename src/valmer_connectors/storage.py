"""Where Valmer's curves, Index fixings and daily Index values live (ms-markets ADR 0047).

Valmer produces three Markets time series besides its own tables: discount
curves (TIIE, SOFR, USD/MXN cross-currency, MXN government), Banxico Index
fixings and canonical daily Index values (Valmer curve quotes, FRED and Banxico
policy rates). ms-markets lets a provider keep each in its own namespace: an
instance of the Markets storage contract (``msm.storage_instances``) for curves
and fixings, found by Markets through the ``storage_table_uid`` of the default
pricing market-data set's bindings, and a provider Index-value table
(``configured_index_values_storage(namespace=, storage_app=, base=)``), found by
Markets' Index dataset catalog. Valmer's revisions migrate them.

Valmer's tables are ``valmer_connectors__discountcurvests__valmer`` and
``valmer_connectors__indexfixingsts__valmer`` (revision ``0002``) and
``valmer_connectors__index_values__t_1d`` (revision ``0003``), all in the
``valmer_connectors`` namespace. Which tables the producers write, the curve
builders, queries, control plane and verification read, and the registry seed
binds is selected by ``VALMER_TIME_SERIES_STORAGE``:

- ``markets`` (the default): the core Markets tables, as before. It stays the
  default until Valmer's tables are migrated and backfilled in an Environment;
- ``valmer``: Valmer's tables.

The switch is per process and every Valmer workload must agree, so it is set as
a workflow environment variable; ``docs/time-series-instances.md`` is the
runbook. Selecting a storage never changes an updater's ``update_hash``
(output tables are not part of it).
"""

from __future__ import annotations

import os
from typing import Any

from valmer_connectors.markets import VALMER_MARKETS_NAMESPACE, VALMER_MARKETS_STORAGE_APP

VALMER_TIME_SERIES_STORAGE_ENV = "VALMER_TIME_SERIES_STORAGE"
MARKETS_TIME_SERIES_STORAGE = "markets"
VALMER_TIME_SERIES_STORAGE = "valmer"
TIME_SERIES_STORAGES = (MARKETS_TIME_SERIES_STORAGE, VALMER_TIME_SERIES_STORAGE)
# The instance key; table names are <storage_app>__<core table>__<key>.
VALMER_INSTANCE_KEY = "valmer"

__all__ = [
    "MARKETS_TIME_SERIES_STORAGE",
    "TIME_SERIES_STORAGES",
    "VALMER_INSTANCE_KEY",
    "VALMER_TIME_SERIES_STORAGE",
    "VALMER_TIME_SERIES_STORAGE_ENV",
    "daily_index_values_storage",
    "discount_curves_storage",
    "index_fixings_storage",
    "resolve_time_series_storage",
    "storage_table_identifier",
    "valmer_discount_curves_instance",
    "valmer_index_fixings_instance",
    "valmer_time_series_instances",
]


def resolve_time_series_storage(storage: str | None = None) -> str:
    """``storage`` or ``VALMER_TIME_SERIES_STORAGE``, defaulting to ``markets``."""

    value = storage if storage is not None else os.environ.get(VALMER_TIME_SERIES_STORAGE_ENV)
    normalized = (value or MARKETS_TIME_SERIES_STORAGE).strip().lower()
    if normalized not in TIME_SERIES_STORAGES:
        raise ValueError(
            f"{VALMER_TIME_SERIES_STORAGE_ENV} must be one of {TIME_SERIES_STORAGES!r}, "
            f"got {value!r}."
        )
    return normalized


def _instance(core_storage: type[Any]) -> type[Any]:
    from msm.base import MarketsBase
    from msm.storage_instances import configured_time_index_instance

    # MarketsBase is the declarative base of Valmer's own tables, so its
    # migration provider owns the instances with them.
    return configured_time_index_instance(
        core_storage,
        instance_key=VALMER_INSTANCE_KEY,
        namespace=VALMER_MARKETS_NAMESPACE,
        storage_app=VALMER_MARKETS_STORAGE_APP,
        base=MarketsBase,
    )


def valmer_discount_curves_instance() -> type[Any]:
    """Valmer's instance of the Markets discount-curve storage (``DiscountCurvesTS``)."""

    from msm_pricing.time_index.curves.storage import DiscountCurvesStorage

    return _instance(DiscountCurvesStorage)


def valmer_index_fixings_instance() -> type[Any]:
    """Valmer's instance of the Markets Index-fixing storage (``IndexFixingsTS``)."""

    from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

    return _instance(IndexFixingsStorage)


def valmer_time_series_instances() -> list[type[Any]]:
    """Both instances, in the order the migration provider registers them."""

    return [valmer_discount_curves_instance(), valmer_index_fixings_instance()]


def discount_curves_storage(storage: str | None = None) -> type[Any]:
    """The discount-curve storage the selected time-series storage names."""

    if resolve_time_series_storage(storage) == VALMER_TIME_SERIES_STORAGE:
        return valmer_discount_curves_instance()
    from msm_pricing.time_index.curves.storage import DiscountCurvesStorage

    return DiscountCurvesStorage


def index_fixings_storage(storage: str | None = None) -> type[Any]:
    """The Index-fixing storage the selected time-series storage names."""

    if resolve_time_series_storage(storage) == VALMER_TIME_SERIES_STORAGE:
        return valmer_index_fixings_instance()
    from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

    return IndexFixingsStorage


def daily_index_values_storage(storage: str | None = None) -> type[Any]:
    """The canonical daily Index-value table the selected time-series storage names.

    The Markets table ``ms_markets__index_values__t_1d`` by default, Valmer's own
    ``valmer_connectors__index_values__t_1d`` with ``valmer``. The quote, FRED and
    Banxico policy producers write it, the curve builders, queries, control plane
    and verification read it, and Markets' Index dataset catalog discovers either.
    """

    from valmer_connectors.data_nodes.canonical_index_values import (
        MarketsDailyIndexValuesStorage,
        ValmerDailyIndexValuesStorage,
    )

    if resolve_time_series_storage(storage) == VALMER_TIME_SERIES_STORAGE:
        return ValmerDailyIndexValuesStorage
    return MarketsDailyIndexValuesStorage


def storage_table_identifier(storage: type[Any]) -> str:
    """The MetaTables identifier of a storage class, without attaching it."""

    return str(storage.__table__.info["identifier"])
