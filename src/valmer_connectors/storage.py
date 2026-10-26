"""Where Valmer's discount curves and Index fixings live (ms-markets ADR 0047).

Valmer produces two Markets time series besides its own tables: discount curves
(TIIE, SOFR, USD/MXN cross-currency, MXN government) and Banxico Index fixings.
ms-markets lets a provider keep such a series in its own namespace: an instance
of the Markets storage contract (``msm.storage_instances``), migrated by the
provider's Alembic revisions and found by Markets through the
``storage_table_uid`` of the default pricing market-data set's bindings.

Valmer's instances are ``valmer_connectors__discountcurvests__valmer`` and
``valmer_connectors__indexfixingsts__valmer`` in the ``valmer_connectors``
namespace (revision ``0002``). Which storage the producers write, the curve
builders and verification read, and the registry seed binds is selected by
``VALMER_TIME_SERIES_STORAGE``:

- ``markets`` (the default): the core Markets tables, as before. It stays the
  default until the instances are migrated and backfilled in an Environment;
- ``valmer``: Valmer's instances.

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


def storage_table_identifier(storage: type[Any]) -> str:
    """The MetaTables identifier of a storage class, without attaching it."""

    return str(storage.__table__.info["identifier"])
