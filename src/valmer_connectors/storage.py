"""Valmer's own curve and fixing tables (ms-markets ADR 0047).

Valmer produces three Markets time series besides its own tables, and keeps
each in its own namespace, migrated by its own revisions:

- discount curves (TIIE, SOFR, USD/MXN cross-currency, MXN government) in
  ``valmer_connectors__discountcurvests__valmer`` and Banxico Index fixings in
  ``valmer_connectors__indexfixingsts__valmer`` (revision ``0002``): instances
  of the Markets storage contracts (``msm.storage_instances``), found by Markets
  through the ``storage_table_uid`` of the default pricing market-data set's
  bindings, which the registry seed writes;
- canonical daily Index values in ``valmer_connectors__index_values__t_1d``
  (revision ``0003``, ``ValmerDailyIndexValuesStorage`` in
  ``valmer_connectors.data_nodes.canonical_index_values``), found by Markets'
  Index dataset catalog.

Valmer's producers write and its readers read only these tables. The Markets
tables Valmer wrote curves and fixings to before (``DiscountCurvesTS``,
``IndexFixingsTS``) appear only in the one-off move
(``valmer_connectors.services.time_series_backfill``), as copy source and
rollback target; docs/time-series-instances.md is the runbook.
"""

from __future__ import annotations

from typing import Any

from valmer_connectors.markets import VALMER_MARKETS_NAMESPACE, VALMER_MARKETS_STORAGE_APP

# The instance key; table names are <storage_app>__<core table>__<key>.
VALMER_INSTANCE_KEY = "valmer"

__all__ = [
    "VALMER_INSTANCE_KEY",
    "storage_table_identifier",
    "valmer_discount_curves_instance",
    "valmer_index_fixings_instance",
    "valmer_time_series_instances",
]


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
    """Valmer's discount-curve table, an instance of the Markets ``DiscountCurvesTS``."""

    from msm_pricing.time_index.curves.storage import DiscountCurvesStorage

    return _instance(DiscountCurvesStorage)


def valmer_index_fixings_instance() -> type[Any]:
    """Valmer's Index-fixing table, an instance of the Markets ``IndexFixingsTS``."""

    from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

    return _instance(IndexFixingsStorage)


def valmer_time_series_instances() -> list[type[Any]]:
    """Both instances, in the order the migration provider registers them."""

    return [valmer_discount_curves_instance(), valmer_index_fixings_instance()]


def storage_table_identifier(storage: type[Any]) -> str:
    """The MetaTables identifier of a storage class, without attaching it."""

    return str(storage.__table__.info["identifier"])
