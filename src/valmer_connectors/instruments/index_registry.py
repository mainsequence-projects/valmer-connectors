"""Set-based writes of the Index registry rows Valmer publishes (ms-markets ADR 0047).

Each helper is one or two ``msm.api`` row calls whatever the number of rows, so
under the Markets API a producer never loops one API call per index.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

__all__ = ["upsert_custom_indexes", "upsert_index_types"]


def upsert_index_types(payloads: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Upsert IndexType rows in one ``IndexType.upsert_many`` call (resource ``index-type``)."""

    from msm.api.indices import IndexType

    values = [dict(payload) for payload in payloads]
    if not values:
        return {}
    return {str(row.index_type): row for row in IndexType.upsert_many(values)}


def upsert_custom_indexes(payloads: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Upsert Index rows in one ``Index.upsert_many`` call (resource ``index``).

    ``Index.upsert_many`` applies ``Index.upsert``'s guard itself: an existing
    index keeps its ``calculation_method`` once it has formulas or canonical
    observations, and one refused payload rejects the whole call
    (``ValueError``).
    """

    from msm.api.indices import Index

    # One row per identifier (the last payload wins): a bulk upsert cannot touch a
    # row twice in one statement.
    values = list(
        {str(payload["unique_identifier"]): dict(payload) for payload in payloads}.values()
    )
    if not values:
        return {}
    return {str(row.unique_identifier): row for row in Index.upsert_many(values)}
