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
    """Upsert Index rows in two set-based row calls (resource ``index``).

    ``Index.upsert`` refuses to change an existing index's ``calculation_method``
    once formulas or observations exist; ``Index.upsert_many`` does not check, so
    the existing rows are read first in one call and any change of
    ``calculation_method`` is refused here.
    """

    from msm.api.indices import Index

    # One row per identifier (the last payload wins): a bulk upsert cannot touch a
    # row twice in one statement.
    requested = {str(payload["unique_identifier"]): dict(payload) for payload in payloads}
    values = list(requested.values())
    if not values:
        return {}
    existing = Index.get_many_by_unique_identifier(list(requested))
    changed = sorted(
        identifier
        for identifier, index in existing.items()
        if identifier in requested
        and requested[identifier].get("calculation_method") not in (None, index.calculation_method)
    )
    if changed:
        raise ValueError(
            "calculation_method cannot change for existing Index rows through a bulk "
            f"upsert: {changed!r}."
        )
    return {str(row.unique_identifier): row for row in Index.upsert_many(values)}
