from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from valmer_connectors.instruments.curve_bootstrap import (
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
)


def resolve_valmer_overnight_index(floating_index: str | None, node: Mapping[str, Any]):
    """Resolve Valmer reference-index identifiers to QuantLib overnight indexes."""

    _ = node
    token = str(floating_index or "").strip()
    if token == TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER:
        return _ftiiie_overnight_index()
    if token == USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER:
        return _quantlib().Sofr()
    raise ValueError(f"Unsupported Valmer overnight floating_index {floating_index!r}.")


def _ftiiie_overnight_index():
    ql = _quantlib()
    return ql.OvernightIndex(
        "FTIIE",
        1,
        ql.MXNCurrency(),
        ql.Mexico(),
        ql.Actual360(),
        ql.YieldTermStructureHandle(),
    )


def _quantlib():
    import QuantLib as ql

    return ql


__all__ = ["resolve_valmer_overnight_index"]
