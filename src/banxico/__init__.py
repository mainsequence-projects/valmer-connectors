"""Banxico SIE source integration for Mexican reference-rate fixings."""

from __future__ import annotations

from banxico.fixings import (
    BANXICO_FIXING_INDEX_IDENTIFIERS,
    DEFAULT_SERIES_DEFINITIONS,
    BanxicoFixingError,
    BanxicoFixingsNode,
    build_banxico_fixing_frame,
    run_banxico_fixings_update,
)

__all__ = [
    "BANXICO_FIXING_INDEX_IDENTIFIERS",
    "DEFAULT_SERIES_DEFINITIONS",
    "BanxicoFixingError",
    "BanxicoFixingsNode",
    "build_banxico_fixing_frame",
    "run_banxico_fixings_update",
]
