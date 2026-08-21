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
from banxico.policy_rates import (
    BANXICO_POLICY_TARGET_DEFINITION,
    BanxicoPolicyRateError,
    BanxicoPolicyRatesNode,
    run_banxico_policy_rates_update,
)

__all__ = [
    "BANXICO_FIXING_INDEX_IDENTIFIERS",
    "DEFAULT_SERIES_DEFINITIONS",
    "BanxicoFixingError",
    "BanxicoFixingsNode",
    "BANXICO_POLICY_TARGET_DEFINITION",
    "BanxicoPolicyRateError",
    "BanxicoPolicyRatesNode",
    "build_banxico_fixing_frame",
    "run_banxico_fixings_update",
    "run_banxico_policy_rates_update",
]
