from __future__ import annotations

from functools import partial
from threading import RLock
from typing import Any, Callable

import QuantLib as ql

DEFAULT_CONSTANTS = {
    "REFERENCE_RATE__TIIE_28": "TIIE_28",
    "REFERENCE_RATE__TIIE_91": "TIIE_91",
    "REFERENCE_RATE__TIIE_182": "TIIE_182",
    "REFERENCE_RATE__TIIE_OVERNIGHT": "TIIE_OVERNIGHT",
    "REFERENCE_RATE__CETE_28": "CETE_28",
    "REFERENCE_RATE__CETE_91": "CETE_91",
    "REFERENCE_RATE__CETE_182": "CETE_182",
    "ZERO_CURVE__VALMER_TIIE_28": "VALMER_TIIE_28",
}

REQUIRED_CURVE_CONSTS = ("ZERO_CURVE__VALMER_TIIE_28",)
REQUIRED_INDEX_CONSTS = (
    "REFERENCE_RATE__TIIE_OVERNIGHT",
    "REFERENCE_RATE__TIIE_28",
    "REFERENCE_RATE__TIIE_91",
    "REFERENCE_RATE__TIIE_182",
)
OPTIONAL_EXTERNAL_CURVE_CONST = "ZERO_CURVE__BANXICO_M_BONOS_OTR"

_LOCK = RLock()
_REGISTERED = False


def seed_defaults() -> None:
    from mainsequence.client import Constant as _C

    _C.create_constants_if_not_exist(DEFAULT_CONSTANTS)


def register_all(*, override: bool = False) -> None:
    global _REGISTERED

    with _LOCK:
        if _REGISTERED and not override:
            return
        seed_defaults()
        register_etl_builders(override=override)
        register_pricing_indices(override=override)
        _REGISTERED = True


def register_etl_builders(*, override: bool = False) -> None:
    from mainsequence.instruments.interest_rates.etl.registry import DISCOUNT_CURVE_BUILDERS

    from src.instruments.rates_curves import build_tiie_valmer

    _safe_register(
        DISCOUNT_CURVE_BUILDERS,
        "ZERO_CURVE__VALMER_TIIE_28",
        build_tiie_valmer,
        override=override,
    )


def register_pricing_indices(*, override: bool = False) -> None:
    from mainsequence.client import Constant as _C
    from mainsequence.instruments.pricing_models.indices import register_index_spec
    from mainsequence.instruments.pricing_models.indices_builders import IndexSpec

    def _const(name: str) -> str:
        return _C.get_value(name=name)

    def _mx_calendar() -> ql.Calendar:
        if hasattr(ql.Mexico, "BMV"):
            return ql.Mexico(ql.Mexico.BMV)
        return ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()

    def _mx_currency() -> ql.Currency:
        return ql.MXNCurrency() if hasattr(ql, "MXNCurrency") else ql.USDCurrency()

    cal = _mx_calendar()
    ccy = _mx_currency()
    dc = ql.Actual360()
    curve_uid_tiie = _const("ZERO_CURVE__VALMER_TIIE_28")

    def _build_ibor_days_spec(
        *,
        curve_uid: str,
        period_days: int,
        settlement_days: int = 1,
        bdc: int = ql.ModifiedFollowing,
    ) -> IndexSpec:
        return IndexSpec(
            curve_uid=curve_uid,
            calendar=cal,
            day_counter=dc,
            currency=ccy,
            period=ql.Period(int(period_days), ql.Days),
            settlement_days=int(settlement_days),
            bdc=int(bdc),
            end_of_month=False,
        )

    def _register_tenors(
        *,
        curve_uid: str,
        tenors: tuple[tuple[str, int], ...],
        bdc: int,
    ) -> None:
        for const_name, days in tenors:
            index_uid = _const(const_name)
            register_index_spec(
                index_uid,
                partial(
                    _build_ibor_days_spec,
                    curve_uid=curve_uid,
                    period_days=days,
                    settlement_days=1,
                    bdc=bdc,
                ),
                override=override,
            )

    _register_tenors(
        curve_uid=curve_uid_tiie,
        tenors=(
            ("REFERENCE_RATE__TIIE_OVERNIGHT", 1),
            ("REFERENCE_RATE__TIIE_28", 28),
            ("REFERENCE_RATE__TIIE_91", 91),
            ("REFERENCE_RATE__TIIE_182", 182),
        ),
        bdc=ql.ModifiedFollowing,
    )

    try:
        curve_uid_cete = _const(OPTIONAL_EXTERNAL_CURVE_CONST)
    except Exception:
        curve_uid_cete = None

    if curve_uid_cete:
        _register_tenors(
            curve_uid=curve_uid_cete,
            tenors=(
                ("REFERENCE_RATE__CETE_28", 28),
                ("REFERENCE_RATE__CETE_91", 91),
                ("REFERENCE_RATE__CETE_182", 182),
            ),
            bdc=ql.Following,
        )


def _safe_register(registry: Any, key: str, fn: Callable[..., Any], *, override: bool) -> None:
    try:
        registry.register(key, fn, override=override)
        return
    except TypeError:
        pass
    except Exception:
        if override:
            return
        raise

    try:
        registry.register(key, fn)
    except Exception:
        if override:
            return
        raise
