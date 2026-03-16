from __future__ import annotations

import json
from datetime import date, timedelta

import QuantLib as ql

import mainsequence.instruments as msi
from mainsequence.client import Constant as _C
from mainsequence.instruments.pricing_models.indices import get_index
from src.instruments.bootstrap import register_all


def _ql_date_to_iso(value: ql.Date) -> str:
    return f"{value.year():04d}-{int(value.month()):02d}-{value.dayOfMonth():02d}"


def main() -> None:
    register_all()

    target_date = date.today() - timedelta(days=1)
    tiie_uid = _C.get_value(name="REFERENCE_RATE__TIIE_28")
    index = get_index(tiie_uid, target_date=target_date, hydrate_fixings=False)

    bond = msi.FixedRateBond(
        face_value=100.0,
        coupon_rate=0.11,
        benchmark_rate_index_name=tiie_uid,
        issue_date=target_date - timedelta(days=180),
        maturity_date=target_date + timedelta(days=720),
        coupon_frequency=ql.Period(182, ql.Days),
        day_count=ql.Actual360(),
        calendar=ql.Mexico(ql.Mexico.BMV) if hasattr(ql.Mexico, "BMV") else ql.Mexico(),
        business_day_convention=ql.Following,
        settlement_days=1,
    )
    bond.set_valuation_date(target_date)
    analytics = bond.analytics(with_yield=0.11)

    result = {
        "target_date": target_date.isoformat(),
        "benchmark_rate_index": tiie_uid,
        "curve_reference_date": _ql_date_to_iso(index.forwardingTermStructure().referenceDate()),
        "clean_price": analytics["clean_price"],
        "dirty_price": analytics["dirty_price"],
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
