import datetime as dt
import uuid
import unittest
from unittest.mock import patch

import QuantLib as ql

from valmer_connectors.instruments.vector_to_asset import (
    CoreBondPricingPayload,
    build_instrument_from_core_bond_pricing_payload,
    resolve_reference_index_uid,
)
from valmer_connectors.settings import SUBYACENTE_TO_INDEX_MAP


class _FakeInstrument:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.valuation_date = None

    def set_valuation_date(self, value):
        self.valuation_date = value


class ValmerInstrumentIndexUidTests(unittest.TestCase):
    def _payload(self, instrument_type: str, **overrides):
        values = {
            "instrument_type": instrument_type,
            "valuation_date": dt.date(2024, 9, 5),
            "issue_date": dt.date(2024, 1, 1),
            "maturity_date": dt.date(2025, 1, 1),
            "face_value": 100.0,
            "day_count": ql.Actual360(),
            "calendar": ql.Mexico(ql.Mexico.BMV),
            "business_day_convention": ql.Following,
            "settlement_days": 1,
            "benchmark_rate_index_uid": uuid.uuid4(),
        }
        values.update(overrides)
        return CoreBondPricingPayload(**values)

    def test_fixed_rate_bond_uses_benchmark_index_uid(self):
        payload = self._payload(
            "fixed_rate_bond",
            coupon_rate=0.1,
            coupon_frequency=ql.Period(182, ql.Days),
        )

        with patch(
            "valmer_connectors.instruments.vector_to_asset.msi.FixedRateBond",
            _FakeInstrument,
        ):
            instrument = build_instrument_from_core_bond_pricing_payload(payload)

        self.assertEqual(
            instrument.kwargs["benchmark_rate_index_uid"],
            payload.benchmark_rate_index_uid,
        )
        self.assertNotIn("benchmark_rate_index_name", instrument.kwargs)
        self.assertEqual(instrument.valuation_date, payload.valuation_date)

    def test_zero_coupon_bond_uses_benchmark_index_uid(self):
        payload = self._payload("zero_coupon_bond")

        with patch(
            "valmer_connectors.instruments.vector_to_asset.msi.ZeroCouponBond",
            _FakeInstrument,
        ):
            instrument = build_instrument_from_core_bond_pricing_payload(payload)

        self.assertEqual(
            instrument.kwargs["benchmark_rate_index_uid"],
            payload.benchmark_rate_index_uid,
        )
        self.assertNotIn("benchmark_rate_index_name", instrument.kwargs)

    def test_floating_rate_bond_uses_floating_index_uid(self):
        index_uid = uuid.uuid4()
        payload = self._payload(
            "floating_rate_bond",
            benchmark_rate_index_uid=index_uid,
            floating_rate_index_uid=index_uid,
            spread=0.0025,
            coupon_frequency=ql.Period(28, ql.Days),
        )

        with patch(
            "valmer_connectors.instruments.vector_to_asset.msi.FloatingRateBond",
            _FakeInstrument,
        ):
            instrument = build_instrument_from_core_bond_pricing_payload(payload)

        self.assertEqual(instrument.kwargs["floating_rate_index_uid"], index_uid)
        self.assertEqual(instrument.kwargs["benchmark_rate_index_uid"], index_uid)
        self.assertNotIn("floating_rate_index_name", instrument.kwargs)
        self.assertNotIn("benchmark_rate_index_name", instrument.kwargs)

    def test_reference_index_uid_resolver_uses_bootstrapped_index_rows(self):
        index_uid = uuid.uuid4()
        fake_index = type("FakeIndex", (), {"uid": index_uid})()

        with patch(
            "valmer_connectors.instruments.vector_to_asset._REFERENCE_INDEX_UID_CACHE",
            None,
        ):
            with patch(
                "valmer_connectors.instruments.curve_bootstrap.upsert_mexican_reference_indexes",
                return_value={"CETE_28": fake_index},
            ):
                self.assertEqual(resolve_reference_index_uid("CETE_28"), index_uid)

    def test_subyacente_map_never_uses_synthetic_government_bond_index(self):
        self.assertNotIn("MXN_GOVERNMENT_BOND", set(SUBYACENTE_TO_INDEX_MAP.values()))
        self.assertEqual(SUBYACENTE_TO_INDEX_MAP["Bonos M Bruta(Yield)"], "CETE_28")
        self.assertEqual(SUBYACENTE_TO_INDEX_MAP["P8-X8"], "CETE_182")


if __name__ == "__main__":
    unittest.main()
