import unittest

import QuantLib as ql
from msm_pricing.pricing_engine.curves import reconstruct_curve_handle_from_key_nodes

from valmer_connectors.instruments.curve_reconstruction import (
    resolve_valmer_overnight_index,
)
from valmer_connectors.instruments.rates_curves import build_tiie_irs_mxn_curve_frame

IRS_MXN_SINGLE_HELPER_SAMPLE = (
    b"FX.USD.MXN.ON,14.000000000000\n"
    b"Swap.28D.MXN.FTIIE.1D/28D.BANXICO,6.52560000\n"
)


class ValmerCurveReconstructionTests(unittest.TestCase):
    def test_resolve_valmer_overnight_index_supports_tiie_and_sofr(self):
        tiie = resolve_valmer_overnight_index("TIIE_OVERNIGHT", {})
        sofr = resolve_valmer_overnight_index("USD_SOFR_OVERNIGHT", {})

        self.assertIsInstance(tiie, ql.OvernightIndex)
        self.assertIsInstance(sofr, ql.OvernightIndex)
        self.assertEqual(sofr.fixingCalendar().name(), "SOFR fixing calendar")

    def test_resolve_valmer_overnight_index_rejects_unknown_identifier(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Valmer overnight"):
            resolve_valmer_overnight_index("BAD_INDEX", {})

    def test_ms_markets_reconstructs_tiie_curve_from_valmer_key_nodes(self):
        frame = build_tiie_irs_mxn_curve_frame(
            IRS_MXN_SINGLE_HELPER_SAMPLE,
            curve_identifier="VALMER_TIIE_OVERNIGHT",
            valuation_date="2026-06-30",
        )
        row = frame.reset_index().iloc[0]

        handle = reconstruct_curve_handle_from_key_nodes(
            row["key_nodes"],
            valuation_date=ql.Date(30, 6, 2026),
            day_counter=ql.Actual360(),
            bootstrap_method="piecewise_log_linear_discount",
            overnight_index_resolver=resolve_valmer_overnight_index,
        )
        rebuilt_rate = handle.zeroRate(
            ql.Date(29, 7, 2026),
            ql.Actual360(),
            ql.Compounded,
            ql.Annual,
            False,
        ).rate()

        self.assertAlmostEqual(rebuilt_rate, row["curve"][29], delta=1e-12)


if __name__ == "__main__":
    unittest.main()
