import unittest

import pandas as pd
import QuantLib as ql

from valmer_connectors.instruments.mexican_government_bond_curve import (
    CETES_FACE_VALUE,
    M_BONOS_FACE_VALUE,
    MexicanGovernmentBondCurveError,
    build_cetes_zero_coupon_helper,
    build_m_bono_fixed_rate_helper,
    build_mxn_government_curve_frame,
    derive_vector_time_index,
    select_mxn_government_bootstrap_instruments,
)


def _source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fecha": "20240830",
                "tipovalor": "BI",
                "emisora": "CETES",
                "serie": "240926",
                "sector": "GUBERNAMENTAL",
                "monedaemision": "MPS",
                "fechaemision": "2024-08-29",
                "fechavcto": "2024-09-26",
                "preciolimpio": 9.929507,
                "preciosucio": 9.929507,
                "interesesacumulados": 0.0,
                "tasacupon": 0.0,
                "freccpn": None,
                "valornominal": CETES_FACE_VALUE,
                "diastransccpn": None,
            },
            {
                "fecha": "20240830",
                "tipovalor": "M",
                "emisora": "BONOS",
                "serie": "260305",
                "sector": "GUBERNAMENTAL",
                "monedaemision": "MPS",
                "fechaemision": "2015-09-17",
                "fechavcto": "2026-03-05",
                "preciolimpio": 93.978153,
                "preciosucio": 96.837181,
                "interesesacumulados": 2.859028,
                "tasacupon": 5.75,
                "freccpn": "182Dias",
                "valornominal": M_BONOS_FACE_VALUE,
                "diastransccpn": 179,
            },
            {
                "fecha": "20240830",
                "tipovalor": "LD",
                "emisora": "BONDESD",
                "serie": "260305",
                "sector": "GUBERNAMENTAL",
                "monedaemision": "MPS",
                "fechaemision": "2015-09-17",
                "fechavcto": "2026-03-05",
                "preciosucio": 100.0,
            },
        ]
    )


class MxnGovernmentBondCurveTests(unittest.TestCase):
    def test_vector_time_index_uses_end_of_valuation_day(self):
        self.assertEqual(
            derive_vector_time_index("20240830"),
            pd.Timestamp("2024-08-30 23:59:59", tz="UTC"),
        )

    def test_selector_keeps_cetes_and_m_bonos_only(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())

        self.assertEqual(
            selected["unique_identifier"].tolist(),
            ["BI_CETES_240926", "M_BONOS_260305"],
        )
        self.assertTrue(selected["monedaemision"].eq("MPS").all())
        self.assertTrue(selected["sector"].eq("GUBERNAMENTAL").all())

    def test_cetes_helper_uses_zero_coupon_face_value(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        helper = build_cetes_zero_coupon_helper(selected.iloc[0])

        self.assertEqual(helper.family, "CETES")
        self.assertEqual(helper.unique_identifier, "BI_CETES_240926")
        self.assertEqual(helper.helper.bond().notional(ql.Date(30, 8, 2024)), CETES_FACE_VALUE)
        self.assertEqual(helper.helper.bond().accruedAmount(), 0.0)

    def test_m_bono_helper_validates_actual_360_accrual(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        helper = build_m_bono_fixed_rate_helper(selected.iloc[1])

        self.assertEqual(helper.family, "M_BONOS")
        self.assertEqual(helper.unique_identifier, "M_BONOS_260305")
        self.assertEqual(helper.helper.bond().notional(ql.Date(30, 8, 2024)), M_BONOS_FACE_VALUE)

    def test_m_bono_helper_rejects_bad_clean_dirty_relation(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        bad = selected.iloc[1].copy()
        bad["preciosucio"] = 95.0

        with self.assertRaisesRegex(
            MexicanGovernmentBondCurveError,
            "clean plus accrued",
        ):
            build_m_bono_fixed_rate_helper(bad)

    def test_curve_builder_returns_discount_curves_node_shape(self):
        frame = build_mxn_government_curve_frame(_source_frame())
        row = frame.reset_index().iloc[0]

        self.assertEqual(frame.index.names, ["time_index", "curve_identifier"])
        self.assertEqual(row["curve_identifier"], "VALMER_MXN_GOVERNMENT_BOND")
        self.assertIsInstance(row["curve"], dict)
        self.assertNotIn("curve_unique_identifier", frame.reset_index().columns)
        self.assertGreaterEqual(len(row["curve"]), 2)


if __name__ == "__main__":
    unittest.main()
