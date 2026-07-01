import unittest

import pandas as pd
import QuantLib as ql
from msm_pricing.data_nodes import DiscountCurvesNode

from valmer_connectors.instruments.curve_key_nodes import validate_mxn_government_key_nodes
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
                "yield_rate": 10.5,
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
                "yield_rate": 9.75,
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
        self.assertEqual(helper.quote, 9.929507)
        self.assertEqual(helper.helper.bond().notional(ql.Date(30, 8, 2024)), CETES_FACE_VALUE)
        self.assertEqual(helper.helper.bond().accruedAmount(), 0.0)

    def test_m_bono_helper_validates_actual_360_accrual(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        helper = build_m_bono_fixed_rate_helper(selected.iloc[1])

        self.assertEqual(helper.family, "M_BONOS")
        self.assertEqual(helper.unique_identifier, "M_BONOS_260305")
        self.assertEqual(helper.quote, 93.978153)
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
        self.assertEqual(
            row["key_nodes"],
            [
                {
                    "maturity_date": "2024-09-26",
                    "asset_identifier": "BI_CETES_240926",
                    "instrument_type": "zero_coupon_bond",
                    "helper_type": "zero_coupon_bond_helper",
                    "quote": 9.929507,
                    "quote_type": "clean_price",
                    "quote_unit": "price_per_10",
                    "quote_side": "mid",
                    "quote_source": "preciosucio",
                    "source_quote_type": "dirty_price",
                    "yield": 0.105,
                    "yield_type": "yield_to_maturity",
                    "yield_unit": "decimal",
                    "yield_source": "tasaderendimiento",
                    "face_value": 10.0,
                    "day_counter": "Actual360",
                },
                {
                    "maturity_date": "2026-03-05",
                    "asset_identifier": "M_BONOS_260305",
                    "instrument_type": "fixed_rate_bond",
                    "helper_type": "fixed_rate_bond_helper",
                    "quote": 93.978153,
                    "quote_type": "clean_price",
                    "quote_unit": "price_per_100",
                    "quote_side": "mid",
                    "quote_source": "preciolimpio",
                    "source_quote_type": "clean_price",
                    "yield": 0.0975,
                    "yield_type": "yield_to_maturity",
                    "yield_unit": "decimal",
                    "yield_source": "tasaderendimiento",
                    "dirty_price": 96.837181,
                    "dirty_price_source": "preciosucio",
                    "accrued_interest": 2.859028,
                    "coupon_rate": 0.0575,
                    "coupon_period_days": 182,
                    "face_value": 100.0,
                    "day_counter": "Actual360",
                },
            ],
        )
        self.assertNotIn("curve_unique_identifier", frame.reset_index().columns)
        self.assertNotIn("metadata_json", frame.reset_index().columns)
        self.assertGreaterEqual(len(row["curve"]), 2)

        normalized = DiscountCurvesNode._normalize_builder_frame(
            frame,
            curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
        )
        normalized_nodes = normalized["key_nodes"].iloc[0]
        self.assertEqual(normalized_nodes[0]["helper_type"], "zero_coupon_bond_helper")
        self.assertEqual(normalized_nodes[1]["helper_type"], "fixed_rate_bond_helper")
        self.assertEqual(normalized_nodes[1]["quote_unit"], "price_per_100")
        self.assertIsNone(normalized["metadata_json"].iloc[0])

        validated_nodes = validate_mxn_government_key_nodes(
            row["key_nodes"],
            row=row.to_dict(),
            curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
        )
        self.assertEqual(validated_nodes, row["key_nodes"])

    def test_mxn_government_key_node_validator_rejects_missing_yield(self):
        frame = build_mxn_government_curve_frame(_source_frame())
        row = frame.reset_index().iloc[0]
        bad_nodes = [dict(node) for node in row["key_nodes"]]
        bad_nodes[0].pop("yield")

        with self.assertRaisesRegex(ValueError, "yield"):
            validate_mxn_government_key_nodes(
                bad_nodes,
                row=row.to_dict(),
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
            )

    def test_mxn_government_key_node_validator_rejects_wrong_m_bonos_unit(self):
        frame = build_mxn_government_curve_frame(_source_frame())
        row = frame.reset_index().iloc[0]
        bad_nodes = [dict(node) for node in row["key_nodes"]]
        bad_nodes[1]["quote_unit"] = "price_per_10"

        with self.assertRaisesRegex(ValueError, "price_per_100"):
            validate_mxn_government_key_nodes(
                bad_nodes,
                row=row.to_dict(),
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
            )


if __name__ == "__main__":
    unittest.main()
