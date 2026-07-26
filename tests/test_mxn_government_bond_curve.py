import unittest

import pandas as pd
from msm_pricing.data_nodes import DiscountCurvesNode
from msm_pricing.pricing_engine.curves import parse_bond_helper_key_node

from valmer_connectors.instruments.curve_key_nodes import validate_mxn_government_key_nodes
from valmer_connectors.instruments.mexican_government_bond_curve import (
    CETES_FACE_VALUE,
    M_BONOS_FACE_VALUE,
    MexicanGovernmentBondCurveError,
    build_cetes_zero_coupon_key_node,
    build_m_bono_fixed_rate_key_node,
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

    def test_cetes_key_node_uses_zero_coupon_face_value(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        instrument = build_cetes_zero_coupon_key_node(selected.iloc[0])

        self.assertEqual(instrument.family, "CETES")
        self.assertEqual(instrument.unique_identifier, "BI_CETES_240926")
        self.assertEqual(instrument.quote, 9.929507)
        self.assertEqual(instrument.key_node["face_value"], CETES_FACE_VALUE)
        self.assertEqual(instrument.key_node["quote_unit"], "price_per_face")
        self.assertEqual(
            parse_bond_helper_key_node(instrument.key_node).helper_type,
            "zero_coupon_bond_helper",
        )

    def test_m_bono_key_node_validates_actual_360_accrual(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        instrument = build_m_bono_fixed_rate_key_node(selected.iloc[1])

        self.assertEqual(instrument.family, "M_BONOS")
        self.assertEqual(instrument.unique_identifier, "M_BONOS_260305")
        self.assertEqual(instrument.quote, 93.978153)
        self.assertEqual(instrument.key_node["face_value"], M_BONOS_FACE_VALUE)
        self.assertEqual(instrument.key_node["quote_unit"], "price_per_100")
        self.assertEqual(
            parse_bond_helper_key_node(instrument.key_node).helper_type,
            "fixed_rate_bond_helper",
        )

    def test_m_bono_helper_rejects_bad_clean_dirty_relation(self):
        selected = select_mxn_government_bootstrap_instruments(_source_frame())
        bad = selected.iloc[1].copy()
        bad["preciosucio"] = 95.0

        with self.assertRaisesRegex(
            MexicanGovernmentBondCurveError,
            "clean plus accrued",
        ):
            build_m_bono_fixed_rate_key_node(bad)

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
                    "source_reference": {
                        "type": "asset",
                        "identifier": "BI_CETES_240926",
                    },
                    "instrument_type": "zero_coupon_bond",
                    "helper_type": "zero_coupon_bond_helper",
                    "quote": 9.929507,
                    "quote_type": "clean_price",
                    "quote_unit": "price_per_face",
                    "quote_side": "mid",
                    "quote_source": "preciosucio",
                    "source_quote_type": "dirty_price",
                    "yield": 0.105,
                    "yield_type": "yield_to_maturity",
                    "yield_unit": "decimal",
                    "yield_source": "tasaderendimiento",
                    "face_value": 10.0,
                    "day_counter": "Actual360",
                    "issue_date": "2024-08-30",
                    "settlement_days": 0,
                    "calendar_code": {"name": "Mexico"},
                    "payment_convention": "Following",
                },
                {
                    "maturity_date": "2026-03-05",
                    "source_reference": {
                        "type": "asset",
                        "identifier": "M_BONOS_260305",
                    },
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
                    "issue_date": "2015-09-17",
                    "coupon_rate": 0.0575,
                    "coupon_period_days": 182,
                    "face_value": 100.0,
                    "day_counter": "Actual360",
                    "calendar_code": {"name": "Mexico"},
                    "day_counter_code": "Actual360",
                    "settlement_days": 0,
                    "payment_convention": "Following",
                    "business_day_convention": "Following",
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
        self.assertEqual(normalized_nodes[0]["quote_unit"], "price_per_face")
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
        bad_nodes[1]["quote_unit"] = "price_per_face"

        with self.assertRaisesRegex(ValueError, "price_per_100"):
            validate_mxn_government_key_nodes(
                bad_nodes,
                row=row.to_dict(),
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
            )


if __name__ == "__main__":
    unittest.main()
