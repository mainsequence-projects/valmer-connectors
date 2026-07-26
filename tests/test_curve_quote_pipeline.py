from __future__ import annotations

import unittest
from pathlib import Path

from valmer_connectors.data_nodes.curve_quote_indices import (
    normalize_valmer_mxn_quote_snapshot,
    normalize_valmer_usd_quote_snapshot,
)
from valmer_connectors.instruments.rates_curves import (
    build_tiie_curve_frame_from_quote_snapshot,
    build_usd_mxn_xccy_curve_frame_from_quote_snapshot,
    build_usd_sofr_curve_frame_from_quote_snapshot,
    read_tiie_irs_mxn_csv,
    read_usd_sofr_irs_csv,
)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
VALUATION_DATE = "2026-06-30"


class ValmerCurveQuotePipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mxn_quotes = normalize_valmer_mxn_quote_snapshot(
            read_tiie_irs_mxn_csv((DATA_DIR / "IRS_MXN_CURVE.csv").read_bytes()),
            valuation_date=VALUATION_DATE,
        )
        cls.usd_quotes = normalize_valmer_usd_quote_snapshot(
            read_usd_sofr_irs_csv((DATA_DIR / "IRS_USD_CURVE.csv").read_bytes()),
            valuation_date=VALUATION_DATE,
        )

    def test_complete_sources_publish_all_81_index_observations(self):
        self.assertEqual(len(self.mxn_quotes), 34)
        self.assertEqual(len(self.usd_quotes), 47)
        self.assertEqual(
            self.mxn_quotes.reset_index()["metadata_json"]
            .map(lambda value: value["source_family"])
            .value_counts()
            .to_dict(),
            {
                "tiie_ois": 15,
                "tiie_sofr_xccy_basis": 9,
                "fx_forward": 9,
                "fx_spot": 1,
            },
        )
        self.assertEqual(
            self.usd_quotes.reset_index()["metadata_json"]
            .map(lambda value: value["source_family"])
            .value_counts()
            .to_dict(),
            {
                "sofr_future": 14,
                "fedfunds_sofr_basis": 12,
                "sofr_ois": 11,
                "fedfunds_ois": 10,
            },
        )

    def test_persisted_quote_adapters_build_all_three_curves(self):
        tiie = build_tiie_curve_frame_from_quote_snapshot(
            self.mxn_quotes,
            curve_identifier="VALMER_TIIE_OVERNIGHT",
            valuation_date=VALUATION_DATE,
        )
        sofr = build_usd_sofr_curve_frame_from_quote_snapshot(
            self.usd_quotes,
            curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            valuation_date=VALUATION_DATE,
        )
        xccy = build_usd_mxn_xccy_curve_frame_from_quote_snapshot(
            self.mxn_quotes,
            tiie_curve_key_nodes=tiie.iloc[0]["key_nodes"],
            usd_sofr_curve_key_nodes=sofr.iloc[0]["key_nodes"],
            curve_identifier="VALMER_MXN_USD_COLLATERAL_DISCOUNT",
            valuation_date=VALUATION_DATE,
        )

        self.assertEqual(len(tiie.iloc[0]["key_nodes"]), 15)
        self.assertEqual(len(sofr.iloc[0]["key_nodes"]), 24)
        self.assertEqual(len(xccy.iloc[0]["key_nodes"]), 17)
        self._assert_key_nodes_reconcile(tiie, self.mxn_quotes)
        self._assert_key_nodes_reconcile(sofr, self.usd_quotes)
        self._assert_key_nodes_reconcile(xccy, self.mxn_quotes)

    def test_fed_funds_quotes_persist_but_are_not_sofr_helpers(self):
        sofr = build_usd_sofr_curve_frame_from_quote_snapshot(
            self.usd_quotes,
            curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            valuation_date=VALUATION_DATE,
        )
        persisted_families = {
            value["source_family"]
            for value in self.usd_quotes.reset_index()["metadata_json"]
        }
        helper_sources = {
            node["source_instrument_identifier"] for node in sofr.iloc[0]["key_nodes"]
        }

        self.assertIn("fedfunds_ois", persisted_families)
        self.assertIn("fedfunds_sofr_basis", persisted_families)
        self.assertFalse(any("FEDFUNDS" in identifier for identifier in helper_sources))

    def _assert_key_nodes_reconcile(self, curve_frame, quote_frame):
        observations = {
            row.index_identifier: row
            for row in quote_frame.reset_index().itertuples(index=False)
        }
        for node in curve_frame.iloc[0]["key_nodes"]:
            source_reference = node["source_reference"]
            self.assertEqual(source_reference["type"], "index")
            observation = observations[source_reference["identifier"]]
            self.assertAlmostEqual(float(node["quote"]), float(observation.value))
            self.assertEqual(node["quote_unit"], observation.unit)
            self.assertEqual(
                node["source_instrument_identifier"],
                observation.metadata_json["source_instrument_identifier"],
            )


if __name__ == "__main__":
    unittest.main()
