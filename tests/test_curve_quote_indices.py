from __future__ import annotations

import unittest

from msm_pricing.data_nodes.curves.key_nodes import (
    compress_key_nodes_to_string,
    decompress_key_nodes_from_string,
)

from valmer_connectors.instruments.curve_quote_indices import (
    ValmerCurveQuoteIndexIdentifierError,
    curve_quote_index_identifier_from_source,
)


class CurveQuoteIndexIdentifierTests(unittest.TestCase):
    def test_maps_every_persisted_valmer_quote_family_to_an_index(self):
        expected = {
            "FX.USD.MXN": "VALMER_CURVE_QUOTE.USDMXN_SPOT.MID",
            "FX.USD.MXN.1W": "VALMER_CURVE_QUOTE.USDMXN_FORWARD.1W.MID",
            "FX.USD.MXN.ON": "VALMER_CURVE_QUOTE.USDMXN_FORWARD.ON.MID",
            "Swap.28D.MXN.FTIIE.1D/28D.BANXICO": (
                "VALMER_CURVE_QUOTE.TIIE_OIS.28D.MID"
            ),
            "Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR": (
                "VALMER_CURVE_QUOTE.TIIE_SOFR_XCCY_BASIS.104W.MID"
            ),
            "Future.USD.CME.CME SR1 EOM.JUL.26": (
                "VALMER_CURVE_QUOTE.SOFR_FUTURE.SR1.2026_07.EOM.MID"
            ),
            "Future.USD.CME.CME SR3 IMM.SEP.27": (
                "VALMER_CURVE_QUOTE.SOFR_FUTURE.SR3.2027_09.IMM.MID"
            ),
            "Swap.10Y.USD.SOFR.1D/1Y.SOFR": (
                "VALMER_CURVE_QUOTE.SOFR_OIS.10Y.MID"
            ),
            "Swap.1Y.USD.FEDFUNDS.1D/1Y.FEDFUNDS1": (
                "VALMER_CURVE_QUOTE.FEDFUNDS_OIS.1Y.MID"
            ),
            "Swap.USD.10Y.FEDFUNDS.1D/SOFR.1D.SOFR": (
                "VALMER_CURVE_QUOTE.FEDFUNDS_SOFR_BASIS.10Y.MID"
            ),
        }

        for source_identifier, expected_identifier in expected.items():
            with self.subTest(source_identifier=source_identifier):
                self.assertEqual(
                    curve_quote_index_identifier_from_source(source_identifier),
                    expected_identifier,
                )
        self.assertEqual(len(set(expected.values())), len(expected))

    def test_rejects_unrecognized_source_family(self):
        with self.assertRaisesRegex(
            ValmerCurveQuoteIndexIdentifierError,
            "Unsupported Valmer curve quote source",
        ):
            curve_quote_index_identifier_from_source("Unknown.USD.1Y")

    def test_rejects_invalid_tenor(self):
        with self.assertRaisesRegex(
            ValmerCurveQuoteIndexIdentifierError,
            "tenor",
        ):
            curve_quote_index_identifier_from_source("FX.USD.MXN.BAD")

    def test_strict_contract_round_trips_only_typed_source_references(self):
        payload = [
            {
                "source_reference": {
                    "type": "index",
                    "identifier": "VALMER_CURVE_QUOTE.TIIE_OIS.28D.MID",
                },
                "quote": 0.065,
            },
            {
                "source_reference": {
                    "type": "asset",
                    "identifier": "BI_CETES_240926",
                },
                "quote": 9.9,
            },
        ]

        encoded = compress_key_nodes_to_string(payload)

        self.assertEqual(decompress_key_nodes_from_string(encoded), payload)

    def test_strict_contract_rejects_removed_top_level_identity_fields(self):
        for removed_field in ("asset_identifier", "index_identifier"):
            with self.subTest(removed_field=removed_field):
                with self.assertRaisesRegex(
                    ValueError,
                    "unsupported top-level source fields",
                ):
                    compress_key_nodes_to_string(
                        [{removed_field: "REMOVED", "quote": 1.0}]
                    )


if __name__ == "__main__":
    unittest.main()
