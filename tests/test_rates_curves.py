import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

import pandas as pd
from msm_pricing.data_nodes import DiscountCurvesNode

from valmer_connectors.instruments.curve_key_nodes import (
    validate_tiie_ois_key_nodes,
    validate_usd_sofr_key_nodes,
)
from valmer_connectors.instruments.rates_curves import (
    VALMER_BENCHMARK_PAGE_URL,
    VALMER_TIIE_IRS_MXN_URL,
    VALMER_USD_SOFR_IRS_URL,
    ValmerTiieCurveError,
    ValmerUsdSofrCurveError,
    build_tiie_irs_mxn_curve_frame,
    build_tiie_irs_mxn_valmer,
    build_usd_sofr_curve_frame,
    build_usd_sofr_valmer,
    classify_tiie_irs_mxn_row,
    classify_usd_sofr_irs_row,
    parse_valmer_benchmark_page_date,
    read_tiie_irs_mxn_csv,
    read_usd_sofr_irs_csv,
)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


IRS_MXN_SAMPLE = (
    b"FX.USD.MXN.ON,14.000000000000\n"
    b"FX.USD.MXN.TN,14.500000000000\n"
    b"FX.USD.MXN,17.468550000000\n"
    b"Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR,0.15550000\n"
    b"Swap.28D.MXN.FTIIE.1D/28D.BANXICO,6.52560000\n"
    b"Swap.8W.MXN.FTIIE.1D/28D.BANXICO,6.53362500\n"
    b"Swap.12W.MXN.FTIIE.1D/28D.BANXICO,6.54500000\n"
    b"Swap.24W.MXN.FTIIE.1D/28D.BANXICO,6.58290000\n"
    b"Swap.36W.MXN.FTIIE.1D/28D.BANXICO,6.63500000\n"
    b"Swap.52W.MXN.FTIIE.1D/28D.BANXICO,6.71885000\n"
    b"Swap.104W.MXN.FTIIE.1D/28D.BANXICO,7.06000000\n"
    b"Swap.156W.MXN.FTIIE.1D/28D.BANXICO,7.30000000\n"
    b"Swap.208W.MXN.FTIIE.1D/28D.BANXICO,7.47000000\n"
    b"Swap.260W.MXN.FTIIE.1D/28D.BANXICO,7.59900000\n"
    b"Swap.364W.MXN.FTIIE.1D/28D.BANXICO,7.80000000\n"
    b"Swap.520W.MXN.FTIIE.1D/28D.BANXICO,8.02500000\n"
    b"Swap.182M.MXN.FTIIE.1D/28D.BANXICO,8.26400000\n"
    b"Swap.1040W.MXN.FTIIE.1D/28D.BANXICO,8.35500000\n"
    b"Swap.364M.MXN.FTIIE.1D/28D.BANXICO,8.33190000\n"
)

BENCHMARK_PAGE_RESPONSE = b"""
<table class="data-2" id="tablaMismoDia" style="display: table;">
  <caption style="display: table-caption;">
    Fecha <span class="lbFechaIndice">30/06/2026</span>
  </caption>
  <tbody>
    <tr>
      <th class="col-01">name</th>
      <th class="col-02">index</th>
    </tr>
  </tbody>
</table>
"""


class ValmerRatesCurvesTests(unittest.TestCase):
    def test_classifies_irs_mxn_source_families(self):
        self.assertEqual(classify_tiie_irs_mxn_row("FX.USD.MXN.ON"), "fx")
        self.assertEqual(
            classify_tiie_irs_mxn_row("Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR"),
            "cross_currency",
        )
        self.assertEqual(
            classify_tiie_irs_mxn_row("Swap.28D.MXN.FTIIE.1D/28D.BANXICO"),
            "domestic_ois",
        )
        self.assertEqual(classify_tiie_irs_mxn_row("Swap.BAD"), "unsupported")

    def test_classifies_irs_usd_source_families(self):
        self.assertEqual(
            classify_usd_sofr_irs_row("Future.USD.CME.CME SR1 EOM.JUL.26"),
            "sofr_future",
        )
        self.assertEqual(
            classify_usd_sofr_irs_row("Future.USD.CME.CME SR3 IMM.SEP.26"),
            "sofr_future",
        )
        self.assertEqual(
            classify_usd_sofr_irs_row("Swap.10Y.USD.SOFR.1D/1Y.SOFR"),
            "sofr_ois",
        )
        self.assertEqual(
            classify_usd_sofr_irs_row("Swap.1Y.USD.FEDFUNDS.1D/1Y.FEDFUNDS1"),
            "fedfunds_ois",
        )
        self.assertEqual(
            classify_usd_sofr_irs_row("Swap.USD.10Y.FEDFUNDS.1D/SOFR.1D.SOFR"),
            "fedfunds_sofr_basis",
        )
        self.assertEqual(classify_usd_sofr_irs_row("Swap.BAD"), "unsupported")

    def test_parse_valmer_benchmark_page_date_selects_same_day_table(self):
        parsed = parse_valmer_benchmark_page_date(BENCHMARK_PAGE_RESPONSE)

        self.assertEqual(parsed, pd.Timestamp("2026-06-30", tz="UTC"))

    def test_read_tiie_irs_mxn_csv_is_two_column_source_shape(self):
        frame = read_tiie_irs_mxn_csv(IRS_MXN_SAMPLE)

        self.assertEqual(
            list(frame.columns),
            ["instrument_identifier", "quote"],
        )
        self.assertEqual(frame.iloc[0]["instrument_identifier"], "FX.USD.MXN.ON")

    def test_read_usd_sofr_irs_csv_is_two_column_local_source_shape(self):
        frame = read_usd_sofr_irs_csv((DATA_DIR / "IRS_USD_CURVE.csv").read_bytes())

        self.assertEqual(
            list(frame.columns),
            ["instrument_identifier", "quote"],
        )
        self.assertEqual(
            frame.iloc[0]["instrument_identifier"],
            "Future.USD.CME.CME SR1 EOM.JUL.26",
        )
        self.assertEqual(len(frame), 47)

    def test_build_tiie_irs_mxn_curve_frame_bootstraps_ois_helpers(self):
        frame = build_tiie_irs_mxn_curve_frame(
            IRS_MXN_SAMPLE,
            curve_identifier="VALMER_TIIE_OVERNIGHT",
            valuation_date="2026-06-30",
        )
        row = frame.reset_index().iloc[0]

        self.assertEqual(frame.index.names, ["time_index", "curve_identifier"])
        self.assertEqual(row["time_index"], pd.Timestamp("2026-06-30", tz="UTC"))
        self.assertEqual(row["curve_identifier"], "VALMER_TIIE_OVERNIGHT")
        self.assertIn(1, row["curve"])
        self.assertIn(29, row["curve"])
        self.assertGreater(row["curve"][1], 0)
        self.assertGreater(row["curve"][29], 0)
        self.assertEqual(len(row["key_nodes"]), 15)
        self.assertNotIn("metadata_json", frame.reset_index().columns)

        first_node = row["key_nodes"][0]
        self.assertEqual(
            first_node["asset_identifier"],
            "Swap.28D.MXN.FTIIE.1D/28D.BANXICO",
        )
        self.assertEqual(first_node["instrument_type"], "overnight_indexed_swap")
        self.assertEqual(first_node["helper_type"], "ois_rate_helper")
        self.assertEqual(first_node["quote"], 0.065256)
        self.assertEqual(first_node["quote_type"], "par_swap_rate")
        self.assertEqual(first_node["quote_unit"], "decimal")
        self.assertEqual(first_node["source_quote"], 6.5256)
        self.assertEqual(first_node["source_quote_unit"], "percent")
        self.assertEqual(first_node["tenor"], "28D")
        self.assertEqual(first_node["floating_index"], "TIIE_OVERNIGHT")
        self.assertEqual(first_node["earliest_date"], "2026-07-01")
        self.assertEqual(first_node["maturity_date"], "2026-07-29")
        self.assertEqual(first_node["pillar_date"], "2026-07-29")

        identifiers = {node["asset_identifier"] for node in row["key_nodes"]}
        self.assertNotIn("FX.USD.MXN.ON", identifiers)
        self.assertNotIn("Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR", identifiers)

        normalized = DiscountCurvesNode._normalize_builder_frame(
            frame,
            curve_identifier="VALMER_TIIE_OVERNIGHT",
        )
        normalized_node = normalized["key_nodes"].iloc[0][0]
        self.assertEqual(normalized_node["helper_type"], "ois_rate_helper")
        self.assertEqual(normalized_node["quote_type"], "par_swap_rate")
        self.assertIsNone(normalized["metadata_json"].iloc[0])

        validated_nodes = validate_tiie_ois_key_nodes(
            row["key_nodes"],
            row=row.to_dict(),
            curve_identifier="VALMER_TIIE_OVERNIGHT",
        )
        self.assertEqual(validated_nodes, row["key_nodes"])

    def test_tiie_key_node_validator_rejects_non_ois_source_family(self):
        frame = build_tiie_irs_mxn_curve_frame(
            IRS_MXN_SAMPLE,
            curve_identifier="VALMER_TIIE_OVERNIGHT",
            valuation_date="2026-06-30",
        )
        row = frame.reset_index().iloc[0]
        bad_nodes = [dict(row["key_nodes"][0])]
        bad_nodes[0]["asset_identifier"] = "Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR"

        with self.assertRaisesRegex(ValueError, "FTIIE.1D/28D.BANXICO"):
            validate_tiie_ois_key_nodes(
                bad_nodes,
                row=row.to_dict(),
                curve_identifier="VALMER_TIIE_OVERNIGHT",
            )

    def test_build_usd_sofr_curve_frame_bootstraps_local_fixture(self):
        frame = build_usd_sofr_curve_frame(
            (DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
            curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            valuation_date="2026-06-30",
        )
        row = frame.reset_index().iloc[0]

        self.assertEqual(frame.index.names, ["time_index", "curve_identifier"])
        self.assertEqual(row["time_index"], pd.Timestamp("2026-06-30", tz="UTC"))
        self.assertEqual(row["curve_identifier"], "VALMER_USD_SOFR_OVERNIGHT")
        self.assertIn(1, row["curve"])
        self.assertTrue(all(days > 0 for days in row["curve"]))
        self.assertGreater(row["curve"][1], 0)
        self.assertEqual(len(row["key_nodes"]), 24)
        self.assertNotIn("metadata_json", frame.reset_index().columns)

        first_node = row["key_nodes"][0]
        self.assertEqual(
            first_node["asset_identifier"],
            "Future.USD.CME.CME SR1 EOM.JUL.26",
        )
        self.assertEqual(first_node["instrument_type"], "sofr_future")
        self.assertEqual(first_node["helper_type"], "sofr_future_rate_helper")
        self.assertEqual(first_node["quote"], 96.355)
        self.assertEqual(first_node["quote_type"], "futures_price")
        self.assertEqual(first_node["quote_unit"], "price")
        self.assertAlmostEqual(first_node["implied_rate"], 0.03645)
        self.assertEqual(first_node["contract_code"], "SR1")
        self.assertEqual(first_node["reference_frequency"], "Monthly")
        self.assertEqual(first_node["earliest_date"], "2026-07-01")
        self.assertEqual(first_node["maturity_date"], "2026-08-01")
        self.assertEqual(first_node["pillar_date"], "2026-08-01")

        swap_node = next(
            node
            for node in row["key_nodes"]
            if node["asset_identifier"] == "Swap.10Y.USD.SOFR.1D/1Y.SOFR"
        )
        self.assertEqual(swap_node["instrument_type"], "overnight_indexed_swap")
        self.assertEqual(swap_node["helper_type"], "ois_rate_helper")
        self.assertEqual(swap_node["quote"], 0.0401375)
        self.assertEqual(swap_node["source_quote"], 4.01375)
        self.assertEqual(swap_node["tenor"], "10Y")
        self.assertEqual(swap_node["floating_index"], "USD_SOFR_OVERNIGHT")
        self.assertEqual(swap_node["fixed_payment_frequency"], "Annual")
        self.assertEqual(swap_node["earliest_date"], "2026-07-02")
        self.assertEqual(swap_node["maturity_date"], "2036-07-02")

        identifiers = {node["asset_identifier"] for node in row["key_nodes"]}
        self.assertNotIn("Future.USD.CME.CME SR3 IMM.JUN.26", identifiers)
        self.assertNotIn("Swap.1Y.USD.FEDFUNDS.1D/1Y.FEDFUNDS1", identifiers)
        self.assertNotIn("Swap.USD.10Y.FEDFUNDS.1D/SOFR.1D.SOFR", identifiers)

        normalized = DiscountCurvesNode._normalize_builder_frame(
            frame,
            curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
        )
        normalized_node = normalized["key_nodes"].iloc[0][0]
        self.assertEqual(normalized_node["helper_type"], "sofr_future_rate_helper")
        self.assertEqual(normalized_node["quote_type"], "futures_price")
        self.assertIsNone(normalized["metadata_json"].iloc[0])

        validated_nodes = validate_usd_sofr_key_nodes(
            row["key_nodes"],
            row=row.to_dict(),
            curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
        )
        self.assertEqual(validated_nodes, row["key_nodes"])

    def test_usd_sofr_key_node_validator_rejects_fed_funds_rows(self):
        frame = build_usd_sofr_curve_frame(
            (DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
            curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            valuation_date="2026-06-30",
        )
        row = frame.reset_index().iloc[0]
        bad_nodes = [dict(node) for node in row["key_nodes"]]
        bad_nodes[0]["asset_identifier"] = "Swap.1Y.USD.FEDFUNDS.1D/1Y.FEDFUNDS1"

        with self.assertRaisesRegex(ValueError, "Fed Funds"):
            validate_usd_sofr_key_nodes(
                bad_nodes,
                row=row.to_dict(),
                curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            )

    def test_build_tiie_irs_mxn_curve_requires_explicit_valuation_date(self):
        with self.assertRaisesRegex(ValmerTiieCurveError, "valuation-date"):
            build_tiie_irs_mxn_curve_frame(
                IRS_MXN_SAMPLE,
                curve_identifier="VALMER_TIIE_OVERNIGHT",
            )

    def test_build_usd_sofr_curve_requires_explicit_valuation_date(self):
        with self.assertRaisesRegex(ValmerUsdSofrCurveError, "valuation-date"):
            build_usd_sofr_curve_frame(
                (DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
                curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            )

    def test_build_tiie_irs_mxn_curve_rejects_missing_domestic_ois_rows(self):
        with self.assertRaisesRegex(ValmerTiieCurveError, "no domestic FTIIE OIS"):
            build_tiie_irs_mxn_curve_frame(
                b"FX.USD.MXN.ON,14.0\n",
                curve_identifier="VALMER_TIIE_OVERNIGHT",
                valuation_date="2026-06-30",
            )

    def test_build_usd_sofr_curve_rejects_missing_sofr_ois_rows(self):
        with self.assertRaisesRegex(ValmerUsdSofrCurveError, "no SOFR OIS"):
            build_usd_sofr_curve_frame(
                b"Future.USD.CME.CME SR1 EOM.JUL.26,96.355000\n",
                curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
                valuation_date="2026-06-30",
            )

    def test_valmer_tiie_update_skips_csv_download_when_source_date_is_not_newer(self):
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-06-30",
            tz="UTC",
        )
        date_response = Mock(content=BENCHMARK_PAGE_RESPONSE)

        with (
            patch("valmer_connectors.instruments.rates_curves.requests.get") as get,
        ):
            get.return_value = date_response
            frame = build_tiie_irs_mxn_valmer(
                update_statistics=update_statistics,
                curve_identifier="VALMER_TIIE_OVERNIGHT",
                base_node_curve_points=None,
            )

        get.assert_called_once_with(VALMER_BENCHMARK_PAGE_URL, timeout=30)
        date_response.raise_for_status.assert_called_once_with()
        update_statistics.get_last_update_for_identity.assert_called_once_with(
            "VALMER_TIIE_OVERNIGHT"
        )
        self.assertTrue(frame.empty)
        self.assertEqual(frame.index.names, ["time_index", "curve_identifier"])
        self.assertIn("curve", frame.reset_index().columns)
        self.assertIn("key_nodes", frame.reset_index().columns)

    def test_valmer_usd_sofr_update_skips_csv_download_when_source_date_is_not_newer(self):
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-06-30",
            tz="UTC",
        )
        date_response = Mock(content=BENCHMARK_PAGE_RESPONSE)

        with (
            patch("valmer_connectors.instruments.rates_curves.requests.get") as get,
        ):
            get.return_value = date_response
            frame = build_usd_sofr_valmer(
                update_statistics=update_statistics,
                curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
                base_node_curve_points=None,
            )

        get.assert_called_once_with(VALMER_BENCHMARK_PAGE_URL, timeout=30)
        date_response.raise_for_status.assert_called_once_with()
        update_statistics.get_last_update_for_identity.assert_called_once_with(
            "VALMER_USD_SOFR_OVERNIGHT"
        )
        self.assertTrue(frame.empty)
        self.assertEqual(frame.index.names, ["time_index", "curve_identifier"])
        self.assertIn("curve", frame.reset_index().columns)
        self.assertIn("key_nodes", frame.reset_index().columns)

    def test_valmer_tiie_update_downloads_csv_when_source_date_is_newer(self):
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-06-29",
            tz="UTC",
        )
        date_response = Mock(content=BENCHMARK_PAGE_RESPONSE)
        curve_response = Mock(content=IRS_MXN_SAMPLE)

        with (
            patch(
                "valmer_connectors.instruments.rates_curves.requests.get",
                side_effect=[date_response, curve_response],
            ) as get,
        ):
            frame = build_tiie_irs_mxn_valmer(
                update_statistics=update_statistics,
                curve_identifier="VALMER_TIIE_OVERNIGHT",
                base_node_curve_points=None,
            )

        get.assert_has_calls(
            [
                call(VALMER_BENCHMARK_PAGE_URL, timeout=30),
                call(VALMER_TIIE_IRS_MXN_URL, timeout=30),
            ]
        )
        date_response.raise_for_status.assert_called_once_with()
        curve_response.raise_for_status.assert_called_once_with()
        row = frame.reset_index().iloc[0]
        self.assertEqual(row["time_index"], pd.Timestamp("2026-06-30", tz="UTC"))
        self.assertEqual(row["curve_identifier"], "VALMER_TIIE_OVERNIGHT")

    def test_valmer_usd_sofr_update_downloads_csv_when_source_date_is_newer(self):
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-06-29",
            tz="UTC",
        )
        date_response = Mock(content=BENCHMARK_PAGE_RESPONSE)
        curve_response = Mock(content=(DATA_DIR / "IRS_USD_CURVE.csv").read_bytes())

        with (
            patch(
                "valmer_connectors.instruments.rates_curves.requests.get",
                side_effect=[date_response, curve_response],
            ) as get,
        ):
            frame = build_usd_sofr_valmer(
                update_statistics=update_statistics,
                curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
                base_node_curve_points=None,
            )

        get.assert_has_calls(
            [
                call(VALMER_BENCHMARK_PAGE_URL, timeout=30),
                call(VALMER_USD_SOFR_IRS_URL, timeout=30),
            ]
        )
        date_response.raise_for_status.assert_called_once_with()
        curve_response.raise_for_status.assert_called_once_with()
        row = frame.reset_index().iloc[0]
        self.assertEqual(row["time_index"], pd.Timestamp("2026-06-30", tz="UTC"))
        self.assertEqual(row["curve_identifier"], "VALMER_USD_SOFR_OVERNIGHT")


if __name__ == "__main__":
    unittest.main()
