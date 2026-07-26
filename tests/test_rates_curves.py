import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from msm_pricing.data_nodes import DiscountCurvesNode
from msm_pricing.pricing_engine.curves import (
    StaticRateHelperRuntimeResolver,
    helper_specs_from_key_nodes,
    parse_cross_currency_key_node,
)

from valmer_connectors.instruments import rates_curves as rates_curves_module
from valmer_connectors.instruments.curve_key_nodes import (
    validate_tiie_ois_key_nodes,
    validate_usd_mxn_xccy_key_nodes,
    validate_usd_sofr_key_nodes,
)
from valmer_connectors.instruments.rates_curves import (
    VALMER_BENCHMARK_DATE_URL,
    VALMER_BENCHMARK_PAGE_URL,
    ValmerTiieCurveError,
    ValmerUsdMxnXccyCurveError,
    ValmerUsdSofrCurveError,
    build_tiie_irs_mxn_curve_frame,
    build_usd_mxn_xccy_curve_frame,
    build_usd_sofr_curve_frame,
    classify_tiie_irs_mxn_row,
    classify_usd_sofr_irs_row,
    fetch_valmer_benchmark_date_content,
    parse_valmer_benchmark_date,
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

BENCHMARK_DATE_RESPONSE = b"""for(;;);({
  "exito": "true",
  "respuesta": [
    {"nombre": "Vector_Gubernamental", "fecha": "30/06/2026"},
    {"nombre": "Indices_Benchmarks", "descripcion": "Indices y Benchmarks", "fecha": "30/06/2026"}
  ]
})"""


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

    def test_parse_valmer_benchmark_date_selects_indices_benchmarks(self):
        parsed = parse_valmer_benchmark_date(BENCHMARK_DATE_RESPONSE)

        self.assertEqual(parsed, pd.Timestamp("2026-06-30", tz="UTC"))

    def test_fetch_valmer_benchmark_date_content_uses_homepage_ajax_flow(self):
        page_response = Mock()
        date_response = Mock(content=BENCHMARK_DATE_RESPONSE)
        session = Mock()
        session.get.return_value = page_response
        session.post.return_value = date_response

        with patch(
            "valmer_connectors.instruments.rates_curves.requests.Session",
            return_value=session,
        ) as session_factory:
            content = fetch_valmer_benchmark_date_content()

        session_factory.assert_called_once_with()
        session.headers.update.assert_called_once()
        session.get.assert_called_once_with(VALMER_BENCHMARK_PAGE_URL, timeout=30)
        session.post.assert_called_once_with(
            VALMER_BENCHMARK_DATE_URL,
            data={"rand": "0"},
            timeout=30,
        )
        page_response.raise_for_status.assert_called_once_with()
        date_response.raise_for_status.assert_called_once_with()
        self.assertEqual(content, BENCHMARK_DATE_RESPONSE)

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
            first_node["source_instrument_identifier"],
            "Swap.28D.MXN.FTIIE.1D/28D.BANXICO",
        )
        self.assertEqual(
            first_node["source_reference"],
            {
                "type": "index",
                "identifier": "VALMER_CURVE_QUOTE.TIIE_OIS.28D.MID",
            },
        )
        self.assertEqual(
            first_node["source_observation_time"],
            "2026-06-30T00:00:00+00:00",
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
        self.assertEqual(first_node["settlement_days"], 1)
        self.assertEqual(first_node["payment_convention"], "ModifiedFollowing")
        self.assertEqual(first_node["payment_frequency"], "EveryFourthWeek")
        self.assertEqual(first_node["payment_calendar_code"], {"name": "Mexico"})
        self.assertEqual(first_node["averaging_method"], "Compound")
        self.assertFalse(first_node["end_of_month"])
        self.assertEqual(first_node["fixed_payment_frequency"], "EveryFourthWeek")
        self.assertEqual(first_node["fixed_calendar_code"], {"name": "Mexico"})
        self.assertEqual(first_node["day_counter"], "Actual360")
        self.assertEqual(first_node["day_counter_code"], "Actual360")
        self.assertEqual(first_node["earliest_date"], "2026-07-01")
        self.assertEqual(first_node["maturity_date"], "2026-07-29")
        self.assertEqual(first_node["pillar_date"], "2026-07-29")

        identifiers = {
            node["source_instrument_identifier"] for node in row["key_nodes"]
        }
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
        bad_nodes[0]["source_instrument_identifier"] = (
            "Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR"
        )

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
            first_node["source_instrument_identifier"],
            "Future.USD.CME.CME SR1 EOM.JUL.26",
        )
        self.assertEqual(
            first_node["source_reference"],
            {
                "type": "index",
                "identifier": "VALMER_CURVE_QUOTE.SOFR_FUTURE.SR1.2026_07.EOM.MID",
            },
        )
        self.assertEqual(
            first_node["source_observation_time"],
            "2026-06-30T00:00:00+00:00",
        )
        self.assertEqual(first_node["instrument_type"], "sofr_future")
        self.assertEqual(first_node["helper_type"], "sofr_future_rate_helper")
        self.assertEqual(first_node["quote"], 96.355)
        self.assertEqual(first_node["quote_type"], "futures_price")
        self.assertEqual(first_node["quote_unit"], "price")
        self.assertAlmostEqual(first_node["implied_rate"], 0.03645)
        self.assertEqual(first_node["contract_code"], "SR1")
        self.assertEqual(first_node["reference_frequency"], "Monthly")
        self.assertEqual(first_node["future_family"], "sofr")
        self.assertEqual(first_node["convexity_adjustment"], 0.0)
        self.assertEqual(first_node["earliest_date"], "2026-07-01")
        self.assertEqual(first_node["maturity_date"], "2026-08-01")
        self.assertEqual(first_node["pillar_date"], "2026-08-01")

        swap_node = next(
            node
            for node in row["key_nodes"]
            if node["source_instrument_identifier"]
            == "Swap.10Y.USD.SOFR.1D/1Y.SOFR"
        )
        self.assertEqual(swap_node["instrument_type"], "overnight_indexed_swap")
        self.assertEqual(swap_node["helper_type"], "ois_rate_helper")
        self.assertEqual(swap_node["quote"], 0.0401375)
        self.assertEqual(swap_node["source_quote"], 4.01375)
        self.assertEqual(swap_node["tenor"], "10Y")
        self.assertEqual(swap_node["floating_index"], "USD_SOFR_OVERNIGHT")
        self.assertEqual(swap_node["settlement_days"], 2)
        self.assertEqual(swap_node["payment_convention"], "ModifiedFollowing")
        self.assertEqual(swap_node["payment_frequency"], "Annual")
        self.assertEqual(
            swap_node["payment_calendar_code"],
            {"name": "UnitedStates", "market": 6},
        )
        self.assertEqual(swap_node["averaging_method"], "Compound")
        self.assertFalse(swap_node["end_of_month"])
        self.assertEqual(swap_node["fixed_payment_frequency"], "Annual")
        self.assertEqual(
            swap_node["fixed_calendar_code"],
            {"name": "UnitedStates", "market": 6},
        )
        self.assertEqual(swap_node["day_counter"], "Actual360")
        self.assertEqual(swap_node["day_counter_code"], "Actual360")
        self.assertEqual(swap_node["earliest_date"], "2026-07-02")
        self.assertEqual(swap_node["maturity_date"], "2036-07-02")

        identifiers = {
            node["source_instrument_identifier"] for node in row["key_nodes"]
        }
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
        bad_nodes[0]["source_instrument_identifier"] = (
            "Swap.1Y.USD.FEDFUNDS.1D/1Y.FEDFUNDS1"
        )

        with self.assertRaisesRegex(ValueError, "Fed Funds"):
            validate_usd_sofr_key_nodes(
                bad_nodes,
                row=row.to_dict(),
                curve_identifier="VALMER_USD_SOFR_OVERNIGHT",
            )

    def test_build_usd_mxn_xccy_curve_bootstraps_local_fixture(self):
        frame = build_usd_mxn_xccy_curve_frame(
            (DATA_DIR / "IRS_MXN_CURVE.csv").read_bytes(),
            usd_sofr_curve_content=(DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
            curve_identifier="VALMER_MXN_USD_COLLATERAL_DISCOUNT",
            valuation_date="2026-06-30",
        )
        row = frame.reset_index().iloc[0]

        self.assertEqual(frame.index.names, ["time_index", "curve_identifier"])
        self.assertEqual(row["time_index"], pd.Timestamp("2026-06-30", tz="UTC"))
        self.assertEqual(
            row["curve_identifier"],
            "VALMER_MXN_USD_COLLATERAL_DISCOUNT",
        )
        self.assertIn(1, row["curve"])
        self.assertTrue(all(days > 0 for days in row["curve"]))
        self.assertEqual(len(row["key_nodes"]), 17)

        spot_node = row["key_nodes"][0]
        self.assertEqual(spot_node["instrument_type"], "fx_spot")
        self.assertEqual(spot_node["source_instrument_identifier"], "FX.USD.MXN")
        self.assertEqual(
            spot_node["source_reference"],
            {
                "type": "index",
                "identifier": "VALMER_CURVE_QUOTE.USDMXN_SPOT.MID",
            },
        )
        self.assertEqual(
            spot_node["source_observation_time"],
            "2026-06-30T00:00:00+00:00",
        )
        self.assertEqual(spot_node["quote"], 17.46855)
        self.assertEqual(spot_node["fx_pair"], "USD/MXN")

        fx_nodes = [
            node for node in row["key_nodes"] if node["instrument_type"] == "fx_swap"
        ]
        ccs_nodes = [
            node
            for node in row["key_nodes"]
            if node["instrument_type"] == "cross_currency_basis_swap"
        ]
        self.assertEqual(len(fx_nodes), 7)
        self.assertEqual(len(ccs_nodes), 9)
        self.assertEqual(fx_nodes[0]["source_quote"], 99.0)
        self.assertEqual(fx_nodes[0]["quote"], 0.0099)
        self.assertEqual(fx_nodes[0]["point_scale"], 10000)
        self.assertTrue(fx_nodes[0]["is_fx_base_currency_collateral_currency"])
        self.assertEqual(ccs_nodes[0]["source_quote"], 0.1555)
        self.assertEqual(ccs_nodes[0]["quote"], 0.001555)
        self.assertEqual(ccs_nodes[0]["basis_side"], "USD_SOFR")
        self.assertEqual(ccs_nodes[0]["notional_style"], "constant_notional")
        self.assertTrue(ccs_nodes[0]["is_basis_on_fx_base_currency_leg"])
        self.assertEqual(ccs_nodes[-3]["source_tenor"], "182M")
        self.assertEqual(ccs_nodes[-3]["tenor"], "15Y")
        self.assertEqual(ccs_nodes[-1]["source_tenor"], "364M")
        self.assertEqual(ccs_nodes[-1]["tenor"], "30Y")
        self.assertLess(max(abs(node["quote_error"]) for node in fx_nodes + ccs_nodes), 1e-8)
        for node in row["key_nodes"]:
            self.assertIsNotNone(parse_cross_currency_key_node(node))

        valuation_ts = pd.Timestamp("2026-06-30", tz="UTC")
        tiie_projection_curve = rates_curves_module._build_tiie_projection_curve_from_source(
            (DATA_DIR / "IRS_MXN_CURVE.csv").read_bytes(),
            valuation_ts=valuation_ts,
        )
        usd_sofr_curve = rates_curves_module._build_usd_sofr_projection_curve_from_source(
            (DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
            valuation_ts=valuation_ts,
        )
        runtime_resolver = rates_curves_module._build_usd_mxn_xccy_runtime_resolver(
            tiie_projection_curve=tiie_projection_curve,
            usd_sofr_curve=usd_sofr_curve,
        )
        helper_specs = helper_specs_from_key_nodes(
            row["key_nodes"],
            helper_runtime_resolver=runtime_resolver,
        )
        self.assertEqual(len(helper_specs), 16)

        validated_nodes = validate_usd_mxn_xccy_key_nodes(
            row["key_nodes"],
            row=row.to_dict(),
            curve_identifier="VALMER_MXN_USD_COLLATERAL_DISCOUNT",
        )
        self.assertEqual(validated_nodes, row["key_nodes"])

    def test_usd_mxn_xccy_builder_uses_ms_markets_reconstruction(self):
        with patch.object(
            rates_curves_module,
            "reconstruct_curve_result_from_key_nodes",
            wraps=rates_curves_module.reconstruct_curve_result_from_key_nodes,
        ) as reconstruction:
            build_usd_mxn_xccy_curve_frame(
                (DATA_DIR / "IRS_MXN_CURVE.csv").read_bytes(),
                usd_sofr_curve_content=(DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
                curve_identifier="VALMER_MXN_USD_COLLATERAL_DISCOUNT",
                valuation_date="2026-06-30",
            )

        self.assertTrue(reconstruction.called)
        _, kwargs = reconstruction.call_args
        self.assertEqual(kwargs["helper_schema"], "rate_helpers@v1")
        self.assertIsInstance(
            kwargs["helper_runtime_resolver"],
            StaticRateHelperRuntimeResolver,
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

    def test_build_usd_mxn_xccy_curve_requires_explicit_valuation_date(self):
        with self.assertRaisesRegex(ValmerUsdMxnXccyCurveError, "valuation-date"):
            build_usd_mxn_xccy_curve_frame(
                (DATA_DIR / "IRS_MXN_CURVE.csv").read_bytes(),
                usd_sofr_curve_content=(DATA_DIR / "IRS_USD_CURVE.csv").read_bytes(),
                curve_identifier="VALMER_MXN_USD_COLLATERAL_DISCOUNT",
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

if __name__ == "__main__":
    unittest.main()
