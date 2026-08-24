import unittest
from unittest.mock import Mock, patch

import pandas as pd

from fred.reference_rates import (
    DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS,
    FredReferenceRateError,
    normalize_fred_observations,
    resolve_fred_api_key,
    run_fred_reference_rates_update,
    validate_fred_series_metadata,
)
from fred.settings import US_TREASURY_CMT_2Y_INDEX_IDENTIFIER
from valmer_connectors.cli.main import build_parser


class FredReferenceRateTests(unittest.TestCase):
    def setUp(self):
        self.definition = next(
            item
            for item in DEFAULT_FRED_REFERENCE_RATE_DEFINITIONS
            if item.index_identifier == US_TREASURY_CMT_2Y_INDEX_IDENTIFIER
        )

    def test_index_definition_preserves_bounded_source_metadata(self):
        payload = self.definition.index.to_index_payload()

        self.assertEqual(payload["unique_identifier"], "US_TREASURY_CMT_2Y")
        self.assertEqual(payload["calculation_method"], "custom")
        self.assertEqual(payload["value_format"], "percent")
        self.assertEqual(
            payload["metadata_json"],
            {
                "provider": "FRED",
                "source_series_id": "DGS2",
                "currency": "USD",
                "country": "US",
                "source_unit": "percent",
                "observation_type": "treasury_constant_maturity_yield",
                "tenor_months": 24,
                "source_agency": "Board of Governors of the Federal Reserve System",
            },
        )

    def test_metadata_validation_accepts_daily_percent_nsa_series(self):
        validated = validate_fred_series_metadata(
            [
                {
                    "id": "DGS2",
                    "title": (
                        "Market Yield on U.S. Treasury Securities at 2-Year "
                        "Constant Maturity"
                    ),
                    "frequency": "Daily",
                    "units": "Percent",
                    "seasonal_adjustment": "Not Seasonally Adjusted",
                }
            ],
            [self.definition],
        )

        self.assertIn(US_TREASURY_CMT_2Y_INDEX_IDENTIFIER, validated)

    def test_metadata_validation_rejects_wrong_units(self):
        with self.assertRaisesRegex(FredReferenceRateError, "percent units"):
            validate_fred_series_metadata(
                [
                    {
                        "id": "DGS2",
                        "title": "Treasury 2-Year Constant Maturity",
                        "frequency": "Daily",
                        "units": "Index",
                        "seasonal_adjustment": "Not Seasonally Adjusted",
                    }
                ],
                [self.definition],
            )

    def test_observations_skip_dot_and_normalize_percent_once(self):
        frame = normalize_fred_observations(
            [
                {"date": "2026-07-16", "value": "."},
                {"date": "2026-07-17", "value": "4.25"},
            ],
            index_identifier=US_TREASURY_CMT_2Y_INDEX_IDENTIFIER,
        )

        self.assertEqual(frame.index.names, ["time_index", "index_identifier"])
        self.assertEqual(
            frame.reset_index()["time_index"].tolist(),
            [pd.Timestamp("2026-07-17", tz="UTC")],
        )
        self.assertAlmostEqual(frame.iloc[0]["value"], 0.0425)
        self.assertNotIn("unit", frame.columns)
        self.assertEqual(frame.iloc[0]["metadata_json"]["source_quote_unit"], "percent")
        self.assertEqual(frame.iloc[0]["observation_status"], "ready")
        self.assertEqual(frame.iloc[0]["metadata_json"]["source_quote"], 4.25)

    def test_observations_preserve_numeric_zero(self):
        frame = normalize_fred_observations(
            [{"date": "2026-07-17", "value": 0}],
            index_identifier=US_TREASURY_CMT_2Y_INDEX_IDENTIFIER,
        )

        self.assertEqual(frame.iloc[0]["value"], 0.0)

    def test_resolve_api_key_hydrates_secret_detail(self):
        class SecretValue:
            def get_secret_value(self):
                return " fred-key "

        metadata_secret = Mock(uid="secret-uid", value=None)
        detail_secret = Mock(value=SecretValue())
        with patch("mainsequence.client.Secret") as secret_class:
            secret_class.get.return_value = metadata_secret
            secret_class.get_by_uid.return_value = detail_secret

            api_key = resolve_fred_api_key(environ={})

        self.assertEqual(api_key, "fred-key")

    def test_runner_wires_runtime_indexes_config_and_node(self):
        with (
            patch("valmer_connectors.instruments.bootstrap.bootstrap_runtime") as bootstrap,
            patch("fred.reference_rates.upsert_reference_rate_indexes") as upsert_indexes,
            patch("fred.reference_rates.FredClient") as client_class,
            patch("fred.reference_rates.FredReferenceRatesNode") as node_class,
        ):
            node = Mock()
            node.set_source.return_value = node
            node.set_runtime_end.return_value = node
            node_class.return_value = node

            run_fred_reference_rates_update(
                index_identifiers=[US_TREASURY_CMT_2Y_INDEX_IDENTIFIER],
                api_key="key",
                validate_metadata=False,
                runtime_end="2026-07-18",
            )

        bootstrap.assert_called_once_with(seed_static_rows=False)
        upsert_indexes.assert_called_once()
        client_class.assert_called_once_with(api_key="key")
        config = node_class.call_args.args[0]
        self.assertEqual(
            config.index_unique_identifiers,
            [US_TREASURY_CMT_2Y_INDEX_IDENTIFIER],
        )
        self.assertIsNone(node_class.call_args.kwargs["hash_namespace"])
        node.run.assert_called_once_with(force_update=True)


class FredReferenceRateCliTests(unittest.TestCase):
    def test_cli_dispatches_normal_update(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "reference-rates",
                "update-fred",
                "--index-identifier",
                US_TREASURY_CMT_2Y_INDEX_IDENTIFIER,
            ]
        )
        with patch("fred.reference_rates.run_fred_reference_rates_update") as run_update:
            result = args.func(args)

        self.assertEqual(result, 0)
        self.assertIsNone(run_update.call_args.kwargs["hash_namespace"])
        self.assertNotIn("backfill_start", run_update.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
