import unittest
from unittest.mock import Mock, patch

import pandas as pd

from banxico.policy_rates import (
    BANXICO_POLICY_TARGET_DEFINITION,
    BanxicoPolicyRateError,
    normalize_banxico_policy_observations,
    run_banxico_policy_rates_update,
    validate_banxico_policy_metadata,
)
from banxico.settings import BANXICO_POLICY_TARGET_INDEX_IDENTIFIER
from valmer_connectors.cli.main import build_parser


class BanxicoPolicyRateTests(unittest.TestCase):
    def test_index_definition_uses_policy_observation_metadata(self):
        payload = BANXICO_POLICY_TARGET_DEFINITION.index.to_index_payload()

        self.assertEqual(payload["unique_identifier"], "BANXICO_POLICY_TARGET")
        self.assertEqual(payload["provider"], "Banco de Mexico")
        self.assertEqual(payload["metadata_json"]["source_series_id"], "SF61745")
        self.assertEqual(payload["metadata_json"]["observation_type"], "policy_target")

    def test_metadata_validation_accepts_live_policy_target_title(self):
        validated = validate_banxico_policy_metadata(
            [{"idSerie": "SF61745", "titulo": "Tasa objetivo"}],
            [BANXICO_POLICY_TARGET_DEFINITION],
        )
        self.assertIn(BANXICO_POLICY_TARGET_INDEX_IDENTIFIER, validated)

        with self.assertRaisesRegex(BanxicoPolicyRateError, "missing terms"):
            validate_banxico_policy_metadata(
                [{"idSerie": "SF61745", "titulo": "Tipo de cambio FIX"}],
                [BANXICO_POLICY_TARGET_DEFINITION],
            )

        with self.assertRaisesRegex(BanxicoPolicyRateError, "did not include series"):
            validate_banxico_policy_metadata(
                [{"idSerie": "SF99999", "titulo": "Tasa objetivo"}],
                [BANXICO_POLICY_TARGET_DEFINITION],
            )

    def test_observations_skip_unavailable_and_normalize_percent_once(self):
        frame = normalize_banxico_policy_observations(
            [
                {
                    "idSerie": "SF61745",
                    "datos": [
                        {"fecha": "01/02/2026", "dato": "7.50"},
                        {"fecha": "02/02/2026", "dato": "N/E"},
                    ],
                }
            ],
            series_id_to_index_identifier={
                "SF61745": BANXICO_POLICY_TARGET_INDEX_IDENTIFIER
            },
        )

        self.assertEqual(
            frame.reset_index()["time_index"].tolist(),
            [pd.Timestamp("2026-02-01", tz="UTC")],
        )
        self.assertAlmostEqual(frame.iloc[0]["value"], 0.075)
        self.assertEqual(frame.iloc[0]["unit"], "decimal")
        self.assertEqual(frame.iloc[0]["metadata_json"]["source_quote"], 7.5)

    def test_observations_preserve_numeric_zero(self):
        frame = normalize_banxico_policy_observations(
            [
                {
                    "idSerie": "SF61745",
                    "datos": [{"fecha": "01/02/2026", "dato": 0}],
                }
            ],
            series_id_to_index_identifier={
                "SF61745": BANXICO_POLICY_TARGET_INDEX_IDENTIFIER
            },
        )

        self.assertEqual(frame.iloc[0]["value"], 0.0)

    def test_runner_wires_runtime_indexes_config_and_node(self):
        with (
            patch("valmer_connectors.instruments.bootstrap.bootstrap_runtime") as bootstrap,
            patch("banxico.policy_rates.upsert_reference_rate_indexes") as upsert_indexes,
            patch("banxico.policy_rates.BanxicoSieClient") as client_class,
            patch("banxico.policy_rates.BanxicoPolicyRatesNode") as node_class,
        ):
            node = Mock()
            node.set_source.return_value = node
            node.set_runtime_end.return_value = node
            node_class.return_value = node

            run_banxico_policy_rates_update(
                token="token",
                validate_metadata=False,
                runtime_end="2026-07-18",
                hash_namespace="adr-0009-smoke",
            )

        bootstrap.assert_called_once_with(seed_static_rows=False)
        upsert_indexes.assert_called_once()
        client_class.assert_called_once_with(token="token")
        config = node_class.call_args.args[0]
        self.assertEqual(
            config.index_unique_identifiers,
            [BANXICO_POLICY_TARGET_INDEX_IDENTIFIER],
        )
        node.run.assert_called_once_with(force_update=True)


class BanxicoPolicyRateCliTests(unittest.TestCase):
    def test_cli_dispatches_normal_update(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "reference-rates",
                "update-banxico-policy",
            ]
        )
        with patch("banxico.policy_rates.run_banxico_policy_rates_update") as run_update:
            result = args.func(args)

        self.assertEqual(result, 0)
        self.assertIsNone(run_update.call_args.kwargs["hash_namespace"])
        self.assertNotIn("backfill_start", run_update.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
