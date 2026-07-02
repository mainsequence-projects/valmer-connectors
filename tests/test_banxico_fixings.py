import datetime as dt
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from banxico.fixings import (
    DEFAULT_SERIES_DEFINITIONS,
    BanxicoFixingError,
    BanxicoFixingsNode,
    build_banxico_fixing_frame,
    make_banxico_fixing_builders,
    normalize_banxico_observations,
    resolve_banxico_token,
    resolve_update_window,
    run_banxico_fixings_update,
    select_series_definitions,
    validate_series_metadata,
)
from banxico.settings import (
    CETE_28_INDEX_IDENTIFIER,
    TIIE_28_INDEX_IDENTIFIER,
    TIIE_91_INDEX_IDENTIFIER,
    TIIE_182_INDEX_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_IDENTIFIER,
)
from valmer_connectors.cli.main import build_parser


class BanxicoFixingsTests(unittest.TestCase):
    def test_validate_series_metadata_accepts_expected_title_terms(self):
        metadata = [
            {
                "idSerie": "SF43783",
                "titulo": "TIIE a 28 dias Tasa de interes en por ciento anual",
            }
        ]
        definition = [
            item
            for item in DEFAULT_SERIES_DEFINITIONS
            if item.index_identifier == TIIE_28_INDEX_IDENTIFIER
        ]

        validated = validate_series_metadata(metadata, definition)

        self.assertEqual(validated[TIIE_28_INDEX_IDENTIFIER].series_id, "SF43783")

    def test_validate_series_metadata_rejects_wrong_title(self):
        metadata = [{"idSerie": "SF43783", "titulo": "Tipo de cambio FIX"}]
        definition = [
            item
            for item in DEFAULT_SERIES_DEFINITIONS
            if item.index_identifier == TIIE_28_INDEX_IDENTIFIER
        ]

        with self.assertRaisesRegex(BanxicoFixingError, "missing terms"):
            validate_series_metadata(metadata, definition)

    def test_normalize_observations_outputs_current_fixing_contract(self):
        frame = normalize_banxico_observations(
            [
                {
                    "idSerie": "SF43783",
                    "datos": [
                        {"fecha": "31/01/2023", "dato": "10.8162"},
                        {"fecha": "01/02/2023", "dato": "N/E"},
                    ],
                }
            ],
            {"SF43783": TIIE_28_INDEX_IDENTIFIER},
        )

        self.assertEqual(frame.index.names, ["time_index", "index_identifier"])
        self.assertEqual(
            frame.reset_index()["time_index"].tolist(),
            [pd.Timestamp("2023-01-31", tz="UTC")],
        )
        self.assertEqual(frame.reset_index()["index_identifier"].tolist(), ["TIIE_28"])
        self.assertAlmostEqual(frame.reset_index()["rate"].iloc[0], 0.108162)
        self.assertNotIn("unique_identifier", frame.reset_index().columns)
        self.assertNotIn("index_uid", frame.reset_index().columns)

    def test_normalize_observations_returns_empty_frame_without_datos(self):
        frame = normalize_banxico_observations(
            [{"idSerie": "SF43783"}],
            {"SF43783": TIIE_28_INDEX_IDENTIFIER},
        )

        self.assertTrue(frame.empty)

    def test_resolve_update_window_uses_offset_start_for_first_run(self):
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = None

        window = resolve_update_window(
            update_statistics=update_statistics,
            index_identifier=TIIE_28_INDEX_IDENTIFIER,
            offset_start="2023-01-01",
            end_date="2023-01-05",
        )

        self.assertEqual(window.start_date, dt.date(2023, 1, 1))
        self.assertEqual(window.end_date, dt.date(2023, 1, 5))
        update_statistics.get_last_update_for_identity.assert_called_once_with(
            TIIE_28_INDEX_IDENTIFIER
        )

    def test_resolve_update_window_uses_day_after_last_update(self):
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2023-01-03 15:00:00",
            tz="UTC",
        )

        window = resolve_update_window(
            update_statistics=update_statistics,
            index_identifier=TIIE_28_INDEX_IDENTIFIER,
            offset_start="2023-01-01",
            end_date="2023-01-05",
        )

        self.assertEqual(window.start_date, dt.date(2023, 1, 4))
        self.assertEqual(window.end_date, dt.date(2023, 1, 5))

    def test_build_banxico_fixing_frame_fetches_expected_series_window(self):
        client = Mock()
        client.fetch_series_data.return_value = [
            {
                "idSerie": "SF43783",
                "datos": [{"fecha": "31/01/2023", "dato": "10.8162"}],
            }
        ]
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = None

        frame = build_banxico_fixing_frame(
            update_statistics=update_statistics,
            index_identifier=TIIE_28_INDEX_IDENTIFIER,
            client=client,
            offset_start="2023-01-31",
            end_date="2023-01-31",
        )

        client.fetch_series_data.assert_called_once_with(
            ["SF43783"],
            start_date=dt.date(2023, 1, 31),
            end_date=dt.date(2023, 1, 31),
        )
        self.assertEqual(frame.reset_index()["index_identifier"].tolist(), ["TIIE_28"])

    def test_make_builders_validates_metadata_once(self):
        client = Mock()
        client.fetch_series_metadata.return_value = [
            {
                "idSerie": "SF43783",
                "titulo": "TIIE a 28 dias Tasa de interes en por ciento anual",
            }
        ]

        builders = make_banxico_fixing_builders(
            client=client,
            index_identifiers=[TIIE_28_INDEX_IDENTIFIER],
            validate_metadata=True,
            end_date="2023-01-31",
            offset_start="2023-01-31",
        )

        client.fetch_series_metadata.assert_called_once()
        self.assertEqual(list(builders), [TIIE_28_INDEX_IDENTIFIER])

    def test_select_series_definitions_defaults_to_provided_subset(self):
        tiie_definitions = select_series_definitions(
            [
                TIIE_OVERNIGHT_INDEX_IDENTIFIER,
                TIIE_28_INDEX_IDENTIFIER,
                TIIE_91_INDEX_IDENTIFIER,
                TIIE_182_INDEX_IDENTIFIER,
            ]
        )

        selected = select_series_definitions(definitions=tiie_definitions)

        self.assertEqual(
            [item.index_identifier for item in selected],
            [
                TIIE_OVERNIGHT_INDEX_IDENTIFIER,
                TIIE_28_INDEX_IDENTIFIER,
                TIIE_91_INDEX_IDENTIFIER,
                TIIE_182_INDEX_IDENTIFIER,
            ],
        )

    def test_run_banxico_fixings_update_wires_node(self):
        with (
            patch("valmer_connectors.instruments.bootstrap.bootstrap_runtime") as bootstrap,
            patch("banxico.fixings.resolve_banxico_token", return_value="token"),
            patch("banxico.fixings.BanxicoSieClient") as client_class,
            patch("banxico.fixings.make_banxico_fixing_builders") as make_builders,
            patch("banxico.fixings.BanxicoFixingsNode") as node_class,
        ):
            builders = {TIIE_28_INDEX_IDENTIFIER: Mock()}
            make_builders.return_value = builders
            node = Mock()
            node.set_fixing_builders.return_value = node
            node_class.return_value = node

            run_banxico_fixings_update(
                index_identifiers=[TIIE_28_INDEX_IDENTIFIER],
                validate_metadata=False,
                end_date="2023-01-31",
                hash_namespace="pytest",
            )

        bootstrap.assert_called_once_with()
        client_class.assert_called_once_with(token="token")
        node_class.assert_called_once()
        config = node_class.call_args.kwargs["fixing_config"]
        self.assertEqual(config.index_unique_identifiers, [TIIE_28_INDEX_IDENTIFIER])
        self.assertEqual(node_class.call_args.kwargs["hash_namespace"], "pytest")
        node.set_fixing_builders.assert_called_once_with(builders)
        node.run.assert_called_once_with(force_update=True)

    def test_resolve_banxico_token_hydrates_secret_detail_when_name_lookup_omits_value(self):
        class SecretValue:
            def get_secret_value(self):
                return " token-from-detail "

        metadata_secret = Mock(uid="secret-uid", value=None)
        detail_secret = Mock(value=SecretValue())

        with patch("mainsequence.client.Secret") as secret_class:
            secret_class.get.return_value = metadata_secret
            secret_class.get_by_uid.return_value = detail_secret

            token = resolve_banxico_token(
                secret_name="BANXICO_TOKEN",
                environ={},
            )

        self.assertEqual(token, "token-from-detail")
        secret_class.get.assert_called_once_with(name="BANXICO_TOKEN")
        secret_class.get_by_uid.assert_called_once_with("secret-uid")

    def test_banxico_node_uses_2010_offset_start(self):
        self.assertEqual(
            BanxicoFixingsNode.OFFSET_START,
            dt.datetime(2010, 1, 1, tzinfo=dt.UTC),
        )


class BanxicoFixingsCliTests(unittest.TestCase):
    def test_cli_dispatches_banxico_fixing_update(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "fixings",
                "update-banxico",
                "--index-identifier",
                TIIE_91_INDEX_IDENTIFIER,
                "--index-identifier",
                CETE_28_INDEX_IDENTIFIER,
                "--skip-metadata-validation",
                "--end-date",
                "2023-01-31",
                "--hash-namespace",
                "pytest",
            ]
        )

        with patch("banxico.fixings.run_banxico_fixings_update") as run_update:
            result = args.func(args)

        self.assertEqual(result, 0)
        run_update.assert_called_once_with(
            index_identifiers=[TIIE_91_INDEX_IDENTIFIER, CETE_28_INDEX_IDENTIFIER],
            token_secret_name="BANXICO_TOKEN",
            validate_metadata=False,
            end_date="2023-01-31",
            hash_namespace="pytest",
        )


if __name__ == "__main__":
    unittest.main()
