import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from valmer_connectors.data_nodes.nodes import MetaTableValmerSourceConfig
from valmer_connectors.services import vector_update


class ValmerVectorUpdateServiceTests(unittest.TestCase):
    def test_vector_update_forces_current_pricing_hydration_without_global_daily_gate(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()

        with (
            patch("valmer_connectors.services.vector_update.bootstrap_runtime") as bootstrap,
            patch("valmer_connectors.services.vector_update._debug_artifact_path") as debug_path,
            patch(
                "valmer_connectors.services.vector_update.build_import_valmer",
                return_value=updater,
            ) as build_import,
        ):
            debug_path.return_value.__enter__.return_value = None
            debug_path.return_value.__exit__.return_value = False

            vector_update.run_vector_update(bucket_name="Vector Bucket")

        bootstrap.assert_called_once_with()
        build_import.assert_called_once_with(
            bucket_name="Vector Bucket",
            source_kind="artifact",
            source_metatables=None,
        )
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_first_time_update_bypasses_daily_gate_and_runs_first_loop(self):
        stats_updater = Mock()
        stats_updater.get_update_statistics.side_effect = AttributeError()
        first_loop_updater = Mock()
        second_loop_updater = Mock()

        with (
            patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
            patch("valmer_connectors.services.vector_update._debug_artifact_path") as debug_path,
            patch(
                "valmer_connectors.services.vector_update.build_import_valmer",
                side_effect=[
                    stats_updater,
                    first_loop_updater,
                    second_loop_updater,
                ],
            ) as build_import,
        ):
            debug_path.return_value.__enter__.return_value = None
            debug_path.return_value.__exit__.return_value = False

            vector_update.run_vector_update(
                bucket_name="Vector Bucket",
                first_loop_count=2,
            )

        self.assertEqual(build_import.call_count, 3)
        first_loop_updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
        )
        first_loop_updater.run.assert_called_once_with(force_update=True)
        second_loop_updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
        )
        second_loop_updater.run.assert_called_once_with(force_update=True)

    def test_vector_update_passes_metatable_sources_config(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.json"
            config_path.write_text(
                """
                {
                  "sources": [
                    {
                      "source_name": "government",
                      "metatable_identifier": "external.gov",
                      "column_map": {
                        "Fecha": "fecha",
                        "TV": "tipovalor",
                        "Emisora": "emisora",
                        "Serie": "serie",
                        "PrecioSucio": "preciosucio"
                      }
                    }
                  ]
                }
                """,
                encoding="utf-8",
            )

            with (
                patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
                patch("valmer_connectors.services.vector_update._debug_artifact_path") as debug_path,
                patch(
                    "valmer_connectors.services.vector_update.build_import_valmer",
                    return_value=updater,
                ) as build_import,
            ):
                debug_path.return_value.__enter__.return_value = None
                debug_path.return_value.__exit__.return_value = False

                vector_update.run_vector_update(
                    bucket_name="Vector Bucket",
                    source_kind="metatable",
                    source_metatables_config_path=str(config_path),
                )

        source = build_import.call_args.kwargs["source_metatables"][0]
        self.assertIsInstance(source, MetaTableValmerSourceConfig)
        self.assertEqual(source.source_name, "government")
        self.assertEqual(source.column_map["Fecha"], "fecha")
        updater.prepare_for_update.assert_called_once_with(force_pricing_update=True)
        updater.run.assert_called_once_with(force_update=True)


if __name__ == "__main__":
    unittest.main()
