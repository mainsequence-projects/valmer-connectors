import datetime as dt
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from valmer_connectors.data_nodes.nodes import MetaTableValmerSourceConfig
from valmer_connectors.services import vector_update
from valmer_connectors.settings import VALMER_METATABLE_SOURCE_CONFIG_RESOURCE


class ValmerVectorUpdateServiceTests(unittest.TestCase):
    def test_packaged_metatable_source_config_is_readable(self):
        self.assertTrue(VALMER_METATABLE_SOURCE_CONFIG_RESOURCE.is_file())

        sources = vector_update.load_metatable_sources_config(
            VALMER_METATABLE_SOURCE_CONFIG_RESOURCE
        )

        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0].source_name, "historical_valmer_vector")
        self.assertEqual(
            sources[0].metatable_identifier,
            "dbo.vector_precios_gubernamental",
        )
        self.assertEqual(sources[0].sql_dialect, "mssql")
        self.assertEqual(len(sources[0].column_map), 29)

    def test_metatable_preflight_probes_the_registered_source(self):
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "sources.json"
            config_path.write_text(
                """
                {
                  "sources": [
                    {
                      "source_name": "government",
                      "metatable_identifier": "dbo.vector_precios_gubernamental",
                      "column_map": {
                        "Fecha": "fecha",
                        "TV": "tipovalor",
                        "Emisora": "emisora",
                        "Serie": "serie"
                      }
                    }
                  ]
                }
                """,
                encoding="utf-8",
            )

            meta_table = SimpleNamespace(
                uid="be6c0a2f-e142-43ae-80e5-1cdb82e53c6b",
                identifier="dbo.vector_precios_gubernamental",
                provisioning_status="active",
                physical_schema="dbo",
                physical_table_name="vector_precios_gubernamental",
                columns=[
                    SimpleNamespace(name=name)
                    for name in ("Fecha", "TV", "Emisora", "Serie")
                ],
            )
            with (
                patch.object(
                    vector_update.ImportValmer,
                    "_resolve_source_metatable",
                    return_value=meta_table,
                ) as resolve_source,
                patch.object(
                    vector_update.ImportValmer,
                    "_run_metatable_query",
                    return_value={
                        "ok": True,
                        "columns": ["Fecha", "TV", "Emisora", "Serie"],
                        "rows": [["2024-01-03", "M", "BONOS", "A"]],
                    },
                ) as run_query,
            ):
                result = vector_update.preflight_metatable_source(config_path)

            source = resolve_source.call_args.args[0]
            self.assertEqual(
                source.metatable_identifier,
                "dbo.vector_precios_gubernamental",
            )
            sql = run_query.call_args.args[1]
            self.assertIn("FROM [dbo].[vector_precios_gubernamental]", sql)
            self.assertEqual(result["source_kind"], "metatable")
            self.assertEqual(result["sample_row_count"], 1)

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

        bootstrap.assert_called_once_with(seed_static_rows=False)
        build_import.assert_called_once_with(
            bucket_name="Vector Bucket",
            source_kind="artifact",
            source_metatables=None,
        )
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
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
            bypass_vector_cursor_filter=False,
        )
        first_loop_updater.run.assert_called_once_with(force_update=True)
        second_loop_updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
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
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_vector_update_uses_local_bucket_path(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()

        with TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            first_file = folder / "a.xls"
            second_file = folder / "b.xlsx"
            first_file.write_text("", encoding="utf-8")
            second_file.write_text("", encoding="utf-8")

            with (
                patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
                patch(
                    "valmer_connectors.services.vector_update._latest_vector_storage_time_index",
                    return_value=None,
                ),
                patch("valmer_connectors.services.vector_update._debug_artifact_files") as debug_files,
                patch(
                    "valmer_connectors.services.vector_update.build_import_valmer",
                    return_value=updater,
                ),
            ):
                debug_files.return_value.__enter__.return_value = None
                debug_files.return_value.__exit__.return_value = False

                vector_update.run_vector_update(
                    bucket_name="Vector Bucket",
                    local_bucket_path=str(folder),
                )

        debug_files.assert_called_once_with(
            os.pathsep.join(str(path) for path in [first_file, second_file])
        )
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_vector_update_resolves_local_bucket_path_from_env_var(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()

        with TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            vector_file = folder / "vector.xls"
            vector_file.write_text("", encoding="utf-8")

            with (
                patch.dict(
                    "os.environ",
                    {"VALMER_VECTOR_UPLOAD_DEBUG_PATH": str(folder)},
                ),
                patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
                patch(
                    "valmer_connectors.services.vector_update._latest_vector_storage_time_index",
                    return_value=None,
                ),
                patch("valmer_connectors.services.vector_update._debug_artifact_files") as debug_files,
                patch(
                    "valmer_connectors.services.vector_update.build_import_valmer",
                    return_value=updater,
                ),
            ):
                debug_files.return_value.__enter__.return_value = None
                debug_files.return_value.__exit__.return_value = False

                vector_update.run_vector_update(
                    bucket_name="Vector Bucket",
                    local_bucket_path_env_var="VALMER_VECTOR_UPLOAD_DEBUG_PATH",
                )

        debug_files.assert_called_once_with(str(vector_file))
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_vector_update_filters_local_bucket_files_by_persisted_vector_date(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()

        with TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            old_file = folder / "VectorAnalitico24h_2024-12-03.xls"
            new_file = folder / "VectorAnalitico24h_2024-12-04.xls"
            old_file.write_text("", encoding="utf-8")
            new_file.write_text("", encoding="utf-8")

            with (
                patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
                patch(
                    "valmer_connectors.services.vector_update._latest_vector_storage_time_index",
                    return_value=dt.datetime(2024, 12, 3, 23, 59, 59, tzinfo=dt.UTC),
                ),
                patch("valmer_connectors.services.vector_update._debug_artifact_files") as debug_files,
                patch(
                    "valmer_connectors.services.vector_update.build_import_valmer",
                    return_value=updater,
                ),
            ):
                debug_files.return_value.__enter__.return_value = None
                debug_files.return_value.__exit__.return_value = False

                vector_update.run_vector_update(
                    bucket_name="Vector Bucket",
                    local_bucket_path=str(folder),
                )

        debug_files.assert_called_once_with(str(new_file))
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_vector_update_bypasses_local_cursor_filter_for_repair_runs(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()

        with TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            old_file = folder / "VectorAnalitico24h_2024-12-03.xls"
            new_file = folder / "VectorAnalitico24h_2024-12-04.xls"
            old_file.write_text("", encoding="utf-8")
            new_file.write_text("", encoding="utf-8")

            with (
                patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
                patch(
                    "valmer_connectors.services.vector_update._latest_vector_storage_time_index",
                    side_effect=AssertionError("cursor lookup should be bypassed"),
                ),
                patch("valmer_connectors.services.vector_update._debug_artifact_files") as debug_files,
                patch(
                    "valmer_connectors.services.vector_update.build_import_valmer",
                    return_value=updater,
                ),
            ):
                debug_files.return_value.__enter__.return_value = None
                debug_files.return_value.__exit__.return_value = False

                vector_update.run_vector_update(
                    bucket_name="Vector Bucket",
                    local_bucket_path=str(folder),
                    bypass_vector_cursor_filter=True,
                )

        debug_files.assert_called_once_with(
            os.pathsep.join(str(path) for path in [old_file, new_file])
        )
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=True,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_vector_update_uses_onedrive_graph_source(self):
        updater = Mock()
        updater.get_update_statistics.return_value = object()

        with TemporaryDirectory() as tmpdir:
            first_file = Path(tmpdir) / "VectorAnalitico24h_2024-12-03.xls"
            first_file.write_text("", encoding="utf-8")

            with (
                patch("valmer_connectors.services.vector_update.bootstrap_runtime"),
                patch(
                    "valmer_connectors.services.vector_update._latest_vector_storage_time_index",
                    return_value=dt.datetime(2024, 12, 2, 23, 59, 59, tzinfo=dt.UTC),
                ) as latest_storage,
                patch(
                    "valmer_connectors.services.onedrive_graph.stage_onedrive_vector_files",
                    return_value=[first_file],
                ) as stage_onedrive,
                patch(
                    "valmer_connectors.services.vector_update._ensure_local_vector_files_materialized"
                ) as ensure_materialized,
                patch("valmer_connectors.services.vector_update._debug_artifact_files") as debug_files,
                patch(
                    "valmer_connectors.services.vector_update.build_import_valmer",
                    return_value=updater,
                ) as build_import,
            ):
                debug_files.return_value.__enter__.return_value = None
                debug_files.return_value.__exit__.return_value = False

                vector_update.run_vector_update(
                    bucket_name="Vector Bucket",
                    source_kind="onedrive-graph",
                    onedrive_drive_id="drive-1",
                    onedrive_folder_path="clients/actinver/vector_de_precios_upload",
                    onedrive_cache_path=str(Path(tmpdir) / "cache"),
                    onedrive_tenant_id_secret_name="TENANT_SECRET",
                    onedrive_client_id_secret_name="CLIENT_SECRET",
                    onedrive_client_secret_secret_name="CLIENT_VALUE_SECRET",
                )

        latest_storage.assert_called_once_with()
        stage_onedrive.assert_called_once_with(
            latest_time_index=dt.datetime(2024, 12, 2, 23, 59, 59, tzinfo=dt.UTC),
            drive_id="drive-1",
            folder_path="clients/actinver/vector_de_precios_upload",
            cache_path=str(Path(tmpdir) / "cache"),
            tenant_id_secret_name="TENANT_SECRET",
            client_id_secret_name="CLIENT_SECRET",
            client_secret_secret_name="CLIENT_VALUE_SECRET",
        )
        ensure_materialized.assert_called_once_with([first_file])
        debug_files.assert_called_once_with(str(first_file))
        build_import.assert_called_once_with(
            bucket_name="Vector Bucket",
            source_kind="artifact",
            source_metatables=None,
        )
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
            bypass_vector_cursor_filter=False,
        )
        updater.run.assert_called_once_with(force_update=True)

    def test_local_vector_file_selection_keeps_undated_files(self):
        latest = dt.datetime(2024, 12, 3, 23, 59, 59, tzinfo=dt.UTC)
        old_file = Path("VectorAnalitico24h_2024-12-03.xls")
        new_file = Path("VectorAnalitico24h_2024-12-04.xls")
        undated_file = Path("manual-vector.xls")

        selected = vector_update._select_local_vector_files_for_update(
            [old_file, new_file, undated_file],
            latest,
        )

        self.assertEqual(selected, [new_file, undated_file])

    def test_local_vector_file_preflight_materializes_placeholder_files(self):
        with TemporaryDirectory() as tmpdir:
            placeholder = Path(tmpdir) / "VectorAnalitico24h_2024-12-04.xls"
            placeholder.write_text("placeholder", encoding="utf-8")

            materialized = iter([False, True])
            with (
                patch(
                    "valmer_connectors.services.vector_update._is_materialized_local_file",
                    side_effect=lambda _path: next(materialized),
                ),
                patch(
                    "valmer_connectors.services.vector_update._request_local_file_materialization"
                ) as request_materialization,
            ):
                vector_update._ensure_local_vector_files_materialized([placeholder])

        request_materialization.assert_called_once_with(placeholder)

    def test_local_vector_file_preflight_raises_when_materialization_fails(self):
        with TemporaryDirectory() as tmpdir:
            placeholder = Path(tmpdir) / "VectorAnalitico24h_2024-12-04.xls"
            placeholder.write_text("placeholder", encoding="utf-8")

            with (
                patch(
                    "valmer_connectors.services.vector_update._is_materialized_local_file",
                    return_value=False,
                ),
                patch(
                    "valmer_connectors.services.vector_update._request_local_file_materialization"
                ),
                patch("valmer_connectors.services.vector_update.time.sleep"),
            ):
                with self.assertRaisesRegex(RuntimeError, str(placeholder)):
                    vector_update._ensure_local_vector_files_materialized([placeholder])

    def test_vector_update_rejects_local_bucket_with_metatable_source(self):
        with self.assertRaisesRegex(
            ValueError,
            "Local artifact paths cannot be used with --source metatable",
        ):
            vector_update.run_vector_update(
                source_kind="metatable",
                source_metatables_config_path="unused.json",
                local_bucket_path="/tmp/valmer-vector-folder",
            )

    def test_vector_update_rejects_debug_artifact_path_with_local_bucket_path(self):
        with self.assertRaisesRegex(
            ValueError,
            "--debug-artifact-path and --local-bucket-path are mutually exclusive",
        ):
            vector_update.run_vector_update(
                debug_artifact_path="/tmp/file.xls",
                local_bucket_path="/tmp/valmer-vector-folder",
            )

if __name__ == "__main__":
    unittest.main()
