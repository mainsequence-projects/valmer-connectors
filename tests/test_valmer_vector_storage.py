import os
import tempfile
import unittest
import uuid
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, PropertyMock, patch

import pandas as pd
from msm.base import markets_table_name
from msm.constants import ASSET_TYPE_BOND
from msm.data_nodes.assets import AssetSnapshot as CoreAssetSnapshot
from msm.models.assets import AssetTable
from msm.settings import ASSET_IDENTIFIER_DIMENSION, markets_auto_register_namespace
from sqlalchemy import Float

from mainsequence.meta_tables.time_index_table_updates.runner import UpdateRunner
from valmer_connectors.data_nodes.nodes import (
    VALMER_ASSET_DETAIL_SOURCE_COLUMNS,
    VALMER_SOURCE_COLUMN_SPECS,
    VALMER_VECTOR_COLUMN_SPECS,
    ImportValmer,
    ImportValmerConfig,
    MetaTableValmerSourceConfig,
    _build_valmer_asset_snapshot_rows,
    _persist_valmer_pricing_details_batch,
    _pricing_adapter_failure_detail,
    _publish_valmer_asset_snapshots,
)
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.markets import VALMER_MARKETS_STORAGE_APP
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable


class _UpdateStatisticsStub:
    @staticmethod
    def filter_df_by_latest_value(frame):
        return frame


def _build_vector_source_row(*, numeric_value: object = "1.25") -> dict:
    source_row = {"unique_identifier": "M_BONOS_241205"}
    for spec in VALMER_SOURCE_COLUMN_SPECS:
        if spec.source_name is None:
            continue
        if spec.transform == "string":
            value = "MXN"
        elif spec.transform in {"float", "percent"}:
            value = numeric_value
        elif spec.transform == "int":
            value = 1
        elif spec.transform == "datetime":
            value = "2024-01-02"
        elif spec.transform == "date_ymd":
            value = "20240102"
        else:
            raise AssertionError(f"Unhandled transform {spec.transform!r}")
        source_row[spec.source_name] = value
    for column_name in VALMER_ASSET_DETAIL_SOURCE_COLUMNS:
        source_row.setdefault(column_name, numeric_value)
    return source_row


class ValmerVectorStorageTest(unittest.TestCase):
    def test_vector_storage_owns_import_valmer_schema(self):
        storage_columns = set(ValmerVectorPricesStorage.__table__.columns.keys())
        expected_columns = {
            "time_index",
            ASSET_IDENTIFIER_DIMENSION,
            *(spec.column_name for spec in VALMER_VECTOR_COLUMN_SPECS),
        }

        self.assertEqual(storage_columns, expected_columns)
        self.assertEqual(
            ValmerVectorPricesStorage.__metatable_identifier__,
            "valmer_connectors.vector_de_precios_valmer",
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__table__.name,
            markets_table_name(
                VALMER_MARKETS_STORAGE_APP,
                ValmerVectorPricesStorage.__markets_authored_identifier__,
                suffix=markets_auto_register_namespace(),
            ),
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__markets_storage_app__,
            VALMER_MARKETS_STORAGE_APP,
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__table__.info["markets_storage_app"],
            VALMER_MARKETS_STORAGE_APP,
        )
        self.assertIsNone(ValmerVectorPricesStorage.__table__.schema)
        self.assertEqual(
            ValmerVectorPricesStorage.__index_names__,
            ["time_index", ASSET_IDENTIFIER_DIMENSION],
        )
        self.assertIn(
            ImportValmer.asset_identity_dimension,
            ValmerVectorPricesStorage.__index_names__,
        )
        for synthetic_column in ("open", "high", "low", "close", "volume", "open_time"):
            self.assertNotIn(synthetic_column, storage_columns)
        self.assertIsInstance(
            ValmerVectorPricesStorage.__table__.c.days_since_coupon.type,
            Float,
        )
        self.assertIsInstance(
            ValmerVectorPricesStorage.__table__.c.coupons_remaining.type,
            Float,
        )

    def test_vector_storage_links_to_asset_unique_identifier(self):
        foreign_keys = list(
            ValmerVectorPricesStorage.__table__.c.asset_identifier.foreign_keys
        )

        self.assertEqual(len(foreign_keys), 1)
        self.assertEqual(foreign_keys[0].column.name, "unique_identifier")
        self.assertEqual(
            foreign_keys[0].column.table.fullname, AssetTable.__table__.fullname
        )

    def test_valmer_asset_details_uses_asset_uid_foreign_key(self):
        self.assertEqual(
            ValmerAssetDetailsTable.__table__.name,
            markets_table_name(
                VALMER_MARKETS_STORAGE_APP,
                ValmerAssetDetailsTable.__markets_authored_identifier__,
                suffix=markets_auto_register_namespace(),
            ),
        )
        self.assertEqual(
            ValmerAssetDetailsTable.__markets_storage_app__,
            VALMER_MARKETS_STORAGE_APP,
        )
        self.assertEqual(
            ValmerAssetDetailsTable.__table__.info["markets_storage_app"],
            VALMER_MARKETS_STORAGE_APP,
        )

        foreign_keys = list(ValmerAssetDetailsTable.__table__.c.asset_uid.foreign_keys)

        self.assertEqual(len(foreign_keys), 1)
        self.assertEqual(foreign_keys[0].column.name, "uid")
        self.assertEqual(
            foreign_keys[0].column.table.fullname, AssetTable.__table__.fullname
        )
        self.assertIsNone(ValmerAssetDetailsTable.__table__.schema)

    def test_valmer_asset_details_columns_have_metadata(self):
        for column in ValmerAssetDetailsTable.__table__.columns:
            with self.subTest(column=column.name):
                self.assertIn("label", column.info)
                self.assertIn("description", column.info)

    def test_bucket_name_is_typed_node_configuration(self):
        config = ImportValmerConfig(bucket_name="Hitorical Valmer Vector Analytico")

        self.assertEqual(config.bucket_name, "Hitorical Valmer Vector Analytico")
        self.assertIn("bucket_name", ImportValmerConfig.model_fields)

    def test_import_valmer_update_matches_storage_contract(self):
        source_row = _build_vector_source_row()
        source_row["diastransccpn"] = float("nan")
        source_row["cuponesxcobrar"] = "nan"

        node = ImportValmer.__new__(ImportValmer)
        node.source_data = pd.DataFrame([source_row])
        node.update_statistics = _UpdateStatisticsStub()

        result = ImportValmer.update(node)
        result_row = result.reset_index().iloc[0]

        self.assertEqual(result.index.names, ["time_index", ASSET_IDENTIFIER_DIMENSION])
        self.assertEqual(
            str(result.index.get_level_values("time_index").dtype),
            "datetime64[ns, UTC]",
        )
        self.assertEqual(
            set(result.reset_index().columns),
            set(ValmerVectorPricesStorage.__table__.columns.keys()),
        )
        self.assertEqual(
            result.reset_index()[ASSET_IDENTIFIER_DIMENSION].iloc[0], "M_BONOS_241205"
        )
        self.assertEqual(str(result["days_since_coupon"].dtype), "float64")
        self.assertEqual(str(result["coupons_remaining"].dtype), "float64")
        self.assertTrue(pd.isna(result_row["days_since_coupon"]))
        self.assertTrue(pd.isna(result_row["coupons_remaining"]))
        for synthetic_column in ("open", "high", "low", "close", "volume", "open_time"):
            self.assertNotIn(synthetic_column, result.columns)
        self.assertEqual(
            result.reset_index()[["days_since_coupon", "coupons_remaining"]].to_json(
                orient="records"
            ),
            '[{"days_since_coupon":null,"coupons_remaining":null}]',
        )
        UpdateRunner.validate_data_frame(
            result.copy(),
            storage_class_type="postgres",
            meta_table=ValmerVectorPricesStorage,
        )

    def test_import_valmer_update_keeps_integer_like_float_fields_as_float64(self):
        source_row = _build_vector_source_row(numeric_value="1")

        node = ImportValmer.__new__(ImportValmer)
        node.source_data = pd.DataFrame([source_row])
        node.update_statistics = _UpdateStatisticsStub()

        result = ImportValmer.update(node)

        self.assertEqual(str(result["days_since_coupon"].dtype), "float64")
        self.assertEqual(str(result["coupons_remaining"].dtype), "float64")
        self.assertEqual(result["days_since_coupon"].iloc[0], 1.0)
        self.assertEqual(result["coupons_remaining"].iloc[0], 1.0)
        UpdateRunner.validate_data_frame(
            result.copy(),
            storage_class_type="postgres",
            meta_table=ValmerVectorPricesStorage,
        )

    def test_source_filter_keeps_rows_newer_than_each_asset_cursor(self):
        frame = pd.DataFrame(
            [
                {
                    "fecha": "20240102",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "A",
                    "unique_identifier": "M_BONOS_A",
                },
                {
                    "fecha": "20240104",
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "A",
                    "unique_identifier": "M_BONOS_A",
                },
                {
                    "fecha": "20240101",
                    "tipovalor": "BI",
                    "emisora": "CETES",
                    "serie": "B",
                    "unique_identifier": "BI_CETES_B",
                },
            ]
        )
        cursor = {
            "M_BONOS_A": pd.Timestamp("2024-01-02 23:59:59", tz="UTC"),
        }

        result = ImportValmer._filter_source_rows_from_last_vector_observation(
            frame,
            cursor,
            source_name="unit",
            logger=Mock(),
        )

        self.assertEqual(result["unique_identifier"].tolist(), ["M_BONOS_A", "BI_CETES_B"])
        self.assertEqual(result["fecha"].tolist(), ["20240104", "20240101"])

    def test_metatable_sources_filter_each_source_then_concatenate(self):
        source_a = MetaTableValmerSourceConfig(
            source_name="government",
            metatable_identifier="external.gov",
            column_map={
                "Fecha": "fecha",
                "TV": "tipovalor",
                "Emisora": "emisora",
                "Serie": "serie",
                "PrecioSucio": "preciosucio",
            },
        )
        source_b = MetaTableValmerSourceConfig(
            source_name="corporate",
            metatable_identifier="external.corp",
            column_map={
                "Fecha": "fecha",
                "TV": "tipovalor",
                "Emisora": "emisora",
                "Serie": "serie",
                "PrecioSucio": "preciosucio",
            },
        )
        node = ImportValmer.__new__(ImportValmer)
        node.source_data = None
        node.source_metatables = [source_a, source_b]
        node._latest_vector_cursor_by_asset = Mock(
            return_value={
                "M_BONOS_A": pd.Timestamp("2024-01-02 23:59:59", tz="UTC"),
                "I_CORP_A": pd.Timestamp("2024-01-05 23:59:59", tz="UTC"),
            }
        )
        source_frames = {
            "government": pd.DataFrame(
                [
                    {
                        "Fecha": "2024-01-02",
                        "TV": "M",
                        "Emisora": "BONOS",
                        "Serie": "A",
                        "PrecioSucio": 99.0,
                    },
                    {
                        "Fecha": "2024-01-03",
                        "TV": "M",
                        "Emisora": "BONOS",
                        "Serie": "A",
                        "PrecioSucio": 100.0,
                    },
                ]
            ),
            "corporate": pd.DataFrame(
                [
                    {
                        "Fecha": "2024-01-06",
                        "TV": "I",
                        "Emisora": "CORP",
                        "Serie": "A",
                        "PrecioSucio": 101.0,
                    },
                    {
                        "Fecha": "2024-01-01",
                        "TV": "BI",
                        "Emisora": "CETES",
                        "Serie": "B",
                        "PrecioSucio": 10.0,
                    },
                ]
            ),
        }
        node._read_metatable_source_frame = Mock(
            side_effect=lambda source, **_: source_frames[source.source_name]
        )

        with patch.object(
            ImportValmer,
            "logger",
            new_callable=PropertyMock,
            return_value=Mock(),
        ):
            ImportValmer._set_metatable_source_data(node)

        self.assertEqual(
            node.source_data["unique_identifier"].tolist(),
            ["M_BONOS_A", "I_CORP_A", "BI_CETES_B"],
        )
        self.assertEqual(node._read_metatable_source_frame.call_count, 2)
        self.assertEqual(
            node._read_metatable_source_frame.call_args_list[0].kwargs[
                "minimum_valuation_date"
            ],
            pd.Timestamp("2024-01-02 23:59:59", tz="UTC"),
        )

    def test_direct_mssql_source_uses_explicit_table_and_cursor_pushdown(self):
        source = MetaTableValmerSourceConfig(
            source_name="government",
            direct_mssql_table="dbo.vector_precios_gubernamental",
            column_map={
                "Fecha": "fecha",
                "TV": "tipovalor",
                "Emisora": "emisora",
                "Serie": "serie",
                "PrecioSucio": "preciosucio",
            },
        )
        expected = pd.DataFrame(
            [
                {
                    "Fecha": "2024-01-03",
                    "TV": "M",
                    "Emisora": "BONOS",
                    "Serie": "A",
                    "PrecioSucio": 100.0,
                }
            ]
        )

        with (
            patch.object(ImportValmer, "_run_direct_mssql_query", return_value=expected) as run,
            patch.object(ImportValmer, "_resolve_source_metatable") as resolve,
        ):
            result = ImportValmer._read_metatable_source_frame(
                source,
                logger=Mock(),
                minimum_valuation_date=pd.Timestamp("2024-01-02 23:59:59", tz="UTC"),
            )

        pd.testing.assert_frame_equal(result, expected)
        resolve.assert_not_called()
        sql = run.call_args.args[0]
        self.assertIn("FROM [dbo].[vector_precios_gubernamental]", sql)
        self.assertIn(
            "WHERE [Fecha] > '2024-01-02T23:59:59.000'",
            sql,
        )

    def test_metatable_source_config_requires_one_source_reference(self):
        column_map = {
            "Fecha": "fecha",
            "TV": "tipovalor",
            "Emisora": "emisora",
            "Serie": "serie",
        }

        with self.assertRaisesRegex(ValueError, "exactly one"):
            MetaTableValmerSourceConfig(
                source_name="government",
                metatable_identifier="external.gov",
                direct_mssql_table="dbo.vector_precios_gubernamental",
                column_map=column_map,
            )

    def test_metatable_query_result_builds_frame_from_rows_and_columns(self):
        result = {
            "ok": True,
            "columns": ["Fecha", "TV", "Emisora", "Serie", "PrecioSucio"],
            "rows": [["2024-01-03", "M", "BONOS", "A", 100.0]],
        }

        frame = ImportValmer._frame_from_metatable_query_result(
            result,
            source_name="unit",
        )

        self.assertEqual(
            frame.to_dict(orient="records"),
            [
                {
                    "Fecha": "2024-01-03",
                    "TV": "M",
                    "Emisora": "BONOS",
                    "Serie": "A",
                    "PrecioSucio": 100.0,
                }
            ],
        )

    def test_metatable_query_result_builds_frame_from_nested_results(self):
        result = {
            "ok": True,
            "results": {
                "columns": [
                    {"name": "Fecha"},
                    {"name": "TV"},
                    {"name": "Emisora"},
                    {"name": "Serie"},
                    {"name": "PrecioSucio"},
                ],
                "rows": [["2024-01-03", "M", "BONOS", "A", 100.0]],
            },
        }

        frame = ImportValmer._frame_from_metatable_query_result(
            result,
            source_name="unit",
        )

        self.assertEqual(
            frame.columns.tolist(),
            ["Fecha", "TV", "Emisora", "Serie", "PrecioSucio"],
        )
        self.assertEqual(frame.loc[0, "PrecioSucio"], 100.0)

    def test_metatable_source_normalization_matches_columns_in_pandas(self):
        source = MetaTableValmerSourceConfig(
            source_name="government",
            metatable_identifier="external.gov",
            column_map={
                "Fecha": "fecha",
                "TV": "tipovalor",
                "Emisora": "emisora",
                "Serie": "serie",
                "PrecioSucio": "preciosucio",
                "Plazo": "plazoemision",
            },
        )
        frame = pd.DataFrame(
            [
                {
                    "fecha": "2024-01-03",
                    "tv": "M",
                    "emisora": "BONOS",
                    "serie": "A",
                    "preciosucio": 100.0,
                    "plazo": 365,
                }
            ]
        )

        normalized = ImportValmer._normalize_metatable_source_frame(frame, source)

        self.assertEqual(normalized.loc[0, "unique_identifier"], "M_BONOS_A")
        self.assertEqual(normalized.loc[0, "preciosucio"], 100.0)
        self.assertEqual(normalized.loc[0, "plazoemision"], 365)

    def test_valmer_asset_snapshot_rows_map_nombre_completo_to_name(self):
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "nombrecompleto": "BONOS 241205 FULL NAME",
                }
            ]
        )

        rows = _build_valmer_asset_snapshot_rows(latest, ["M_BONOS_241205"])

        self.assertEqual(
            rows,
            [
                {
                    "time_index": pd.Timestamp("2024-01-02T23:59:59Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "name": "BONOS 241205 FULL NAME",
                }
            ],
        )

    def test_publish_valmer_asset_snapshots_uses_asset_snapshot_node(self):
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "nombrecompleto": "BONOS 241205 FULL NAME",
                }
            ]
        )
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )
        snapshot_node = Mock()
        snapshot_node.table_update = None
        snapshot_node.get_df_between_dates.return_value = pd.DataFrame()
        snapshot_node.set_snapshots.return_value = snapshot_node
        snapshot_node.run.return_value = (False, pd.DataFrame([{"name": "BONOS 241205 FULL NAME"}]))

        with patch("valmer_connectors.data_nodes.nodes.AssetSnapshot") as asset_snapshot_class:
            asset_snapshot_class.return_value = snapshot_node
            asset_snapshot_class.build_frame.side_effect = CoreAssetSnapshot.build_frame

            published = _publish_valmer_asset_snapshots(
                latest,
                {"M_BONOS_241205": asset},
                logger=Mock(),
            )

        snapshot_node.set_snapshots.assert_called_once_with(
            [
                {
                    "time_index": pd.Timestamp("2024-01-02T23:59:59Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "name": "BONOS 241205 FULL NAME",
                }
            ],
            verify_existing=False,
        )
        snapshot_node.run.assert_called_once_with(force_update=True)
        snapshot_node.get_df_between_dates.assert_not_called()
        self.assertEqual(published, 1)

    def test_publish_valmer_asset_snapshots_uses_supported_idempotent_mode(self):
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "nombrecompleto": "BONOS 241205 FULL NAME",
                }
            ]
        )
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )
        snapshot_node = Mock()
        snapshot_node.table_update = None
        snapshot_node.set_snapshots.return_value = snapshot_node
        snapshot_node.run.return_value = (False, pd.DataFrame([{"name": "BONOS 241205 FULL NAME"}]))

        with patch("valmer_connectors.data_nodes.nodes.AssetSnapshot") as asset_snapshot_class:
            asset_snapshot_class.return_value = snapshot_node

            published = _publish_valmer_asset_snapshots(
                latest,
                {"M_BONOS_241205": asset},
                logger=Mock(),
            )

        snapshot_node.get_df_between_dates.assert_not_called()
        snapshot_node.set_snapshots.assert_called_once_with(
            [
                {
                    "time_index": pd.Timestamp("2024-01-02T23:59:59Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "name": "BONOS 241205 FULL NAME",
                }
            ],
            verify_existing=False,
        )
        snapshot_node.run.assert_called_once_with(force_update=True)
        self.assertEqual(published, 1)

    def test_current_pricing_face_value_resolver_uses_projection_query(self):
        asset_uid = uuid.uuid4()
        logger = Mock()

        with patch(
            "valmer_connectors.data_nodes.nodes.AssetCurrentPricingDetails._active_context",
            return_value=object(),
        ):
            with patch(
                "msm.repositories.crud.search_model",
                side_effect=AssertionError("full-row search_model should not be used"),
            ):
                with patch(
                    "valmer_connectors.data_nodes.nodes.compile_markets_statement",
                    return_value=object(),
                ):
                    with patch(
                        "valmer_connectors.data_nodes.nodes.execute_markets_operation",
                        return_value={
                            "rows": [
                                {
                                    "asset_uid": str(asset_uid),
                                    "instrument_dump": {"face_value": 100.0},
                                }
                            ]
                        },
                    ):
                        face_values = ImportValmer._get_current_pricing_face_values_by_uid(
                            {"M_BONOS_241205": SimpleNamespace(uid=asset_uid)},
                            batch_size=1000,
                            logger=logger,
                        )

        self.assertEqual(face_values, {"M_BONOS_241205": 100.0})

    def test_pricing_adapter_failure_detail_includes_schedule_inputs(self):
        row = pd.Series(
            {
                "fecha": pd.Timestamp("2024-09-05T00:00:00Z"),
                "unique_identifier": "F_BINVEX_24484",
                "tipovalor": "F",
                "emisora": "BINVEX",
                "serie": "24484",
                "subyacente": "TIIE",
                "monedaemision": "MPS",
                "fechaemision": pd.Timestamp("2024-01-01T00:00:00Z"),
                "fechavcto": pd.Timestamp("2024-09-06T00:00:00Z"),
                "freccpn": 28,
                "cuponesxcobrar": 100,
                "diastransccpn": 1,
                "cuponactual": 2,
                "cuponesemision": 10,
                "reglacupon": "Tasa Variable",
                "valornominalactualizado": 100.0,
            }
        )

        detail = _pricing_adapter_failure_detail(
            row,
            RuntimeError("Failed to insert extra dates."),
        )

        self.assertIn("F_BINVEX_24484", detail)
        self.assertIn("schedule reconciliation failed", detail)
        self.assertIn("cuponesxcobrar", detail)
        self.assertIn("fechavcto", detail)
        self.assertIn("schedule_inputs", detail)

    def test_read_debug_artifact_file_stages_local_file_and_reads_excel_once(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_path = Path(temp_dir) / "VectorAnalitico24h_2024-12-03.xls"
            source_path.write_bytes(b"fake excel bytes")
            read_paths: list[Path] = []

            def fake_read_excel(path, **_kwargs):
                read_path = Path(path)
                read_paths.append(read_path)
                self.assertNotEqual(read_path, source_path)
                return pd.DataFrame(
                    {
                        "EMISORA": ["NA", ""],
                        "TIPO VALOR": ["M", "BI"],
                        "SERIE": ["1", "2"],
                        "OTHER": ["NA", "value"],
                    }
                )

            with patch(
                "valmer_connectors.data_nodes.nodes.pd.read_excel",
                side_effect=fake_read_excel,
            ):
                frame = ImportValmer._read_debug_artifact_file(
                    source_path,
                    logger=Mock(),
                )

        self.assertEqual(len(read_paths), 1)
        self.assertEqual(frame.loc[0, "EMISORA"], "NA")
        self.assertTrue(pd.isna(frame.loc[1, "EMISORA"]))
        self.assertTrue(pd.isna(frame.loc[0, "OTHER"]))

    def test_read_debug_artifact_files_propagates_cloud_timeout_files(self):
        paths = [Path("bad.xls"), Path("good.xls")]
        good_frame = pd.DataFrame({"EMISORA": ["ABC"]})

        def fake_read(path, _logger):
            if path.name == "bad.xls":
                raise RuntimeError("stage failed") from TimeoutError("timed out")
            return good_frame

        with patch.object(ImportValmer, "_read_debug_artifact_file", side_effect=fake_read):
            with self.assertRaisesRegex(RuntimeError, "stage failed"):
                ImportValmer._read_debug_artifact_files(paths, logger=Mock())

    def test_pricing_refresh_decision_uses_face_values(self):
        target_bonds = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "valornominalactualizado": 100.0,
                },
                {
                    "unique_identifier": "LD_BONDESD_250101",
                    "valornominalactualizado": 99.5,
                },
            ]
        )

        refreshes = ImportValmer._get_pricing_refresh_uids(
            ["M_BONOS_241205", "LD_BONDESD_250101"],
            {
                "M_BONOS_241205": SimpleNamespace(uid=uuid.uuid4()),
                "LD_BONDESD_250101": SimpleNamespace(uid=uuid.uuid4()),
            },
            {
                "M_BONOS_241205": 100.0,
                "LD_BONDESD_250101": 100.0,
            },
            target_bonds,
            logger=Mock(),
        )

        self.assertEqual(refreshes, ["LD_BONDESD_250101"])

    def test_persist_valmer_pricing_details_batches_by_pricing_date(self):
        asset_a = SimpleNamespace(uid=uuid.uuid4(), unique_identifier="M_BONOS_241205")
        asset_b = SimpleNamespace(uid=uuid.uuid4(), unique_identifier="LD_BONDESD_250101")
        instrument_a = object()
        instrument_b = object()
        details = {
            "M_BONOS_241205": {
                "instrument": instrument_a,
                "pricing_details_date": pd.Timestamp("2024-01-02T00:00:00Z"),
            },
            "LD_BONDESD_250101": {
                "instrument": instrument_b,
                "pricing_details_date": pd.Timestamp("2024-01-03T00:00:00Z"),
            },
        }

        with patch(
            "valmer_connectors.data_nodes.nodes.add_many_pricing_details",
            side_effect=[
                SimpleNamespace(pricing_details=[object()], updated_current_count=1),
                SimpleNamespace(pricing_details=[object()], updated_current_count=1),
            ],
        ) as add_many:
            persisted_uids = _persist_valmer_pricing_details_batch(
                assets_for_update={
                    "M_BONOS_241205": asset_a,
                    "LD_BONDESD_250101": asset_b,
                },
                instrument_pricing_detail_map=details,
                batch_size=5000,
                logger=Mock(),
            )

        self.assertEqual(persisted_uids, ["M_BONOS_241205", "LD_BONDESD_250101"])
        self.assertEqual(add_many.call_count, 2)
        first_items = add_many.call_args_list[0].args[0]
        self.assertEqual(add_many.call_args_list[0].kwargs["batch_size"], 5000)
        self.assertEqual(
            add_many.call_args_list[0].kwargs["pricing_details_date"].isoformat(),
            "2024-01-02T00:00:00+00:00",
        )
        self.assertEqual(
            first_items,
            [
                {
                    "asset": asset_a,
                    "instrument": instrument_a,
                    "source": "valmer",
                    "metadata_json": {"valmer_unique_identifier": "M_BONOS_241205"},
                }
            ],
        )

    def test_persist_valmer_pricing_details_raises_on_incomplete_batch_result(self):
        asset_a = SimpleNamespace(uid=uuid.uuid4(), unique_identifier="M_BONOS_241205")
        asset_b = SimpleNamespace(uid=uuid.uuid4(), unique_identifier="LD_BONDESD_250101")
        details = {
            "M_BONOS_241205": {
                "instrument": object(),
                "pricing_details_date": pd.Timestamp("2024-01-02T00:00:00Z"),
            },
            "LD_BONDESD_250101": {
                "instrument": object(),
                "pricing_details_date": pd.Timestamp("2024-01-02T00:00:00Z"),
            },
        }

        with patch(
            "valmer_connectors.data_nodes.nodes.add_many_pricing_details",
            return_value=SimpleNamespace(
                pricing_details=[object()],
                updated_current_count=1,
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "submitted 2 items, returned 1 rows",
            ):
                _persist_valmer_pricing_details_batch(
                    assets_for_update={
                        "M_BONOS_241205": asset_a,
                        "LD_BONDESD_250101": asset_b,
                    },
                    instrument_pricing_detail_map=details,
                    batch_size=5000,
                    logger=Mock(),
                )

    def test_sync_asset_registry_defaults_to_pricing_target_registration_scope(self):
        asset_uid = uuid.uuid4()
        asset = SimpleNamespace(
            uid=asset_uid,
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                    "tasacupon": 10.0,
                },
                {
                    "unique_identifier": "X_OTHER_1",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                },
            ]
        )
        target_bonds = latest.iloc[[0]].copy()
        logger = Mock()

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            resolve_refs = stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={},
                )
            )
            upsert_assets = stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._upsert_asset_table_rows",
                    return_value={"M_BONOS_241205": asset},
                )
            )
            upsert_details = stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_asset_details",
                    return_value=[],
                )
            )
            publish_snapshots = stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._publish_valmer_asset_snapshots",
                    return_value=1,
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    side_effect=[{}, {"M_BONOS_241205": 100.0}],
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.get_instrument_conventions",
                    return_value=(object(), object(), 1, object()),
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.build_qll_bond_from_row",
                    return_value=object(),
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.add_many_pricing_details",
                    return_value=SimpleNamespace(
                        pricing_details=[object()],
                        updated_current_count=1,
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=logger,
                )
            )

            asset_scope = ImportValmer._sync_asset_registry_and_pricing(
                node,
                ["M_BONOS_241205", "X_OTHER_1"],
                latest,
                target_bonds,
            )

        resolve_refs.assert_called_once_with(
            ["M_BONOS_241205", "X_OTHER_1"],
            batch_size=1000,
            logger=logger,
        )
        upsert_assets.assert_called_once_with(
            {"M_BONOS_241205": ASSET_TYPE_BOND},
            batch_size=1000,
            logger=logger,
        )
        self.assertEqual(
            upsert_details.call_args.args[0]["unique_identifier"].tolist(),
            ["M_BONOS_241205"],
        )
        self.assertEqual(
            list(publish_snapshots.call_args.args[1]),
            ["M_BONOS_241205"],
        )
        self.assertEqual(asset_scope, [asset])

    def test_sync_asset_registry_publishes_existing_priced_non_hydration_asset(self):
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="X_OTHER_1",
            asset_type="fund",
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "X_OTHER_1",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                }
            ]
        )
        target_bonds = pd.DataFrame(columns=["unique_identifier"])

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={
                        "X_OTHER_1": SimpleNamespace(
                            asset_type="fund",
                            as_asset=lambda: asset,
                        )
                    },
                )
            )
            upsert_assets = stack.enter_context(
                patch("valmer_connectors.data_nodes.nodes._upsert_asset_table_rows")
            )
            publish_snapshots = stack.enter_context(
                patch("valmer_connectors.data_nodes.nodes._publish_valmer_asset_snapshots")
            )
            upsert_details = stack.enter_context(
                patch("valmer_connectors.data_nodes.nodes.upsert_valmer_asset_details")
            )
            build_instrument = stack.enter_context(
                patch("valmer_connectors.data_nodes.nodes.build_qll_bond_from_row")
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    return_value={"X_OTHER_1": 100.0},
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            asset_scope = ImportValmer._sync_asset_registry_and_pricing(
                node,
                ["X_OTHER_1"],
                latest,
                target_bonds,
            )

        self.assertEqual(asset_scope, [asset])
        self.assertEqual(node._publication_unique_identifiers, {"X_OTHER_1"})
        upsert_assets.assert_not_called()
        publish_snapshots.assert_not_called()
        upsert_details.assert_not_called()
        build_instrument.assert_not_called()

    def test_sync_asset_registry_drops_existing_asset_without_pricing_details(self):
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="X_OTHER_1",
            asset_type="fund",
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "X_OTHER_1",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                }
            ]
        )
        target_bonds = pd.DataFrame(columns=["unique_identifier"])

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={
                        "X_OTHER_1": SimpleNamespace(
                            asset_type="fund",
                            as_asset=lambda: asset,
                        )
                    },
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    return_value={},
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            asset_scope = ImportValmer._sync_asset_registry_and_pricing(
                node,
                ["X_OTHER_1"],
                latest,
                target_bonds,
            )

        self.assertEqual(asset_scope, [])
        self.assertEqual(node._publication_unique_identifiers, set())

    def test_sync_asset_registry_raises_when_target_row_has_no_asset_classification(self):
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="X_UNKNOWN_1",
            asset_type="future",
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "X_UNKNOWN_1",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                }
            ]
        )
        logger = Mock()

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={
                        "X_UNKNOWN_1": SimpleNamespace(
                            asset_type="future",
                            as_asset=lambda: asset,
                        )
                    },
                )
            )
            upsert_assets = stack.enter_context(
                patch("valmer_connectors.data_nodes.nodes._upsert_asset_table_rows")
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=logger,
                )
            )

            with self.assertRaisesRegex(RuntimeError, "missing an explicit asset classification"):
                ImportValmer._sync_asset_registry_and_pricing(
                    node,
                    ["X_UNKNOWN_1"],
                    latest,
                    latest,
                )

        upsert_assets.assert_not_called()

    def test_sync_asset_registry_repairs_asset_type_conflict_when_classifier_matches(self):
        old_asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="IM_BPAG28_271104",
            asset_type="future",
        )
        repaired_asset = SimpleNamespace(
            uid=old_asset.uid,
            unique_identifier="IM_BPAG28_271104",
            asset_type=ASSET_TYPE_BOND,
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "IM_BPAG28_271104",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                    "tipovalor": "IM",
                    "emisora": "BPAG28",
                    "serie": "271104",
                    "monedaemision": "MPS",
                    "fechaemision": "2020-01-01",
                }
            ]
        )
        logger = Mock()

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={
                        "IM_BPAG28_271104": SimpleNamespace(
                            asset_type="future",
                            as_asset=lambda: old_asset,
                        )
                    },
                )
            )
            upsert_assets = stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._upsert_asset_table_rows",
                    return_value={"IM_BPAG28_271104": repaired_asset},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_asset_details",
                    return_value=[],
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._publish_valmer_asset_snapshots",
                    return_value=1,
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    return_value={"IM_BPAG28_271104": 100.0},
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=logger,
                )
            )

            asset_scope = ImportValmer._sync_asset_registry_and_pricing(
                node,
                ["IM_BPAG28_271104"],
                latest,
                latest,
            )

        upsert_assets.assert_called_once_with(
            {"IM_BPAG28_271104": ASSET_TYPE_BOND},
            batch_size=1000,
            logger=logger,
        )
        self.assertEqual(asset_scope, [repaired_asset])

    def test_prepare_for_update_scopes_vector_source_to_pricing_targets_by_default(self):
        node = ImportValmer.__new__(ImportValmer)
        node.source_kind = "artifact"
        source_data = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240102",
                    "tipovalor": "M",
                    "subyacente": "Bonos M",
                    "monedaemision": "MPS",
                    "emisora": "BONOS",
                    "tasacupon": 10.0,
                    "fechaemision": "2020-01-01",
                },
                {
                    "unique_identifier": "X_OTHER_1",
                    "fecha": "20240102",
                    "tipovalor": "X",
                    "subyacente": "Other",
                    "monedaemision": "MPS",
                    "emisora": "OTHER",
                    "fechaemision": "2020-01-01",
                },
            ]
        )
        node.source_data = source_data
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "prepare_source_data",
                    return_value=source_data,
                )
            )
            sync = stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_sync_asset_registry_and_pricing",
                    return_value=[asset],
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            asset_scope = ImportValmer.prepare_for_update(node)

        self.assertEqual(asset_scope, [asset])
        self.assertEqual(node.source_data["unique_identifier"].tolist(), ["M_BONOS_241205"])
        sync.assert_called_once()
        self.assertNotIn("register_pricing_target_assets_only", sync.call_args.kwargs)

    def test_prepare_for_update_uses_force_pricing_patch_env(self):
        node = ImportValmer.__new__(ImportValmer)
        node.source_kind = "artifact"
        source_data = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240102",
                    "tipovalor": "M",
                    "subyacente": "Bonos M",
                    "monedaemision": "MPS",
                    "emisora": "BONOS",
                    "tasacupon": 10.0,
                    "fechaemision": "2020-01-01",
                },
            ]
        )
        node.source_data = source_data
        asset = SimpleNamespace(
            uid=uuid.uuid4(),
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch.dict(
                    os.environ,
                    {"VALMER_FORCE_PRICING_DETAILS_PATCH": "1"},
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "prepare_source_data",
                    return_value=source_data,
                )
            )
            sync = stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_sync_asset_registry_and_pricing",
                    return_value=[asset],
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            ImportValmer.prepare_for_update(node)

        self.assertTrue(sync.call_args.kwargs["force_update"])

    def test_prepare_source_data_bypasses_artifact_cursor_filter_from_env(self):
        node = ImportValmer.__new__(ImportValmer)
        node.source_kind = "artifact"
        node.source_data = None
        node.artifact_data = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": "20240102",
                },
            ]
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch.dict(
                    os.environ,
                    {"VALMER_VECTOR_BYPASS_CURSOR_FILTER": "1"},
                )
            )
            stack.enter_context(patch.object(ImportValmer, "_set_artifact_data"))
            cursor_filter = stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_filter_source_rows_from_last_vector_observation",
                    side_effect=AssertionError("cursor filter should be bypassed"),
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            result = ImportValmer.prepare_source_data(node)

        cursor_filter.assert_not_called()
        self.assertEqual(result["unique_identifier"].tolist(), ["M_BONOS_241205"])

    def test_sync_asset_registry_raises_when_current_pricing_persist_fails(self):
        asset_uid = uuid.uuid4()
        asset = SimpleNamespace(
            uid=asset_uid,
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                    "tasacupon": 10.0,
                }
            ]
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._upsert_asset_table_rows",
                    return_value={"M_BONOS_241205": asset},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_asset_details",
                    return_value=[],
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.get_instrument_conventions",
                    return_value=(object(), object(), 1, object()),
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.build_qll_bond_from_row",
                    return_value=object(),
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.add_many_pricing_details",
                    side_effect=RuntimeError("backend insert failed"),
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            with self.assertRaisesRegex(RuntimeError, "backend insert failed"):
                ImportValmer._sync_asset_registry_and_pricing(
                    node,
                    ["M_BONOS_241205"],
                    latest,
                    latest,
                )

    def test_sync_asset_registry_raises_when_pricing_instrument_build_fails(self):
        asset_uid = uuid.uuid4()
        asset = SimpleNamespace(
            uid=asset_uid,
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                    "tipovalor": "M",
                    "emisora": "BONOS",
                    "serie": "241205",
                    "tasacupon": 10.0,
                }
            ]
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._upsert_asset_table_rows",
                    return_value={"M_BONOS_241205": asset},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_asset_details",
                    return_value=[],
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.get_instrument_conventions",
                    side_effect=RuntimeError("invalid source schedule"),
                )
            )
            add_many = stack.enter_context(
                patch("valmer_connectors.data_nodes.nodes.add_many_pricing_details")
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "M_BONOS_241205.*instrument build failures.*invalid source schedule",
            ):
                ImportValmer._sync_asset_registry_and_pricing(
                    node,
                    ["M_BONOS_241205"],
                    latest,
                    latest,
                )

        add_many.assert_not_called()

    def test_sync_asset_registry_raises_when_current_pricing_readback_is_missing(self):
        asset_uid = uuid.uuid4()
        asset = SimpleNamespace(
            uid=asset_uid,
            unique_identifier="M_BONOS_241205",
            asset_type=ASSET_TYPE_BOND,
        )
        node = ImportValmer.__new__(ImportValmer)
        latest = pd.DataFrame(
            [
                {
                    "unique_identifier": "M_BONOS_241205",
                    "fecha": pd.Timestamp("2024-01-02T00:00:00Z"),
                    "valornominalactualizado": 100.0,
                }
            ]
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_meta_operation_batch_size",
                    return_value=1000,
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.resolve_valmer_asset_refs",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes._upsert_asset_table_rows",
                    return_value={"M_BONOS_241205": asset},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_asset_details",
                    return_value=[],
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "_get_current_pricing_face_values_by_uid",
                    return_value={},
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.get_instrument_conventions",
                    return_value=(object(), object(), 1, object()),
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.build_qll_bond_from_row",
                    return_value=object(),
                )
            )
            stack.enter_context(
                patch(
                    "valmer_connectors.data_nodes.nodes.add_many_pricing_details",
                    return_value=SimpleNamespace(
                        pricing_details=[object()],
                        updated_current_count=1,
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    ImportValmer,
                    "logger",
                    new_callable=PropertyMock,
                    return_value=Mock(),
                )
            )

            with self.assertRaisesRegex(RuntimeError, "M_BONOS_241205"):
                ImportValmer._sync_asset_registry_and_pricing(
                    node,
                    ["M_BONOS_241205"],
                    latest,
                    latest,
                )


if __name__ == "__main__":
    unittest.main()
