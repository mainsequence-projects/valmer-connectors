from contextlib import ExitStack
import uuid
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, PropertyMock, patch

import pandas as pd
from mainsequence.meta_tables.data_nodes.run_operations import UpdateRunner
from msm.base import markets_table_name
from msm.constants import ASSET_TYPE_BOND
from msm.data_nodes.assets import AssetSnapshot as CoreAssetSnapshot
from msm.models.assets import AssetTable
from msm.settings import ASSET_IDENTIFIER_DIMENSION
from msm.settings import markets_auto_register_namespace
from sqlalchemy import BigInteger, Float

from valmer_connectors.data_nodes.nodes import (
    VALMER_ASSET_DETAIL_SOURCE_COLUMNS,
    VALMER_SOURCE_COLUMN_SPECS,
    VALMER_VECTOR_COLUMN_SPECS,
    ImportValmer,
    ImportValmerConfig,
    _build_valmer_asset_snapshot_rows,
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
            "vector_de_precios_valmer",
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__table__.name,
            markets_table_name(
                VALMER_MARKETS_STORAGE_APP,
                ValmerVectorPricesStorage.__metatable_identifier__,
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
        self.assertIsInstance(ValmerVectorPricesStorage.__table__.c.volume.type, BigInteger)
        self.assertIsInstance(ValmerVectorPricesStorage.__table__.c.open_time.type, BigInteger)
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
                ValmerAssetDetailsTable.__metatable_identifier__,
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
        self.assertEqual(str(result["volume"].dtype), "Int64")
        self.assertEqual(str(result["open_time"].dtype), "Int64")
        self.assertTrue(pd.isna(result_row["days_since_coupon"]))
        self.assertTrue(pd.isna(result_row["coupons_remaining"]))
        self.assertEqual(result_row["volume"], 0)
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
        snapshot_node.get_df_between_dates.return_value = pd.DataFrame()
        snapshot_node.set_snapshots.return_value = snapshot_node
        snapshot_node.run.return_value = (None, pd.DataFrame([{"name": "BONOS 241205 FULL NAME"}]))

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
            ]
        )
        snapshot_node.run.assert_called_once_with(force_update=True)
        self.assertEqual(published, 1)

    def test_publish_valmer_asset_snapshots_skips_existing_snapshot_keys(self):
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
        snapshot_node.get_df_between_dates.return_value = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-01-02T23:59:59Z"),
                    ASSET_IDENTIFIER_DIMENSION: "M_BONOS_241205",
                    "name": "BONOS 241205 FULL NAME",
                }
            ]
        )

        with patch("valmer_connectors.data_nodes.nodes.AssetSnapshot") as asset_snapshot_class:
            asset_snapshot_class.return_value = snapshot_node
            asset_snapshot_class.build_frame.side_effect = CoreAssetSnapshot.build_frame

            published = _publish_valmer_asset_snapshots(
                latest,
                {"M_BONOS_241205": asset},
                logger=Mock(),
            )

        snapshot_node.set_snapshots.assert_not_called()
        snapshot_node.run.assert_not_called()
        self.assertEqual(published, 0)

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
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_assets",
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
                    "valmer_connectors.data_nodes.nodes.persist_current_pricing_details",
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

            with self.assertRaisesRegex(RuntimeError, "M_BONOS_241205"):
                ImportValmer._sync_asset_registry_and_pricing(
                    node,
                    ["M_BONOS_241205"],
                    latest,
                    latest,
                )

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
                    "valmer_connectors.data_nodes.nodes.upsert_valmer_assets",
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
                    "valmer_connectors.data_nodes.nodes.persist_current_pricing_details",
                    return_value=SimpleNamespace(
                        pricing_details_date=pd.Timestamp("2024-01-02T00:00:00Z")
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
