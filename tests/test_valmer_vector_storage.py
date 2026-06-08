import unittest

import pandas as pd
from msm.base import markets_table_name
from msm.models.assets import AssetTable
from msm.settings import markets_auto_register_namespace

from valmer_connectors.data_nodes.nodes import (
    VALMER_ASSET_DETAIL_SOURCE_COLUMNS,
    VALMER_SOURCE_COLUMN_SPECS,
    VALMER_VECTOR_COLUMN_SPECS,
    ImportValmer,
    ImportValmerConfig,
)
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.markets import VALMER_MARKETS_STORAGE_APP
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable


class _UpdateStatisticsStub:
    @staticmethod
    def filter_df_by_latest_value(frame):
        return frame


class ValmerVectorStorageTest(unittest.TestCase):
    def test_vector_storage_owns_import_valmer_schema(self):
        storage_columns = set(ValmerVectorPricesStorage.__table__.columns.keys())
        expected_columns = {
            "time_index",
            "unique_identifier",
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
            ["time_index", "unique_identifier"],
        )

    def test_vector_storage_links_to_asset_unique_identifier(self):
        foreign_keys = list(
            ValmerVectorPricesStorage.__table__.c.unique_identifier.foreign_keys
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
        source_row = {"unique_identifier": "M_BONOS_241205"}
        for spec in VALMER_SOURCE_COLUMN_SPECS:
            if spec.source_name is None:
                continue
            if spec.transform == "string":
                value = "MXN"
            elif spec.transform in {"float", "percent"}:
                value = "1.25"
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
            source_row.setdefault(column_name, "1.25")

        node = ImportValmer.__new__(ImportValmer)
        node.source_data = pd.DataFrame([source_row])
        node.update_statistics = _UpdateStatisticsStub()

        result = ImportValmer.update(node)

        self.assertEqual(result.index.names, ["time_index", "unique_identifier"])
        self.assertEqual(
            set(result.reset_index().columns),
            set(ValmerVectorPricesStorage.__table__.columns.keys()),
        )
        self.assertEqual(
            result.reset_index()["unique_identifier"].iloc[0], "M_BONOS_241205"
        )


if __name__ == "__main__":
    unittest.main()
