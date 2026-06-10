import unittest

from mainsequence.meta_tables.migrations import (
    namespace_version_location,
)
from msm.base import markets_table_name
from msm.models.assets import AssetTable
from msm.settings import markets_auto_register_namespace

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.markets import (
    VALMER_MARKETS_NAMESPACE,
    VALMER_MARKETS_STORAGE_APP,
)
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from migrations import (
    VALMER_TABLE_APP,
    VALMER_MIGRATION_MODELS,
    ValmerAlembicVersion,
    migration,
)
from migrations.registry import METATABLE_PROVIDER_MODELS, metatable_provider_models


class ValmerMigrationProviderTests(unittest.TestCase):
    def test_provider_uses_real_project_package(self):
        expected_version_location = namespace_version_location(
            VALMER_MARKETS_NAMESPACE,
            prefix="migrations:versions",
        )

        self.assertEqual(migration.package, "valmer_connectors")
        self.assertEqual(migration.migration_namespace, VALMER_MARKETS_NAMESPACE)
        self.assertEqual(migration.script_location, "migrations:")
        self.assertEqual(
            migration.resolved_version_locations(),
            (expected_version_location,),
        )
        self.assertEqual(
            migration.resolved_version_path(),
            expected_version_location,
        )
        self.assertEqual(
            ValmerAlembicVersion.__alembic_version_table_name__,
            markets_table_name(
                VALMER_TABLE_APP,
                "alembic_version",
                suffix=markets_auto_register_namespace(),
            ),
        )
        self.assertIsNone(ValmerAlembicVersion.__alembic_version_schema__)

    def test_registry_registers_only_project_models(self):
        self.assertEqual(
            tuple(metatable_provider_models()),
            (ValmerAssetDetailsTable, ValmerVectorPricesStorage),
        )
        self.assertEqual(METATABLE_PROVIDER_MODELS, VALMER_MIGRATION_MODELS)

    def test_provider_registers_only_project_models(self):
        self.assertEqual(
            tuple(migration.metatable_models),
            (ValmerAssetDetailsTable, ValmerVectorPricesStorage),
        )
        self.assertEqual(tuple(migration.metatable_models), VALMER_MIGRATION_MODELS)
        for model in VALMER_MIGRATION_MODELS:
            with self.subTest(model=model.__name__):
                self.assertIsNone(model.__table__.schema)

        self.assertEqual(
            ValmerAssetDetailsTable.__table__.name,
            markets_table_name(
                VALMER_MARKETS_STORAGE_APP,
                ValmerAssetDetailsTable.__markets_authored_identifier__,
                suffix=markets_auto_register_namespace(),
            ),
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
            ValmerAssetDetailsTable.__metatable_identifier__,
            f"{VALMER_MARKETS_NAMESPACE}.ValmerAssetDetails",
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__metatable_identifier__,
            f"{VALMER_MARKETS_NAMESPACE}.vector_de_precios_valmer",
        )
        self.assertEqual(
            ValmerAssetDetailsTable.__table__.info["namespace"],
            VALMER_MARKETS_NAMESPACE,
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__table__.info["namespace"],
            VALMER_MARKETS_NAMESPACE,
        )

    def test_provider_metadata_includes_core_markets_reference_tables(self):
        target_table_names = {
            table.name for table in migration.target_metadata.tables.values()
        }
        self.assertEqual(
            target_table_names,
            {
                ValmerAssetDetailsTable.__table__.name,
                ValmerVectorPricesStorage.__table__.name,
                AssetTable.__table__.name,
            },
        )
        self.assertIn(AssetTable.__table__.name, target_table_names)

    def test_provider_default_include_excludes_core_markets_tables(self):
        self.assertIsNone(migration.after_register_metatables)

        for model in VALMER_MIGRATION_MODELS:
            table = model.__table__
            self.assertTrue(
                migration.include_name(
                    table.name,
                    "table",
                    {"schema_name": table.schema},
                )
            )

        asset_table = AssetTable.__table__
        self.assertFalse(
            migration.include_name(
                asset_table.name,
                "table",
                {"schema_name": asset_table.schema},
            )
        )


if __name__ == "__main__":
    unittest.main()
