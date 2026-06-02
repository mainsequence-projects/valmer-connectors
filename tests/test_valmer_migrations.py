import unittest

from msm.models.assets import AssetTable

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.migrations import (
    VALMER_MIGRATION_MODELS,
    include_valmer_name,
    migration,
)


class ValmerMigrationProviderTests(unittest.TestCase):
    def test_provider_uses_real_project_package(self):
        self.assertEqual(migration.package, "valmer_connectors")
        self.assertEqual(migration.script_location, "valmer_connectors:migrations")

    def test_provider_registers_only_project_models(self):
        self.assertEqual(
            tuple(migration.metatable_models),
            (ValmerAssetDetailsTable, ValmerVectorPricesStorage),
        )
        self.assertEqual(tuple(migration.metatable_models), VALMER_MIGRATION_MODELS)

    def test_include_hook_excludes_core_markets_tables(self):
        for model in VALMER_MIGRATION_MODELS:
            table = model.__table__
            self.assertTrue(
                include_valmer_name(
                    table.name,
                    "table",
                    {"schema_name": table.schema},
                )
            )

        asset_table = AssetTable.__table__
        self.assertFalse(
            include_valmer_name(
                asset_table.name,
                "table",
                {"schema_name": asset_table.schema},
            )
        )


if __name__ == "__main__":
    unittest.main()
