import sqlite3
import tempfile
import unittest
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from metatables.migrations import alembic_config_for_provider, namespace_version_location
from msm.base import markets_table_name
from msm.data_nodes.indices import index_values_output_table_name
from msm.models.assets import AssetTable
from msm.models.index_formulas import IndexFormulaDefinitionTable
from msm.models.indices import IndexTable
from msm.settings import markets_auto_register_namespace
from msm_pricing.models.curves import CurveTable
from sqlalchemy import create_engine

from valmer_connectors.data_nodes.canonical_index_values import (
    DailyIndexValuesStorage,
)
from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.markets import (
    VALMER_MARKETS_NAMESPACE,
    VALMER_MARKETS_STORAGE_APP,
)
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable
from valmer_connectors.migrations import (
    VALMER_MIGRATION_MODELS,
    VALMER_REFERENCE_MODELS,
    VALMER_TABLE_APP,
    ValmerAlembicVersion,
    migration,
)
from valmer_connectors.migrations.registry import (
    METATABLE_PROVIDER_MODELS,
    metatable_provider_models,
)
from valmer_connectors.storage import (
    valmer_discount_curves_instance,
    valmer_index_fixings_instance,
)


class ValmerMigrationProviderTests(unittest.TestCase):
    def test_provider_uses_real_project_package(self):
        expected_version_location = namespace_version_location(
            VALMER_MARKETS_NAMESPACE,
            prefix="valmer_connectors.migrations:versions",
        )

        self.assertEqual(migration.package, "valmer_connectors")
        self.assertEqual(migration.migration_namespace, VALMER_MARKETS_NAMESPACE)
        self.assertEqual(migration.script_location, "valmer_connectors.migrations:")
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
            (
                ValmerAssetDetailsTable,
                ValmerVectorPricesStorage,
                DailyIndexValuesStorage,
                valmer_discount_curves_instance(),
                valmer_index_fixings_instance(),
            ),
        )
        self.assertEqual(METATABLE_PROVIDER_MODELS, VALMER_MIGRATION_MODELS)

    def test_provider_registers_only_project_models(self):
        self.assertEqual(
            tuple(migration.metatable_models),
            (
                ValmerAssetDetailsTable,
                ValmerVectorPricesStorage,
                DailyIndexValuesStorage,
                valmer_discount_curves_instance(),
                valmer_index_fixings_instance(),
            ),
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
            DailyIndexValuesStorage.__table__.name,
            index_values_output_table_name(cadence="1d"),
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
            DailyIndexValuesStorage.__metatable_identifier__,
            "IndexValuesTS.1d",
        )
        self.assertEqual(
            ValmerAssetDetailsTable.__table__.info["namespace"],
            VALMER_MARKETS_NAMESPACE,
        )
        self.assertEqual(
            ValmerVectorPricesStorage.__table__.info["namespace"],
            VALMER_MARKETS_NAMESPACE,
        )
        self.assertEqual(
            DailyIndexValuesStorage.__table__.info["namespace"],
            "mainsequence.markets",
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
                DailyIndexValuesStorage.__table__.name,
                "valmer_connectors__discountcurvests__valmer",
                "valmer_connectors__indexfixingsts__valmer",
                AssetTable.__table__.name,
                IndexTable.__table__.name,
                IndexFormulaDefinitionTable.__table__.name,
                CurveTable.__table__.name,
            },
        )
        self.assertIn(AssetTable.__table__.name, target_table_names)
        self.assertIn(IndexTable.__table__.name, target_table_names)
        self.assertEqual(
            VALMER_REFERENCE_MODELS,
            (AssetTable, IndexTable, IndexFormulaDefinitionTable, CurveTable),
        )
        self.assertFalse(
            migration.include_name(CurveTable.__table__.name, "table", {"schema_name": None})
        )
        self.assertNotIn(IndexTable, VALMER_MIGRATION_MODELS)

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
        index_table = IndexTable.__table__
        self.assertFalse(
            migration.include_name(
                index_table.name,
                "table",
                {"schema_name": index_table.schema},
            )
        )


ASSET_DETAILS_TABLE = "valmer_connectors__valmerassetdetails"


class ValmerAssetDetailsCascadeTests(unittest.TestCase):
    """A Valmer details row belongs to its Asset: deleting the Asset deletes it.

    MetaTables registers cascading foreign keys since 0.1.17, so the model and
    revision 0001 keep ON DELETE CASCADE.
    """

    def test_model_cascades_asset_deletes_to_details(self):
        (key,) = ValmerAssetDetailsTable.__table__.c.asset_uid.foreign_keys

        self.assertEqual(key.target_fullname, f"{AssetTable.__table__.fullname}.uid")
        self.assertEqual(key.ondelete, "CASCADE")

    def test_revision_0001_deletes_details_with_their_asset_on_sqlite(self):
        scripts = ScriptDirectory.from_config(
            alembic_config_for_provider(migration, sqlalchemy_url="sqlite://")
        )
        self.assertEqual(scripts.get_heads(), ["0002"])
        baseline = scripts.get_revision("0001").module
        asset_uid = "a" * 32

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "valmer.sqlite"
            engine = create_engine(f"sqlite:///{database}")
            try:
                with engine.begin() as connection:
                    connection.exec_driver_sql(
                        "CREATE TABLE ms_markets__asset ("
                        "uid CHAR(32) PRIMARY KEY, unique_identifier VARCHAR(255) NOT NULL UNIQUE)"
                    )
                    with Operations.context(MigrationContext.configure(connection)):
                        baseline.upgrade()
            finally:
                engine.dispose()

            connection = sqlite3.connect(database)
            try:
                # PRAGMA foreign_key_list rows: id, seq, table, from, to,
                # on_update, on_delete, match.
                (key,) = connection.execute(
                    f"PRAGMA foreign_key_list({ASSET_DETAILS_TABLE})"
                ).fetchall()
                self.assertEqual(
                    key[2:7], ("ms_markets__asset", "asset_uid", "uid", "NO ACTION", "CASCADE")
                )

                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute(
                    "INSERT INTO ms_markets__asset VALUES (?, 'VALMER_ASSET')", (asset_uid,)
                )
                connection.execute(
                    f"INSERT INTO {ASSET_DETAILS_TABLE} (asset_uid, valmer_unique_identifier) "
                    "VALUES (?, 'V1')",
                    (asset_uid,),
                )
                connection.commit()
                connection.execute("DELETE FROM ms_markets__asset WHERE uid = ?", (asset_uid,))
                connection.commit()

                self.assertEqual(
                    connection.execute(f"SELECT COUNT(*) FROM {ASSET_DETAILS_TABLE}").fetchone(),
                    (0,),
                )
            finally:
                connection.close()



class ValmerTimeSeriesInstanceMigrationTests(unittest.TestCase):
    """Revision 0002 creates Valmer's curve and fixing instances in its namespace."""

    def test_instances_copy_the_core_storage_contracts(self):
        from msm_pricing.time_index.curves.storage import DiscountCurvesStorage
        from msm_pricing.time_index.index_fixings.storage import IndexFixingsStorage

        for core, instance, name, identifier in (
            (
                DiscountCurvesStorage,
                valmer_discount_curves_instance(),
                "valmer_connectors__discountcurvests__valmer",
                "valmer_connectors.DiscountCurvesTS.valmer",
            ),
            (
                IndexFixingsStorage,
                valmer_index_fixings_instance(),
                "valmer_connectors__indexfixingsts__valmer",
                "valmer_connectors.IndexFixingsTS.valmer",
            ),
        ):
            with self.subTest(table=name):
                table = instance.__table__
                self.assertEqual(table.name, name)
                self.assertEqual(table.info["namespace"], VALMER_MARKETS_NAMESPACE)
                self.assertEqual(table.info["identifier"], identifier)
                self.assertEqual(
                    [column.name for column in table.columns],
                    [column.name for column in core.__table__.columns],
                )
                self.assertEqual(instance.__index_names__, core.__index_names__)
                self.assertLessEqual(len(name), 63)

    def test_revision_0002_creates_both_instances_on_sqlite(self):
        scripts = ScriptDirectory.from_config(
            alembic_config_for_provider(migration, sqlalchemy_url="sqlite://")
        )
        instances = scripts.get_revision("0002").module
        self.assertEqual(scripts.get_revision("0002").down_revision, "0001")

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "valmer.sqlite"
            engine = create_engine(f"sqlite:///{database}")
            try:
                with engine.begin() as connection:
                    connection.exec_driver_sql(
                        "CREATE TABLE ms_markets__curve (unique_identifier VARCHAR(255) UNIQUE)"
                    )
                    connection.exec_driver_sql(
                        "CREATE TABLE ms_markets__index (unique_identifier VARCHAR(255) UNIQUE)"
                    )
                    with Operations.context(MigrationContext.configure(connection)):
                        instances.upgrade()
            finally:
                engine.dispose()

            connection = sqlite3.connect(database)
            try:
                for table, target in (
                    ("valmer_connectors__discountcurvests__valmer", "ms_markets__curve"),
                    ("valmer_connectors__indexfixingsts__valmer", "ms_markets__index"),
                ):
                    (key,) = connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
                    self.assertEqual(key[2], target)
                    self.assertEqual(key[6], "RESTRICT")
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
