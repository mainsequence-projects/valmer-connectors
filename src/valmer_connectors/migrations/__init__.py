from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)
from msm.base import MARKETS_SCHEMA, MarketsBase
from msm.maintenance.catalog import (
    CatalogBootstrapError,
    catalog_repository_context,
    resolve_catalog_table,
    upsert_catalog_row,
)
from msm.settings import markets_identifier, markets_namespace

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable

VALMER_MIGRATION_MODELS = (
    ValmerAssetDetailsTable,
    ValmerVectorPricesStorage,
)


class ValmerAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = markets_namespace()
    __metatable_identifier__ = markets_identifier("valmer.alembic_version")
    __alembic_version_schema__ = MARKETS_SCHEMA
    __alembic_version_table_name__ = "valmer_alembic_version"
    __alembic_version_column_name__ = "version_num"


def _table_keys(model: type[Any]) -> set[str]:
    table = model.__table__
    keys = {str(table.name)}
    if table.schema:
        keys.add(f"{table.schema}.{table.name}")
    return keys


_VALMER_TABLE_KEYS = frozenset(
    key
    for model in VALMER_MIGRATION_MODELS
    for key in _table_keys(model)
)


def _matches_valmer_table(name: str | None, schema: str | None = None) -> bool:
    if not name:
        return False
    table_name = str(name)
    if table_name in _VALMER_TABLE_KEYS:
        return True
    if schema and f"{schema}.{table_name}" in _VALMER_TABLE_KEYS:
        return True
    return False


def include_valmer_name(
    name: str | None,
    type_: str,
    parent_names: Mapping[str, Any] | None,
) -> bool:
    if type_ != "table":
        return True
    schema = None
    if parent_names is not None:
        schema = parent_names.get("schema_name")
    return _matches_valmer_table(name, schema)


def include_valmer_object(
    object_: Any,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    if type_ == "table":
        return _matches_valmer_table(name, getattr(object_, "schema", None))

    parent_table = getattr(object_, "table", None)
    if parent_table is None and compare_to is not None:
        parent_table = getattr(compare_to, "table", None)
    if parent_table is None:
        return True

    return _matches_valmer_table(
        getattr(parent_table, "name", None),
        getattr(parent_table, "schema", None),
    )


def refresh_valmer_markets_catalog(
    registered_metatables: Sequence[Any],
) -> list[dict[str, Any]]:
    meta_tables = list(registered_metatables)
    if len(meta_tables) != len(VALMER_MIGRATION_MODELS):
        raise CatalogBootstrapError(
            "Valmer migration provider registered a different number of MetaTables "
            f"than VALMER_MIGRATION_MODELS. Registered={len(meta_tables)}, "
            f"expected={len(VALMER_MIGRATION_MODELS)}."
        )

    catalog_meta_table = resolve_catalog_table()
    context = catalog_repository_context(catalog_meta_table=catalog_meta_table)
    return [
        upsert_catalog_row(context, model=model, meta_table=meta_table)
        for model, meta_table in zip(VALMER_MIGRATION_MODELS, meta_tables, strict=True)
    ]


migration = AlembicMetaTableMigration(
    package="valmer_connectors",
    migration_namespace=markets_namespace(),
    script_location="valmer_connectors:migrations",
    target_metadata=MarketsBase.metadata,
    alembic_registry=ValmerAlembicVersion,
    metatable_models=VALMER_MIGRATION_MODELS,
    include_name_hook=include_valmer_name,
    include_object_hook=include_valmer_object,
    after_register_metatables=refresh_valmer_markets_catalog,
)


__all__ = [
    "VALMER_MIGRATION_MODELS",
    "ValmerAlembicVersion",
    "include_valmer_name",
    "include_valmer_object",
    "migration",
    "refresh_valmer_markets_catalog",
]
