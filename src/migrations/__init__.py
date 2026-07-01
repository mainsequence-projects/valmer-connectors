from __future__ import annotations

from msm.base import MARKETS_SCHEMA, markets_table_name
from msm.models.assets import AssetTable
from msm.settings import (
    markets_auto_register_namespace,
    markets_identifier,
)

from mainsequence.meta_tables.migrations import (
    build_alembic_version_metatable,
    build_metatable_migration_provider,
    metadata_for_models,
)
from migrations.registry import metatable_provider_models
from valmer_connectors.markets import (
    VALMER_MARKETS_NAMESPACE,
    VALMER_MARKETS_STORAGE_APP,
)

VALMER_MIGRATION_MODELS = tuple(metatable_provider_models())
VALMER_REFERENCE_MODELS = (AssetTable,)
VALMER_TARGET_METADATA = metadata_for_models(
    (*VALMER_MIGRATION_MODELS, *VALMER_REFERENCE_MODELS)
)

VALMER_TABLE_APP = VALMER_MARKETS_STORAGE_APP


def _model_table_names(models: tuple[type[object], ...]) -> set[str]:
    return {
        table_name
        for model in models
        if (table_name := getattr(getattr(model, "__table__", None), "name", None))
    }


def _model_table_fullnames(models: tuple[type[object], ...]) -> set[str]:
    return {
        table_fullname
        for model in models
        if (
            table_fullname := getattr(
                getattr(model, "__table__", None), "fullname", None
            )
        )
    }


VALMER_OWNED_TABLE_NAMES = _model_table_names(VALMER_MIGRATION_MODELS)
VALMER_OWNED_TABLE_FULLNAMES = _model_table_fullnames(VALMER_MIGRATION_MODELS)


def _include_valmer_name(
    name: str | None,
    type_: str,
    parent_names: dict[str, object],
) -> bool:
    if type_ != "table":
        return True
    schema_name = parent_names.get("schema_name") if parent_names else None
    qualified_name = f"{schema_name}.{name}" if schema_name else str(name)
    return (
        str(name) in VALMER_OWNED_TABLE_NAMES
        or qualified_name in VALMER_OWNED_TABLE_FULLNAMES
    )


def _include_valmer_object(
    object_: object,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: object | None,
) -> bool:
    if type_ != "table":
        return True
    return _include_valmer_name(
        name,
        type_,
        {"schema_name": getattr(object_, "schema", None)},
    )

ValmerAlembicVersion = build_alembic_version_metatable(
    class_name="ValmerAlembicVersion",
    namespace=VALMER_MARKETS_NAMESPACE,
    identifier=markets_identifier(
        "valmer.alembic_version",
        namespace=VALMER_MARKETS_NAMESPACE,
    ),
    schema=MARKETS_SCHEMA,
    table_name=markets_table_name(
        VALMER_TABLE_APP,
        "alembic_version",
        suffix=markets_auto_register_namespace(),
    ),
)


migration = build_metatable_migration_provider(
    package="valmer_connectors",
    migration_namespace=VALMER_MARKETS_NAMESPACE,
    script_location="migrations:",
    target_metadata=VALMER_TARGET_METADATA,
    alembic_registry=ValmerAlembicVersion,
    metatable_models=VALMER_MIGRATION_MODELS,
    include_name_hook=_include_valmer_name,
    include_object_hook=_include_valmer_object,
)


__all__ = [
    "VALMER_TABLE_APP",
    "VALMER_MIGRATION_MODELS",
    "VALMER_REFERENCE_MODELS",
    "VALMER_TARGET_METADATA",
    "ValmerAlembicVersion",
    "migration",
]
