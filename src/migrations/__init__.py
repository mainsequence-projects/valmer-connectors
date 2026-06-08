from __future__ import annotations

from mainsequence.meta_tables.migrations import (
    build_alembic_version_metatable,
    build_metatable_migration_provider,
    metadata_for_models,
)
from msm.base import MARKETS_SCHEMA, markets_table_name
from msm.settings import (
    markets_auto_register_namespace,
    markets_identifier,
    markets_namespace,
)

from migrations.registry import metatable_provider_models
from valmer_connectors.markets import VALMER_MARKETS_STORAGE_APP

VALMER_MIGRATION_MODELS = tuple(metatable_provider_models())

VALMER_TABLE_APP = VALMER_MARKETS_STORAGE_APP

ValmerAlembicVersion = build_alembic_version_metatable(
    class_name="ValmerAlembicVersion",
    namespace=markets_namespace(),
    identifier=markets_identifier("valmer.alembic_version"),
    schema=MARKETS_SCHEMA,
    table_name=markets_table_name(
        VALMER_TABLE_APP,
        "alembic_version",
        suffix=markets_auto_register_namespace(),
    ),
)


migration = build_metatable_migration_provider(
    package="valmer_connectors",
    migration_namespace=markets_namespace(),
    script_location="migrations:",
    target_metadata=metadata_for_models(VALMER_MIGRATION_MODELS),
    alembic_registry=ValmerAlembicVersion,
    metatable_models=VALMER_MIGRATION_MODELS,
)


__all__ = [
    "VALMER_TABLE_APP",
    "VALMER_MIGRATION_MODELS",
    "ValmerAlembicVersion",
    "migration",
]
