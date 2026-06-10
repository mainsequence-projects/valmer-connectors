# ADR 0003: Adopt Migration-First MetaTable Lifecycle

## Status

Proposed

## Date

2026-06-02

## Context

Main Sequence SDK and ms-markets now treat platform-managed MetaTables and
PlatformTimeIndexMetaTable tables as migration-first resources.

The old pattern in this project was effectively:

- import project and ms-markets models,
- call bootstrap helpers such as `register_all()` or pricing schema helpers,
- let runtime startup resolve, register, or repair tables as needed,
- then seed assets, indexes, curves, pricing bindings, and publish DataNodes.

That is no longer the correct lifecycle. The current architecture separates:

1. Alembic migration and platform MetaTable catalog registration.
2. Runtime attachment to already migrated and registered tables.
3. Static domain row seeding and DataNode update execution.

The relevant Main Sequence SDK contract is now:

- `PlatformManagedMetaTable` and `PlatformTimeIndexMetaTable` models are registered
  through `AlembicMetaTableMigration`.
- Direct `Model.register()` calls are internal migration plumbing and should fail
  in normal runtime code.
- DataNode storage is not registered by constructing or updating a DataNode.
- Runtime code must attach to already registered MetaTables and fail if the
  platform catalog is missing or stale.

The relevant ms-markets contract is now:

- built-in ms-markets, msm_pricing, and portfolio models are owned by
  `msm.migrations:migration`;
- `msm.start_engine(...)` performs runtime attachment only;
- `msm.start_engine(...)` does not create schemas, run migrations, refresh catalog
  rows, or repair schema drift;
- project-local ms-markets extension tables can be passed to
  `msm.start_engine(models=[...])` only after the project migration provider has
  created and registered them.

This project currently has project-owned ms-markets extension tables:

- `src/valmer_connectors/meta_tables/valmer_asset_details.py::ValmerAssetDetailsTable`
- `src/valmer_connectors/data_nodes/valmer_vector_storage.py::ValmerVectorPricesStorage`

These are not built-in ms-markets tables. They must not be added to the core
`msm.migrations:migration` provider. They need a project migration provider.

## Architecture References Reviewed

This decision is based on the current SDK and ms-markets migration contracts,
not the older project bootstrap behavior.

Reviewed contracts:

- Main Sequence SDK MetaTable migration tutorial:
  `docs/tutorial/metatable_migrations.md`
- Main Sequence SDK ADR 0021:
  `platform-managed-metatables-migration-first`
- `mainsequence.meta_tables.migrations.AlembicMetaTableMigration`
- `mainsequence.meta_tables.migrations.build_metatable_migration_provider`
- `mainsequence.meta_tables.migrations.env.run_mainsequence_alembic_env`
- `mainsequence.meta_tables.sqlalchemy_contracts`
- `msm.migrations:migration`
- `msm.migrations.registry`
- `msm.bootstrap.start_engine`
- refreshed project skills for Main Sequence DataNodes, MetaTables,
  ms-markets bootstrap registration, and ms-markets MetaTable migrations

The important behavior confirmed from those sources is that the migration
provider is now the lifecycle boundary for SQL schema evolution, MetaTable
catalog registration, and Alembic version tracking. Runtime startup attaches to
that finalized state.

## Decision

Adopt a migration-first lifecycle for Valmer project MetaTables and DataNode
storage.

The runtime bootstrap API in this project must stop claiming that it registers
schemas. Its responsibility will be renamed and narrowed to:

- attach the ms-markets runtime to already migrated tables;
- attach project extension tables that were already migrated by the Valmer
  provider;
- seed static domain rows that are not schema objects, such as required Mexican
  reference index rows or Valmer-specific static details;
- configure pricing runtime objects only after ms-markets and project migrations
  have completed.

Schema creation, schema changes, MetaTable registration, Alembic versioning, and
MetaTable catalog refresh belong to migration providers and CLI migration
commands.

## Migration Providers

### Core ms-markets Provider

Before this project runs its own migrations, operators must run the canonical
ms-markets provider:

```bash
mainsequence migrations current --provider msm.migrations:migration
mainsequence migrations upgrade --provider msm.migrations:migration head
```

This provider owns the built-in ms-markets tables, including:

- `AssetTable`
- `IndexTypeTable`
- `IndexTable`
- `IndexConventionDetailsTable`
- `CurveTable`
- `AssetCurrentPricingDetailsTable`
- `DiscountCurvesStorage`
- `IndexFixingsStorage`
- `AssetPricingDetailsStorage`
- portfolio/account tables
- the ms-markets MetaTable catalog table

This project must not generate duplicate migrations for those tables.

### Valmer Project Provider

Add a project migration provider exposed from the importable project package:

```text
migrations:migration
```

This project uses a normal src-layout package at `src/valmer_connectors`. The
migration provider should be addressed explicitly as
`migrations:migration`, which also matches provider
auto-discovery from the distribution name `valmer-connectors`.

The provider should use the SDK-owned helper machinery and include only
project-owned models:

```python
VALMER_MIGRATION_MODELS = (
    ValmerAssetDetailsTable,
    ValmerVectorPricesStorage,
)
```

The intended registry shape is:

```python
from mainsequence.meta_tables.migrations import build_metatable_model_registry

from msm.base import MarketsBase


def _metatable_provider_model_sources() -> list[type[MarketsBase]]:
    from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
    from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable

    return [
        ValmerAssetDetailsTable,
        ValmerVectorPricesStorage,
    ]


METATABLE_PROVIDER_MODELS = tuple(
    build_metatable_model_registry(
        _metatable_provider_model_sources(),
        base=MarketsBase,
    )
)
```

The intended provider shape is:

```python
from mainsequence.meta_tables.migrations import (
    build_alembic_version_metatable,
    build_metatable_migration_provider,
    metadata_for_models,
)
from msm.base import MARKETS_SCHEMA, markets_table_name
from msm.settings import (
    markets_auto_register_namespace,
    markets_identifier,
)

from migrations.registry import metatable_provider_models
from valmer_connectors.markets import (
    VALMER_MARKETS_NAMESPACE,
    VALMER_MARKETS_STORAGE_APP,
)


VALMER_MIGRATION_MODELS = tuple(metatable_provider_models())
VALMER_TABLE_APP = VALMER_MARKETS_STORAGE_APP


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
    target_metadata=metadata_for_models(VALMER_MIGRATION_MODELS),
    alembic_registry=ValmerAlembicVersion,
    metatable_models=VALMER_MIGRATION_MODELS,
)
```

If the Python package is renamed later, the provider path, `package`, and
`script_location` values must be updated together.

The provider must keep revision files namespace-aware through
`build_metatable_migration_provider(...)`. The Valmer project extension uses
the explicit namespace `valmer_connectors`, so revisions live under
`src/migrations/versions/valmer_connectors/`. The built-in ms-markets provider
keeps the library namespace `mainsequence.markets`; project-owned Valmer
MetaTables must not use that library namespace.

The provider must register those two models with the Main Sequence platform and
must not register or mutate built-in ms-markets models.

Because the current Valmer project tables subclass `MarketsBase`, the provider
must prevent Alembic autogenerate from emitting built-in ms-markets tables. The
implementation uses `metadata_for_models(VALMER_MIGRATION_MODELS)` so the
provider target metadata contains only project-owned Valmer tables. Initial
table creation for provider-scoped platform MetaTables happens through
`metatable_models` registration in the SDK migration lifecycle, not through
hand-written `op.create_table(...)` statements. Alembic revisions should contain
explicit DDL only when a later in-place schema evolution requires it.

The project provider must not include:

- built-in ms-markets asset, index, curve, pricing, or portfolio tables;
- built-in ms-markets DataNode storage;
- any table already owned by `msm.migrations:migration`.

## Project Catalog Refresh

The upgraded ms-markets runtime resolves already-registered project extension
tables through the migration-registered physical table names and
`msm.start_engine(models=...)`. This project should not define an
`after_register_metatables` hook unless a future ms-markets API reintroduces a
project table-spec refresh requirement.

Valmer must not call built-in ms-markets migration catalog refresh logic. That
logic is reserved for the full core ms-markets migration model registry, not for
project-local extension models.

## Runtime Bootstrap Refactor

Current project bootstrap names are misleading under the new architecture.

Refactor the bootstrap surface so that code names reflect runtime attachment and
static seeding, not schema registration.

Required changes:

- Rename or deprecate `register_all()` so it no longer implies schema creation.
- Rename or deprecate helpers such as `create_valmer_curve_pricing_schemas(...)`
  if they only attach pricing runtime objects or seed pricing configuration.
- Keep any compatibility wrapper temporary and make the deprecation explicit.
- Remove documentation language that says runtime code "registers schemas".
- Ensure DataNode update paths do not depend on implicit storage registration.

Runtime attachment should include the project extension tables when needed:

```python
msm.start_engine(
    models=[
        ...,
        ValmerAssetDetailsTable,
        ValmerVectorPricesStorage,
    ],
)
```

That call is valid only after both migration providers have been applied.

## DataNode Storage Implications

`ValmerVectorPricesStorage` is a time-indexed storage table. Under the new
Main Sequence architecture, it must be migrated and registered before any
DataNode writes occur.

`ValmerVectorPricesStorage.asset_identifier` must remain a foreign key to
`AssetTable.unique_identifier`. The value is the Asset unique identifier, but
the DataNode dimension name must be the `ms-markets` canonical
`asset_identifier`.

DataNode validation must verify:

- rows use `time_index` with dtype `datetime64[ns, UTC]`;
- rows include the correct `asset_identifier`;
- static asset details do not remain duplicated in the DataNode storage;
- storage identity and hash namespace are explicit enough for shared backend
  validation.

DataNode construction or first update must not be treated as a registration
step.

## Asset Details Implications

`ValmerAssetDetailsTable.asset_uid` must remain a foreign key to
`AssetTable.uid`.

The project migration must preserve the one-to-one asset detail contract:

- `AssetTable` stores canonical asset identity.
- `ValmerAssetDetailsTable` stores Valmer-specific static details.
- Valmer vector price storage stores time-varying market data only.

This avoids reintroducing static issuer, series, maturity, issue, currency, or
instrument metadata into DataNode storage.

## Migration Workflow

The correct migration and runtime workflow is:

1. Refresh SDK and scaffold files when required by the project instructions.
2. Run the core ms-markets migration provider:

   ```bash
   mainsequence migrations current --provider msm.migrations:migration
   mainsequence migrations upgrade --provider msm.migrations:migration head
   ```

3. Run the Valmer project migration provider:

   ```bash
   mainsequence migrations current --provider migrations:migration
   mainsequence migrations upgrade --provider migrations:migration head
   ```

   Do not run `mainsequence migrations revision` as part of normal setup. Use
   it only after changing the Valmer SQLAlchemy table contract and expecting an
   in-place Alembic DDL delta. Initial table registration and physical table
   creation are driven by `metatable_models` during `upgrade`; the baseline
   `0001` revision intentionally has no `op.create_table(...)` statements.

4. Start or run project code that attaches runtime tables.
5. Seed static rows such as reference indexes and Valmer asset details.
6. Execute DataNode updates.
7. Verify rows and catalog state through platform CLI or governed table APIs.

## Implementation Tasks

- [x] Add `src/migrations/__init__.py` exposing the Valmer project migration
  provider as `migrations:migration`.
- [x] Add an Alembic migration package for the Valmer project migration stream.
- [x] Define a Valmer Alembic version MetaTable for the project migration stream.
- [x] Configure namespace-aware Alembic version locations and version table names.
- [x] Scope autogenerate so it emits only Valmer project tables and never emits
  built-in ms-markets tables.
- [x] Include `ValmerAssetDetailsTable` in the project provider
  `metatable_models`.
- [x] Include `ValmerVectorPricesStorage` in the project provider
  `metatable_models`.
- [x] Remove the obsolete project-specific `after_register_metatables` hook;
  project extension tables are resolved through migrated MetaTables and
  `msm.start_engine(models=...)`.
- [x] Update bootstrap names and docs so runtime functions say "attach" or
  "seed", not "register schemas".
- [x] Update scripts to run or document the required migration commands before
  runtime bootstrap.
- [x] Update tests to validate the provider model list and runtime attachment
  boundaries.
- [x] Add a regression test proving direct project MetaTable registration is not
  used in runtime paths.
- [ ] Validate both migration providers with `current`, `render`, `dry-run`, and
  `upgrade` in a real platform context.
- [ ] Validate a Valmer vector DataNode update after migrations and confirm rows
  land in migrated storage keyed by `time_index` and `asset_identifier`.

## Validation Evidence Required

This ADR is implemented only when the following evidence exists:

- `msm.migrations:migration` has been applied successfully in the target
  platform context.
- `migrations:migration` has been applied successfully in the same
  platform context.
- The Valmer provider registers `ValmerAssetDetailsTable`.
- The Valmer provider registers `ValmerVectorPricesStorage`.
- ms-markets runtime startup attaches to the migrated Valmer tables without
  attempting registration.
- Valmer static asset details are present in `ValmerAssetDetailsTable` and linked
  to `AssetTable.uid`.
- Valmer vector market data rows land in `ValmerVectorPricesStorage` with the
  correct `time_index` and `asset_identifier`.
- No runtime path calls direct `Model.register()` for platform-managed project
  tables.

## Consequences

Runtime startup becomes stricter. Code that previously succeeded by lazily
registering or repairing tables will now fail until migrations have been applied.

This is intentional. It makes schema state explicit, versioned, reviewable, and
repeatable across local, staging, and production platform contexts.

Existing platform environments that were initialized through old direct
registration paths may need a baseline/adoption migration rather than a naive
create-table migration. That must be assessed during live platform validation.

## Superseded Guidance

This ADR supersedes any earlier project documentation that says:

- `register_all()` creates or registers schemas;
- pricing bootstrap creates schemas at runtime;
- DataNode construction registers storage;
- project MetaTables can be registered directly outside a migration provider.

It does not change prior ADR decisions about:

- separating asset registration from pricing hydration;
- moving core pricing logic into ms-markets;
- keeping Valmer source parsing and vendor semantics in this project;
- separating static Valmer asset details from time-varying DataNode rows.
