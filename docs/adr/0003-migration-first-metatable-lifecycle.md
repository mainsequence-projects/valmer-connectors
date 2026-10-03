# ADR 0003: Adopt Migration-First MetaTable Lifecycle

## Status

Accepted / Implemented Locally; Live Validation Pending

Amended on 2026-10-01 for Main Sequence SDK 9 and ms-markets 2: SDK 9 no
longer ships MetaTables. The provider helpers now come from the `metatables`
client (`mainsequence-metatable`): `metatables.migrations` replaces
`mainsequence.meta_tables.migrations`, and `metatables.migrations.env` replaces
`mainsequence.meta_tables.migrations.env`. The admin commands are
`metatables migrations ... --provider <provider>`. At the time the Valmer
provider still lived in a top-level `migrations` package and needed
`PYTHONPATH=src`, because ms-markets also installed a top-level `migrations`
package. The provider package, namespace, model registry, version table, and
revision history are unchanged. The `mainsequence.meta_tables` imports quoted
below are historical; see `README.md` and `docs/markets.md` for the current
commands.

Amended on 2026-10-02: the Valmer provider moved from the top-level
`migrations` package (`src/migrations`) to `valmer_connectors.migrations`
(`src/valmer_connectors/migrations`). The wheel now installs only the
`valmer_connectors` import package and no longer overwrites the top-level
`migrations` package that backs `msm.migrations`. The provider reference is
`valmer_connectors.migrations:migration` and needs no `PYTHONPATH`. The
provider sets `script_location` and `version_location_prefix` to the new
package explicitly. The `package`, migration namespace, version table, and
revision `0001` are unchanged, so no database migration is required. The
provider references and paths below were updated to the new package on
2026-10-03.

Amended on 2026-10-03 for MetaTables 0.1.14 and ms-markets 2.1.1: hosted
environments are migrated only by the `migrate-valmer` Job in
`.mainsequence/workflows/valmer-control-plane-api.yaml`, which runs
`scripts/migrate_valmer.py` from the candidate image before the control-plane
API deploys. The Job applies only `valmer_connectors.migrations:migration`.
The ms-markets deployment owns and migrates the ms-markets schema; this project
no longer applies or documents the ms-markets provider. Local development runs
`metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`.
The "Core ms-markets Provider", "Migration Workflow", "Implementation Tasks"
and "Validation Evidence Required" sections below state this procedure; the
operator commands they listed before are historical.

Amended again on 2026-10-03 for MetaTables 0.1.18 and ms-markets 2.2.2:
MetaTables registers cascading foreign keys since 0.1.17, and 0.1.18 makes
write on a table depend only on that table's own grants. The asset-details key
to `AssetTable.uid` keeps the `ON DELETE CASCADE` that revision `0001`
declares, so deleting an Asset deletes its Valmer details row. ms-markets ships
its provider as `msm_migrations:migration`; `msm.migrations` is only a
compatibility alias. The ownership statements below use the new name; the
reviewed-contracts list is historical.

## Date

2026-06-02

## Context

Main Sequence SDK and ms-markets now treat platform-managed MetaTables and
PlatformTimeIndexMetaTable tables as migration-first resources.

The old pattern in this project was effectively:

- import project and ms-markets models,
- call bootstrap helpers such as `register_all()` or pricing schema helpers,
- let runtime startup resolve, register, or repair tables as needed,
- then seed assets, indexes, curves, pricing bindings, and publish TimeIndexTableUpdaters.

That is no longer the correct lifecycle. The current architecture separates:

1. Alembic migration and platform MetaTable catalog registration.
2. Runtime attachment to already migrated and registered tables.
3. Static domain row seeding and updater update execution.

The relevant Main Sequence SDK contract is now:

- `PlatformManagedMetaTable` and `PlatformTimeIndexMetaTable` models are registered
  through `AlembicMetaTableMigration`.
- Direct `Model.register()` calls are internal migration plumbing and should fail
  in normal runtime code.
- updater output table is not registered by constructing or updating a TimeIndexTableUpdater.
- Runtime code must attach to already registered MetaTables and fail if the
  platform catalog is missing or stale.

The relevant ms-markets contract is now:

- built-in ms-markets, msm_pricing, and portfolio models are owned by
  `msm_migrations:migration`;
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
`msm_migrations:migration` provider. They need a project migration provider.

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
- refreshed project skills for Main Sequence TimeIndexTableUpdaters, MetaTables,
  ms-markets bootstrap registration, and ms-markets MetaTable migrations

The important behavior confirmed from those sources is that the migration
provider is now the lifecycle boundary for SQL schema evolution, MetaTable
catalog registration, and Alembic version tracking. Runtime startup attaches to
that finalized state.

## Decision

Adopt a migration-first lifecycle for Valmer project MetaTables and TimeIndexTableUpdater
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

The ms-markets deployment migrates the built-in ms-markets tables with its own
provider. This project neither applies nor documents that provider; its tables
only reference the ms-markets schema that the ms-markets deployment has applied.

That provider owns the built-in ms-markets tables, including:

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
valmer_connectors.migrations:migration
```

This project uses a normal src-layout package at `src/valmer_connectors`. The
migration provider is addressed explicitly as
`valmer_connectors.migrations:migration`, inside the package this project owns,
so it cannot collide with a `migrations` module from another distribution.

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

from msm.models.assets import AssetTable
from valmer_connectors.migrations.registry import metatable_provider_models
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


def _include_valmer_name(name, type_, parent_names):
    ...


def _include_valmer_object(object_, name, type_, reflected, compare_to):
    ...


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
    script_location="valmer_connectors.migrations:",
    version_location_prefix="valmer_connectors.migrations:versions",
    target_metadata=VALMER_TARGET_METADATA,
    alembic_registry=ValmerAlembicVersion,
    metatable_models=VALMER_MIGRATION_MODELS,
    include_name_hook=_include_valmer_name,
    include_object_hook=_include_valmer_object,
)
```

If the Python package is renamed later, the provider path, `package`,
`script_location`, and `version_location_prefix` values must be updated
together.

The provider must keep revision files namespace-aware through
`build_metatable_migration_provider(...)`. The Valmer project extension uses
the explicit namespace `valmer_connectors`, so revisions live under
`src/valmer_connectors/migrations/versions/valmer_connectors/`. The built-in ms-markets provider
keeps the library namespace `mainsequence.markets`; project-owned Valmer
MetaTables must not use that library namespace.

The provider must register those two models with the Main Sequence platform and
must not register or mutate built-in ms-markets models.

Because the current Valmer project tables subclass `MarketsBase`, the provider
must prevent Alembic autogenerate from emitting built-in ms-markets tables. The
implementation includes `AssetTable` in target metadata only so Valmer foreign
keys can resolve during Alembic comparison. DDL emission is scoped back to
Valmer-owned tables through `include_name_hook` and `include_object_hook`.

The current baseline revision under
`src/valmer_connectors/migrations/versions/valmer_connectors/0001_current_schema_baseline.py` contains
Alembic-generated DDL for the two Valmer-owned project tables. It must not be
expanded to built-in ms-markets tables, and future revisions should contain
explicit DDL only when a later in-place Valmer schema evolution requires it.

The project provider must not include:

- built-in ms-markets asset, index, curve, pricing, or portfolio tables;
- built-in ms-markets updater output table;
- any table already owned by `msm_migrations:migration`.

## Project Catalog Refresh

The upgraded ms-markets runtime resolves already-registered project extension
tables through the migration-registered physical table names and
`msm.start_engine(models=...)`. This project should not define an
`after_register_metatables` hook unless a future ms-markets API reintroduces a
project table-spec refresh requirement.

Valmer must not call built-in ms-markets migration catalog refresh logic. That
logic is reserved for the full core ms-markets migration model registry, not for
project-local extension models.

## Runtime Bootstrap Contract

`bootstrap_runtime()` is the project runtime entry point. It attaches runtime
tables and seeds static rows; it does not create schemas, run migrations, or
register MetaTables.

Compatibility helpers such as `register_all()` delegate to
`bootstrap_runtime()` and must not be used to imply schema creation.

Runtime attachment includes the project extension tables:

```python
msm.start_engine(
    models=[
        ...,
        ValmerAssetDetailsTable,
        ValmerVectorPricesStorage,
        *extra_markets_models,
    ],
)
```

`bootstrap_runtime(extra_markets_models=[...])` is the supported extension hook
for downstream libraries that need to compose portfolio, account, signal, or
project-local market tables into the same process runtime. Those extra models
must be included in the first shared markets runtime startup. A later
`msm.start_engine(...)` or `msm_portfolios.start_engine(...)` call with a
different model list conflicts with the existing runtime configuration.

Pricing string selectors belong to `msm_pricing` resolution. Do not pass pricing
strings such as `"Curve"` or `"AssetCurrentPricingDetails"` through a portfolio
startup wrapper. Concrete pricing SQLAlchemy classes are valid `MarketsBase`
models if they are intentionally included in shared markets startup, but pricing
behavior still needs `msm_pricing.bootstrap.attach_pricing_schemas(...)`.

That call is valid only after both migration providers have been applied.

## TimeIndexTableUpdater Storage Implications

`ValmerVectorPricesStorage` is a time-indexed storage table. Under the new
Main Sequence architecture, it must be migrated and registered before any
TimeIndexTableUpdater writes occur.

`ValmerVectorPricesStorage.asset_identifier` must remain a foreign key to
`AssetTable.unique_identifier`. The value is the Asset unique identifier, but
the TimeIndexTableUpdater dimension name must be the `ms-markets` canonical
`asset_identifier`.

TimeIndexTableUpdater validation must verify:

- rows use `time_index` with dtype `datetime64[ns, UTC]`;
- rows include the correct `asset_identifier`;
- static asset details do not remain duplicated in the updater output table;
- storage identity and hash namespace are explicit enough for shared backend
  validation.

TimeIndexTableUpdater construction or first update must not be treated as a registration
step.

## Asset Details Implications

`ValmerAssetDetailsTable.asset_uid` must remain a foreign key to
`AssetTable.uid`, with `ON DELETE CASCADE`: a details row belongs to its Asset.

The project migration must preserve the one-to-one asset detail contract:

- `AssetTable` stores canonical asset identity.
- `ValmerAssetDetailsTable` stores Valmer-specific static details.
- Valmer vector price storage stores time-varying market data only.

This avoids reintroducing static issuer, series, maturity, issue, currency, or
instrument metadata into updater output table.

## Migration Workflow

The correct migration and runtime workflow is:

1. Refresh SDK and scaffold files when required by the project instructions.
2. The ms-markets deployment has applied the ms-markets schema in the target
   environment.
3. Apply the Valmer project migration provider,
   `valmer_connectors.migrations:migration`:

   - hosted environments: only the `migrate-valmer` deployment Job, which runs
     before the control-plane API deploys;
   - local development:

     ```bash
     metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head
     metatables --local migrations current --provider valmer_connectors.migrations:migration
     ```

   Do not run `metatables migrations revision` as part of normal setup. Use
   it only after changing the Valmer SQLAlchemy table contract and expecting an
   in-place Alembic DDL delta. Initial table registration and physical table
   registration are driven by the project migration provider during `upgrade`;
   the current baseline `0001` revision contains Alembic-generated DDL only for
   Valmer-owned project tables.

4. Start or run project code that attaches runtime tables.
5. Seed static rows such as reference indexes and Valmer asset details.
6. Execute updater updates.
7. Verify rows and catalog state through platform CLI or governed table APIs.

## Implementation Tasks

- [x] Add `src/valmer_connectors/migrations/__init__.py` exposing the Valmer
  project migration provider as `valmer_connectors.migrations:migration`.
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
- [ ] Validate the Valmer provider in a real platform context through a
  successful `migrate-valmer` deployment Job run and a read-only
  `metatables migrations current`.
- [ ] Validate a Valmer vector updater update after migrations and confirm rows
  land in migrated storage keyed by `time_index` and `asset_identifier`.

## Validation Evidence Required

This ADR is implemented only when the following evidence exists:

- The ms-markets schema is at head in the target platform context, applied by
  the ms-markets deployment.
- The `migrate-valmer` deployment Job has applied
  `valmer_connectors.migrations:migration` successfully in the same platform
  context.
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
- TimeIndexTableUpdater construction registers storage;
- project MetaTables can be registered directly outside a migration provider.

It does not change prior ADR decisions about:

- separating asset registration from pricing hydration;
- moving core pricing logic into ms-markets;
- keeping Valmer source parsing and vendor semantics in this project;
- separating static Valmer asset details from time-varying TimeIndexTableUpdater rows.
