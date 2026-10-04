# New Version Migration

This page tracks the project migration to the current `mainsequence-sdk`,
`mainsequence-metatable` (`metatables`), `ms-markets`, and `msm_pricing`
architecture.

The architecture now has four separate boundaries:

```text
Source Import
    -> Asset And Detail Sync
    -> TimeIndexTableUpdater Publication
    -> Pricing Hydration / Curve Publication
```

Do not collapse those responsibilities back into one TimeIndexTableUpdater hook.

## Current Sources Of Truth

Project pages:

- `source-import.md`
- `time-index-table-updates.md`
- `markets.md`
- `pricing.md`
- `instruments.md`

Relevant Main Sequence documentation checked for this migration:

- Main Sequence SDK docs root: `https://mainsequence-sdk.github.io/mainsequence-sdk/`
- TimeIndexTableUpdater knowledge page: `https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/time_index_table_updates/`

Relevant repository skills:

- `.agents/skills/metatables/metatables-time-index-table-updates/SKILL.md`
- `.agents/skills/metatables/metatables-meta-tables/SKILL.md`
- `.agents/skills/metatables/metatables-migrations/SKILL.md`
- `.agents/skills/metatables/metatables-upgrade-legacy-app/SKILL.md`
- `.agents/skills/ms_markets/assets/asset_model_extension/SKILL.md`
- `.agents/skills/ms_markets/assets/asset_indexed_data_nodes/SKILL.md`
- `.agents/skills/ms_markets/platform/bootstrap_registration/SKILL.md`
- `.agents/skills/ms_markets/pricing/fixed_income_curve_building/SKILL.md`

## Dependency Baseline

Current declared package constraints:

- `mainsequence>=9.0.6,<10`
- `mainsequence-metatable>=0.1.20,<0.2`
- `ms-markets>=2.2.5,<3`
- `streamlit>=1.58.0`
- `xlrd>=2.0.2`

Main Sequence SDK 9 no longer ships MetaTables. MetaTable, time-index table,
updater, compiled SQL, and migration interfaces are imported from `metatables`
(`metatables.updaters`, `metatables.migrations`, `metatables.compiled_sql.v1`).
Identity, CodeRepository, Job, Secret, and Artifact APIs stay in `mainsequence`.
`ms-markets` 2 compiles governed SQL without the retired table scope:
`compile_markets_statement(statement, context=..., operation=...)`.
`ms-markets` 2.1 renamed its `cli` and `command_center` packages to `msm_cli`
and `msm_command_center` and moved its provider to `msm_migrations`, and 2.2.0
removed `msm_command_center`; this project imports none of them.
MetaTables registers cascading foreign keys since 0.1.17, and 0.1.18 makes write
on a table depend only on that table's own grants, so the asset-details key
keeps the `ON DELETE CASCADE` that revision `0001` declares. MetaTables 0.1.19
and later run one local API per local storage directory, which
`metatables --local` and `metatables.configure_local_client()` reach from any
checkout. SDK 9.0.6 never replays a write request.

The 0.1.20 client needs the Environment's hosted MetaTables API on 0.1.19 or
later: it rejects the `/runtime-context/` response of an older API
([MetaTables issue #33](https://github.com/mainsequence-projects/MetaTables/issues/33)),
so the `migrate-valmer` Job and the updaters fail against it. Upgrade that API
before pushing this repository; see "Publishing" in `deployment.md`.

## Migration Status

| Area | Current state | Remaining validation |
| --- | --- | --- |
| Source import | `ImportValmer.prepare_source_data()` selects platform Artifact bucket or local `DEBUG_ARTIFACT_PATH` | run both paths with representative files |
| updater output table | `ValmerVectorPricesStorage` owns the time-series table contract | live namespaced write |
| Asset registration | `valmer_connectors.assets.register_valmer_assets_from_rows(...)` is the public batch row-registration API; the minimal `AssetTable` write helper is private and requires explicit asset types | live row-registration run with representative Valmer rows |
| Static details | `ValmerAssetDetailsTable.asset_uid` is a 1:1 FK to `AssetTable.uid`, `ON DELETE CASCADE` | live row link validation |
| Pricing hydration | `prepare_for_update()` calls `_sync_asset_registry_and_pricing(...)` before `run()` and writes through `msm_pricing.api.add_many_pricing_details(...)` | live pricing-details write validation, including incomplete-result failure behavior |
| Runtime bootstrap | `bootstrap_runtime()` is the single project runtime entry point | live idempotency validation |
| Project migrations | `valmer_connectors.migrations:migration` uses `metatables.migrations` helper machinery; the `migrate-valmer` deployment Job applies it before the API rolls out | first successful hosted Job run |
| Curves | Valmer TIIE and MXN government curves publish through `DiscountCurvesNode` | live curve updates |
| Fixings | no project-owned fixing ETL exists | remains out of scope |
| Dashboard | monitors source, pricing hydration, and curve health | run with valid credentials |

## Current Vector Update Flow

```text
valmer-connectors vector update
    |
    v
bootstrap_runtime()
    |
    v
build_import_valmer()
    |
    v
prepare_for_update()
    |
    +-- source import
    +-- source rows filtered from last vector observation per asset_identifier
    +-- AssetTable sync
    +-- ValmerAssetDetailsTable sync
    +-- supported bond pricing hydration
    |
    v
run(force_update=True)
    |
    +-- get_asset_list()
    |      returns already prepared scope
    |
    +-- update()
           returns vector_de_precios_valmer rows
```

`get_asset_list()` is no longer the place where assets or pricing details are
created.

## MetaTables Created Or Owned By This Project

Project-owned Valmer models:

- `ValmerAssetDetailsTable`
- `ValmerVectorPricesStorage`
- `ValmerAlembicVersion`

Core models used by this project:

- `AssetTable`
- `IndexTypeTable`
- `IndexTable`
- `IndexConventionDetailsTable`
- `CurveTable`
- core `msm_pricing` current pricing details tables
- core `msm_pricing` discount curve storage

Relationship summary:

```text
AssetTable.uid
    -> ValmerAssetDetailsTable.asset_uid

AssetTable.unique_identifier
    -> ValmerVectorPricesStorage.asset_identifier

IndexTable.uid
    -> IndexConventionDetails.index_uid

CurveTable.uid
    -> CurveBuildingDetails.curve_uid

PricingMarketDataSet.uid
    -> PricingMarketDataSetBinding.market_data_set_uid
    -> PricingMarketDataSetCurveBinding.market_data_set_uid

PricingMarketDataSetCurveBinding
    -> selector index_uid + role_key + quote_side
    -> CurveTable.uid

AssetTable.uid
    -> current pricing details asset_uid
```

Curve rows are not owned by `IndexTable`. Valmer seeds real TIIE/CETE indexes
for instrument selectors and conventions, then binds those selectors to
`VALMER_TIIE_OVERNIGHT` or `VALMER_MXN_GOVERNMENT_BOND` through explicit
`PricingMarketDataSetCurveBinding` rows with `quote_side="mid"`.

## Migration Commands

Hosted environments are migrated only by the deployment workflow:
`.mainsequence/workflows/valmer-control-plane-api.yaml` runs the
`migrate-valmer` Job (`scripts/migrate_valmer.py`) from the candidate image and
deploys the API with `needs: [migrate]`. The Job applies only the Valmer
provider; the ms-markets deployment migrates the ms-markets schema.

During local development, apply the provider to the MetaTables local runtime:

```bash
metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head
metatables --local migrations current --provider valmer_connectors.migrations:migration
```

The Valmer provider lives in `src/valmer_connectors/migrations`, so its
reference cannot collide with the ms-markets provider and needs no
`PYTHONPATH`.

Generate a project revision only after changing Valmer SQLAlchemy table
contracts, against the local runtime at head. `revision` autogenerates by
default and compares the models with the runtime that the command selects,
which without `--local` is the Environment's hosted database:

```bash
metatables --local migrations revision --provider valmer_connectors.migrations:migration --message "<change>"
```

## Runtime Checks

After the migrations are applied and credentials are available:

```bash
valmer-connectors runtime validate
valmer-connectors vector update
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-usd-mxn-xccy
valmer-connectors curves update-mxn-government
```

The first live backend validation should run in an explicit namespace if the
environment is shared.

## Non-Goals

This migration does not:

- create portfolios
- create asset translation tables
- create fixing-rate ETL builders
- move Valmer source parsing into core `msm_pricing`
- treat every Valmer row as a supported pricing instrument
