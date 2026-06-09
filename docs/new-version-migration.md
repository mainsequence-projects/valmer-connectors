# New Version Migration

This page tracks the project migration to the current `mainsequence-sdk`,
`ms-markets`, and `msm_pricing` architecture.

The architecture now has four separate boundaries:

```text
Source Import
    -> Asset And Detail Sync
    -> DataNode Publication
    -> Pricing Hydration / Curve Publication
```

Do not collapse those responsibilities back into one DataNode hook.

## Current Sources Of Truth

Project pages:

- `source-import.md`
- `data-nodes.md`
- `markets.md`
- `pricing.md`
- `instruments.md`

Relevant Main Sequence documentation checked for this migration:

- Main Sequence SDK docs root: `https://mainsequence-sdk.github.io/mainsequence-sdk/`
- DataNode knowledge page: `https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/data_nodes/`

Relevant repository skills:

- `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- `.agents/skills/ms_markets/assets/asset_model_extension/SKILL.md`
- `.agents/skills/ms_markets/assets/asset_indexed_data_nodes/SKILL.md`
- `.agents/skills/ms_markets/platform/bootstrap_registration/SKILL.md`
- `.agents/skills/ms_markets/pricing/fixed_income_curve_building/SKILL.md`

## Dependency Baseline

Current declared package constraints:

- `mainsequence>=4.3.14`
- `ms-markets>=0.0.34`
- `streamlit>=1.58.0`
- `xlrd>=2.0.2`

## Migration Status

| Area | Current state | Remaining validation |
| --- | --- | --- |
| Source import | `ImportValmer.prepare_source_data()` selects platform Artifact bucket or local `DEBUG_ARTIFACT_PATH` | run both paths with representative files |
| DataNode storage | `ValmerVectorPricesStorage` owns the time-series table contract | live namespaced write |
| Asset registration | `upsert_valmer_assets(...)` is the single Valmer asset upsert helper | wrong-type and missing-asset regression tests |
| Static details | `ValmerAssetDetailsTable.asset_uid` is a 1:1 FK to `AssetTable.uid` | live row link validation |
| Pricing hydration | `prepare_for_update()` calls `_sync_asset_registry_and_pricing(...)` before `run()` | live `AssetCurrentPricingDetails` write validation |
| Runtime bootstrap | `bootstrap_runtime()` is the single project runtime entry point | live idempotency validation |
| Project migrations | `migrations:migration` uses SDK migration helper machinery | live revision/current/upgrade check |
| Curves | Valmer TIIE curve publishes through `DiscountCurvesNode` | live curve update |
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
    -> Curve.index_uid

AssetTable.uid
    -> current pricing details asset_uid
```

## Migration Commands

Run core `ms-markets` migrations before project migrations:

```bash
mainsequence migrations current --provider msm.migrations:migration
mainsequence migrations upgrade --provider msm.migrations:migration head

mainsequence migrations current --provider migrations:migration
mainsequence migrations upgrade --provider migrations:migration head
```

Generate a project revision only after changing Valmer SQLAlchemy table
contracts:

```bash
mainsequence migrations revision --provider migrations:migration
```

## Runtime Checks

After migrations and credentials are available:

```bash
valmer-connectors runtime validate
valmer-connectors vector update
valmer-connectors curves update-tiie-zero
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
