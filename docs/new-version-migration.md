# New Version Migration

## Purpose

This document tracks the refactors required to align `valmer-connectors` with
the current `mainsequence-sdk`, `ms-markets`, and `msm_pricing` architecture.

The first migration wave is code-level complete for DataNode storage, canonical
market/pricing bootstrap, Valmer asset details, dashboard import surfaces, and
basic curve publication wiring. The remaining work is live platform validation
and a few targeted pricing-behavior checks.

## Canonical Sources

Canonical SDK source for DataNode and storage migration:

- `/Users/jose/code/MainSequenceClientSide/mainsequence-sdk/docs/migrations/datanode_metatable_storage_migration.md`

Canonical `ms-markets` package guidance used for this migration:

- `.agents/skills/ms_markets/platform/bootstrap_registration/SKILL.md`
- `.agents/skills/ms_markets/assets/asset_model_extension/SKILL.md`
- `.agents/skills/ms_markets/assets/asset_indexed_data_nodes/SKILL.md`

Local package sources checked during the migration:

- `.venv/lib/python3.11/site-packages/msm/bootstrap.py`
- `.venv/lib/python3.11/site-packages/msm/models/registration.py`
- `.venv/lib/python3.11/site-packages/msm_pricing/bootstrap.py`
- `.venv/lib/python3.11/site-packages/msm_pricing/data_nodes/`
- `.venv/lib/python3.11/site-packages/msm_pricing/api/`

Current dependency baseline:

- `mainsequence>=4.2.1`
- `ms-markets>=0.0.27`
- `streamlit>=1.58.0`
- `xlrd>=2.0.2`

## Success Criteria

The migration is complete when:

- `ImportValmer` publishes through storage-first DataNode architecture.
- Valmer vector schema lives on a SQLAlchemy storage class, not inline DataNode
  metadata methods.
- `ImportValmerConfig` owns update identity only.
- Valmer static descriptors live in a 1:1 `ValmerAssetDetailsTable`.
- Valmer assets and Valmer asset details use one bootstrap path:
  `valmer_connectors.instruments.bootstrap.bootstrap_runtime()`.
- Market MetaTables are migrated through the core `msm.migrations:migration`
  provider and attached through canonical `msm.start_engine(...)`.
- Pricing MetaTables are migrated through the core `msm.migrations:migration`
  provider and attached through canonical
  `msm_pricing.bootstrap.create_pricing_schemas(...)`.
- Pricing-detail hydration uses current `msm_pricing` APIs.
- Curves publish through canonical `msm_pricing.data_nodes.DiscountCurvesNode`.
- Dashboard code no longer imports removed SDK Streamlit scaffolding.
- No code imports removed `mainsequence.instruments` or `mainsequence.tdag`
  surfaces.
- Offline tests pass.
- A namespaced live run validates backend registration and row writes.

## Current State Summary

| Area | Current state | Remaining target |
| --- | --- | --- |
| Dependencies | Constraints and lockfile target current SDK/package versions | live platform validation |
| Vector DataNode | `ImportValmer(AssetIndexedDataNode)` with `ImportValmerConfig` | run a namespaced backend write |
| Vector storage | `ValmerVectorPricesStorage` declares the table contract | validate project migration provider registration |
| Asset FK | storage `unique_identifier` uses `MetaTableForeignKey(AssetTable, column="unique_identifier")` | confirm FK resolves live |
| Static details | `ValmerAssetDetailsTable.asset_uid` is 1:1 FK to `AssetTable.uid` | confirm rows link live |
| Bootstrap | `bootstrap_runtime()` is the single project bootstrap entry point | add idempotency regression tests |
| Asset registration | `upsert_valmer_assets(...)` is the single asset upsert helper | add wrong-type/missing-asset tests |
| Pricing hydration | `persist_current_pricing_details(...)` from `msm_pricing.api.instruments` | verify row API behavior live |
| Curves | `DiscountCurvesNode` is the only active publication path | run `scripts/update_tiie_zero_curve.py` live |
| Fixings | no project fixing source | keep no-op until a real source exists |
| Dashboard | local Streamlit bootstrap plus `mainsequence.meta_tables.APIDataNode` | run dashboard with credentials |

## Phase 0: Dependency And Scaffold Baseline

Status: partially complete.

Completed:

- [x] Updated dependency constraints for the current imports used by this repo.
- [x] Ran `uv lock` and `uv sync`.
- [x] Installed current `ms-markets` skills into `.agents/skills/ms_markets/`.
- [x] Ran import and compile smoke tests for touched modules.

Remaining:

- [ ] Run `mainsequence project update-sdk --path .` in a clean migration
  checkpoint if the project scaffold needs to be refreshed again.
- [ ] Run `mainsequence project update AGENTS.md --path .` after SDK refresh.
- [ ] Run `mainsequence project update_agent_skills --path .` after SDK
  refresh.
- [ ] Keep the first backend validation run namespaced.

## Phase 1: DataNode Storage Migration

Status: code-level complete, live write pending.

Canonical rule:

- The storage class owns schema, indexes, metadata, storage identity, and
  foreign keys.
- The DataNode owns update logic.
- The config owns update identity.

Implemented files:

- `src/valmer_connectors/data_nodes/valmer_vector_storage.py`
- `src/valmer_connectors/data_nodes/nodes.py`
- `scripts/update_vector_valmer.py`
- `tests/test_valmer_vector_storage.py`

Implemented shape:

- `ValmerVectorPricesStorage(MarketsTimeIndexMetaTableMixin, MarketsBase)`
- `__markets_base_identifier__ = "vector_de_precios_valmer"`
- `__time_index_name__ = "time_index"`
- `__index_names__ = ["time_index", "unique_identifier"]`
- `unique_identifier` declares:
  `MetaTableForeignKey(AssetTable, column="unique_identifier", ondelete="RESTRICT")`
- `ImportValmerConfig(AssetIndexedDataNodeConfiguration)` owns `bucket_name`.
- `ImportValmer(AssetIndexedDataNode)` binds to `ValmerVectorPricesStorage`
  through `_required_storage_table()`.

Deleted legacy architecture:

- inline `get_table_metadata()`
- inline `get_column_metadata()`
- `_ARGS_IGNORE_IN_STORAGE_HASH`
- `mainsequence.tdag.DataNode`
- `mainsequence.client.TableMetaData`
- `mainsequence.client.ColumnMetaData`

DataNode output boundary:

- The DataNode stores time-varying price, yield, spread, risk, rating,
  liquidity, and derived OHLC observations.
- Static repeated Valmer asset descriptors stay out of the DataNode.
- Static descriptors are persisted in `ValmerAssetDetailsTable`.

Completed:

- [x] Add storage class.
- [x] Move vector schema to storage class.
- [x] Replace old DataNode base with current markets DataNode wrapper.
- [x] Replace raw storage FK with `MetaTableForeignKey`.
- [x] Move `bucket_name` into `ImportValmerConfig`.
- [x] Keep `bucket_name` in update identity.
- [x] Remove old inline metadata methods.
- [x] Keep `update()` returning a frame indexed by
  `(time_index, unique_identifier)`.
- [x] Add storage contract tests.

Remaining:

- [ ] Run a namespaced `ImportValmer` update against the backend.
- [ ] Confirm `ValmerVectorPricesStorage` is registered by
  `valmer_connectors.migrations:migration`.
- [ ] Confirm backend rows land indexed by `time_index` and
  `unique_identifier`.

## Phase 2: Canonical Runtime Bootstrap

Status: code-level complete, live bootstrap pending.

Canonical rule from `ms-markets`:

- Application startup attaches market runtime tables through `msm.start_engine(...)`.
- Pricing startup attaches pricing runtime tables through
  `msm_pricing.bootstrap.create_pricing_schemas(...)`.
- Main Sequence and ms-markets MetaTables are migration-first; row APIs do not
  own schema creation or registration.
- Do not call row-level schema shortcuts such as `Asset.create_schemas()`.

Single project entry point:

- `valmer_connectors.instruments.bootstrap.bootstrap_runtime()`

Current behavior:

1. `bootstrap_runtime()` calls `seed_static_defaults(...)`.
2. `seed_static_defaults(...)` calls `bootstrap_valmer_curve_pricing(...)`.
3. `bootstrap_valmer_curve_pricing(...)` calls
   `attach_valmer_curve_pricing_runtime(...)`.
4. `attach_valmer_curve_pricing_runtime(...)` calls:
   `msm.start_engine(models=[..., ValmerAssetDetailsTable, ValmerVectorPricesStorage])`.
5. It then calls:
   `msm_pricing.bootstrap.create_pricing_schemas(...)`.
6. Static rows are upserted after runtime attachment:
   `IndexType`, `Index`, `IndexConventionDetails`, and `Curve`.

Code-level verification:

- `resolve_markets_meta_table_models([ValmerAssetDetailsTable])` resolves:
  `["AssetTable", "ValmerAssetDetailsTable"]`.
- No active `src/`, `scripts/`, `tests/`, or dashboard code calls
  `Asset.create_schemas()`.
- No active project code calls `register_markets_meta_tables(...)` directly.
- `ensure_valmer_asset_runtime()` only verifies the active runtime.
- `ensure_valmer_asset_detail_runtime()` only verifies the active runtime.

Completed:

- [x] Keep one project bootstrap entry point: `bootstrap_runtime()`.
- [x] Use canonical `msm.start_engine(...)` for market runtime attachment.
- [x] Include `ValmerAssetDetailsTable` and `ValmerVectorPricesStorage` in the
  same market model graph.
- [x] Use canonical `msm_pricing.bootstrap.create_pricing_schemas(...)` for
  pricing runtime attachment.
- [x] Remove implicit asset schema creation from row helpers.
- [x] Update runtime scripts to call `bootstrap_runtime()`.

Remaining:

- [ ] Add idempotency tests for repeated `bootstrap_runtime()` calls.
- [ ] Run live bootstrap with credentials.
- [ ] Confirm backend MetaTables include `AssetTable`, `ValmerAssetDetailsTable`,
  pricing tables, `Index`, `IndexConventionDetails`, and `Curve`.

## Phase 3: Valmer Asset Details MetaTable

Status: code-level complete, live row validation pending.

Implemented file:

- `src/valmer_connectors/meta_tables/valmer_asset_details.py`

Contract:

- `ValmerAssetDetailsTable` stores static or slowly changing Valmer asset
  details.
- It is a one-to-one extension of `AssetTable`.
- `asset_uid` is the primary key.
- `asset_uid` declares:
  `MetaTableForeignKey(AssetTable, column="uid", ondelete="CASCADE")`.
- No separate `uid` column is added to this one-to-one table.

Fields that belong here:

- `security_type`
- `issuer`
- `series`
- `full_name`
- `sector`
- `issued_amount`
- `issue_date`
- `issue_term`
- `maturity_date`
- `face_value`
- `issue_currency`
- `underlying`
- `placement_yield`
- `placement_spread`
- `coupon_frequency`
- `coupon_rate`
- `coupon_rule`
- `coupons_at_issue`

Fields intentionally not here:

- ratings, because they are time-varying
- prices, yields, spreads, liquidity, and risk measures, because they are
  observation facts
- pricing/instrument dumps, because they belong to pricing detail APIs

Completed:

- [x] Add or verify the one-to-one table shape.
- [x] Use `MetaTableForeignKey(AssetTable, column="uid")`.
- [x] Keep ratings out of the detail table.
- [x] Add schema tests for primary key and FK shape.
- [x] Require `bootstrap_runtime()` before row operations.

Remaining:

- [ ] Run a live Valmer update and confirm `asset_uid` values reference
  `AssetTable.uid`.
- [ ] Confirm one detail row per asset after repeated updates.

## Phase 4: Asset Registration And Pricing Hydration

Status: mostly complete, targeted tests and live validation pending.

Current ownership:

- asset identity:
  `src/valmer_connectors/instruments/asset_identity.py`
- asset registration:
  `upsert_valmer_assets(...)`
- static details:
  `upsert_valmer_asset_details(...)`
- pricing detail hydration:
  `persist_current_pricing_details(...)`
- source DataNode facts:
  `ImportValmer.update()`

Asset identity:

- Valmer unique identifier is:
  `tipovalor_emisora_serie`.
- Assets are upserted as `ASSET_TYPE_BOND` from `msm.constants`.
- Existing assets with another `asset_type` are selected for bond-type update.

Pricing hydration boundary:

- `ImportValmer.update()` publishes source facts only.
- `ImportValmer.get_asset_list()` and
  `ImportValmer.update_pricing_details_from_last_vector(...)` orchestrate
  asset and pricing-detail side effects.
- `build_qll_bond_from_row(...)` converts a normalized Valmer row into an
  `msm_pricing.instruments` instrument.
- `persist_current_pricing_details(...)` persists the current instrument dump
  and related pricing detail payload for the matching `Asset`.

Completed:

- [x] Keep `upsert_valmer_assets(...)` as the single Valmer asset upsert helper.
- [x] Use `ASSET_TYPE_BOND` from `msm.constants`.
- [x] Remove asset-row-owned schema creation.
- [x] Keep pricing hydration out of the DataNode `update()` return frame.
- [x] Convert source rows into normalized pricing payloads before instrument
  construction.
- [x] Keep Mexican vendor convention parsing in this repo.

Remaining:

- [ ] Verify `AssetCurrentPricingDetails` row lookups against current
  `msm_pricing` live APIs.
- [ ] Verify `persist_current_pricing_details(...)` writes expected rows live.
- [ ] Add tests for missing assets being upserted as bonds.
- [ ] Add tests for wrong-type existing assets being corrected to bonds.
- [ ] Add tests that pricing hydration is skipped for unsupported target rows.

## Phase 5: Curves And Fixings

Status: code-level curve migration complete, live curve publication pending.

Current architecture:

- Mexican reference rates are core `Index` rows:
  `TIIE_OVERNIGHT`, `TIIE_28`, `TIIE_91`, `TIIE_182`, `CETE_28`, `CETE_91`,
  and `CETE_182`.
- Pricing conventions are `IndexConventionDetails` rows keyed by `index_uid`.
- Valmer TIIE 28 curve identity is a `Curve` row:
  `VALMER_TIIE_28`.
- Curve observations are published through
  `msm_pricing.data_nodes.DiscountCurvesNode`.
- This project does not override `PricingMarketDataBinding`.

Current execution path:

- `scripts/update_tiie_zero_curve.py`
- `valmer_connectors.instruments.bootstrap.bootstrap_runtime()`
- `valmer_connectors.instruments.rates_curves.build_tiie_valmer(...)`
- `DiscountCurvesNode(curve_config=CurveConfig(...)).set_curve_builder(...)`

Provider boundary:

- Valmer source parsing stays in this repo.
- Curve identity, convention identity, and curve DataNode machinery stay in
  `msm_pricing`.
- No fixing-rate DataNode is added until this repo has a real fixing source.

Completed:

- [x] Add Valmer curve/index bootstrap module.
- [x] Upsert Mexican TIIE/CETE `Index` rows.
- [x] Move curve identity to core `Curve` row.
- [x] Publish through `DiscountCurvesNode` instead of a custom standalone
  curve DataNode.
- [x] Remove project override of `PricingMarketDataBinding`.
- [x] Keep `build_tiie_valmer(...)` focused on Valmer source parsing.
- [x] Add curve bootstrap tests.

Remaining:

- [ ] Run `scripts/update_tiie_zero_curve.py` live.
- [ ] Confirm `discount_curves` receives rows for `VALMER_TIIE_28`.
- [ ] Verify `DiscountCurvesNode` builder result columns and update statistics
  against the current runtime.
- [ ] If fixings are added later, publish them through the core fixing DataNode
  keyed by `Index.unique_identifier`.

## Phase 6: Dashboard Migration

Status: code-level import migration complete, runtime dashboard validation
pending.

Removed imports:

- `mainsequence.dashboards.streamlit.scaffold.PageConfig`
- `mainsequence.dashboards.streamlit.scaffold.run_page`
- `mainsequence.dashboards.streamlit.components`
- `mainsequence.tdag.APIDataNode`
- `mainsequence.client.models_tdag.DataNodeStorage`

Current dashboard shape:

- local `dashboards/valmer_monitor/page_bootstrap.py`
- `mainsequence.meta_tables.APIDataNode`
- local Streamlit sidebar/select helpers
- VS Code launch config points at `dashboards/valmer_monitor/app.py`
- launch config includes `${workspaceFolder}/.env`

Completed:

- [x] Replace removed SDK Streamlit scaffold imports.
- [x] Keep dashboard helpers local to the dashboard app.
- [x] Use the current `APIDataNode` import location.
- [x] Keep dashboard reads separate from DataNode publication code.
- [x] Add `.env` file loading to the dashboard launch configuration.

Remaining:

- [ ] Run the Streamlit dashboard with valid platform credentials.
- [ ] Verify source-vector, pricing-hydration, and curve-health pages against
  real backend data.
- [ ] Replace any dashboard-only query workaround if `msm_pricing.data_interface`
  provides a cleaner current helper.

## Phase 7: Instrument And Pricing Imports

Status: code-level migration complete for moved helpers, deeper instrument
coverage still intentionally scoped.

Current imports:

- `msm_pricing.instruments as msi`
- `msm_pricing.instruments.Position`
- `msm_pricing.pricing_engine.bond_analytics.compare_bond_to_market_quote`
- `msm_pricing.pricing_engine.coupon_schedules.compute_coupon_schedule_force_match`

Local responsibilities that should stay in this repo:

- Valmer sheet parsing
- Mexican vendor convention mapping
- supported-row selection for this source
- provider-specific row normalization

Core responsibilities that should stay in `msm_pricing`:

- coupon schedule reconciliation
- provider-neutral bond price checks
- instrument serialization/deserialization
- generic bond pricing analytics

Completed:

- [x] Move provider-neutral coupon schedule reconciliation to `msm_pricing`.
- [x] Move provider-neutral bond price-check helper to `msm_pricing`.
- [x] Import those helpers from core.
- [x] Keep Mexican convention parsing local.
- [x] Remove stale `mainsequence.instruments` imports.

Remaining:

- [ ] Re-check non-`MPS` convention requirements before broadening supported
  instrument families.
- [ ] Add focused tests for any new product family before adding it to
  `_get_target_bonds(...)`.

## Phase 8: Validation Plan

Offline validation already run:

- [x] `python -m py_compile` for touched modules.
- [x] import smoke checks for DataNode, instruments, and dashboard helpers.
- [x] storage schema tests for `ValmerVectorPricesStorage`.
- [x] config and frame tests for `ImportValmer`.
- [x] asset identity tests.
- [x] curve bootstrap tests.
- [x] `git diff --check`.

Current offline regression command:

```bash
TDAG_ROOT_PATH=/private/tmp/tdag-test .venv/bin/python -m unittest \
  tests.test_valmer_vector_storage \
  tests.test_curve_bootstrap \
  tests.test_rates_curves \
  tests.test_valmer_asset_identity
```

Live validation still required:

- [ ] `mainsequence project current --debug`
- [ ] `mainsequence project refresh_token --path .`
- [ ] run `mainsequence migrations upgrade --provider msm.migrations:migration --to head`
- [ ] run `mainsequence migrations upgrade --provider valmer_connectors.migrations:migration --to head`
- [ ] run `valmer_connectors.instruments.bootstrap.bootstrap_runtime()` with credentials
- [ ] confirm market/pricing MetaTables are migrated and runtime attaches
- [ ] run a small `ImportValmer` update with a test `hash_namespace`
- [ ] confirm `vector_de_precios_valmer` is registered by the Valmer provider
- [ ] confirm vector rows are indexed by `time_index`, `unique_identifier`
- [ ] confirm `ValmerAssetDetailsTable.asset_uid` links to `AssetTable.uid`
- [ ] run `scripts/update_tiie_zero_curve.py`
- [ ] confirm `discount_curves` receives `VALMER_TIIE_28` rows
- [ ] run the Streamlit dashboard with platform credentials

## Sequencing From Here

1. Add the remaining idempotency and asset-registration tests.
2. Run live bootstrap validation with credentials.
3. Run a namespaced source-vector update.
4. Validate `ValmerAssetDetailsTable` rows against `AssetTable.uid`.
5. Validate `persist_current_pricing_details(...)` live.
6. Validate `DiscountCurvesNode` live.
7. Run the dashboard against live data.

Do not combine new instrument-family support with the DataNode/bootstrap
migration. New product coverage should be a separate pricing-domain change.

## Known Non-Goals

This migration does not:

- create portfolios
- create asset translation tables
- introduce fixing ETLs without a real source
- migrate Mexican vendor convention semantics into core
- replace source-vector DataNode observations with MetaTables
- use `PricingMarketDataBinding` overrides in this project
- add new supported instrument families beyond the current target set
