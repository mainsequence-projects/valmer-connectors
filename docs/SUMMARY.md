# Documentation Summary

## Core Pages

- `docs/index.md`: documentation entry point and responsibility map.
- `docs/introduction.md`: project overview and current operational surface.
- `docs/source-import.md`: Valmer source hydration paths for Artifact buckets,
  local folders, OneDrive Graph, MetaTable sources, and debug files.
- `docs/time-index-table-updates.md`: TimeIndexTableUpdater output boundary,
  `ValmerVectorPricesStorage` contract, public vector query helpers, and spread
  analytics helpers.
- `docs/markets.md`: AssetTable registration, Valmer asset identity,
  `ValmerAssetDetailsTable`, public detail lookup helpers, and
  extension-library asset registration boundaries.
- `docs/pricing.md`: pricing runtime bootstrap, target-bond hydration, and curve
  publication.
- `docs/reference-rate-observations.md`: canonical FRED, Banxico, and Valmer
  daily Index observations, repair, scheduling, and verification workflow.
- `docs/instruments.md`: Valmer row-to-`msm_pricing` instrument mapping.
- `docs/metatable-query-optimization.md`: current thin-projection MetaTable
  read contract for hot Valmer validation paths.
- `docs/agent-skills.md`: copying bundled Valmer Codex skills into downstream
  host projects.
- `docs/dashboards.md`: Streamlit dashboard pages and monitoring purpose.
- `docs/control-plane.md`: two-repository Command Center control-plane architecture, FastAPI
  contracts, Job authorization, static-site integration, release order, and verification.
- `docs/deployment.md`: deployment and verification commands.
- `docs/new-version-migration.md`: migration status against current SDK and
  `ms-markets` architecture.

## Implementation Plans

- `docs/implementation/metatable-source-import-plan.md`: plan for adding a
  one-or-many MetaTable-backed Valmer vector source alongside local files and
  Artifact buckets.
- `docs/implementation/curve-resolution-and-asset-patching-plan.md`: plan for
  migrating Valmer curve/index resolution to market-data-set curve bindings and
  patching existing asset pricing details through the normal vector TimeIndexTableUpdater.
- `docs/implementation/valmer-query-helper-promotion-plan.md`: plan for
  promoting generic Valmer vector read helpers, asset-detail identifier lookup,
  and spread-analysis helpers from the fund competition project into canonical
  `valmer-connectors` query and analytics surfaces.
- `docs/implementation/ms-markets-curve-reconstruction-refactor.md`: plan for
  removing generic QuantLib rate-helper reconstruction from Valmer curve
  builders and delegating it to `msm_pricing.pricing_engine.curves` and
  `msm_pricing.scenarios.curves`.
- `docs/implementation/ms-markets-cross-currency-helper-refactor.md`: plan for
  moving USD/MXN FX swap and constant-notional cross-currency basis helper
  reconstruction to ms-markets `rate_helpers@v1` imports while keeping Valmer
  row selection and provenance local.
- `docs/implementation/spread-analysis-dashboard-import-plan.md`: plan for
  importing only the spread-analysis Streamlit dashboard as a dashboard-owned
  Valmer example without adding new core library services.
- `docs/implementation/valmer-curve-quote-index-pipeline-refactor.md`: current
  implementation and verification task for canonical daily Index observations
  and dependency-backed TIIE, SOFR, and USD/MXN curves.
- `docs/implementation/python-3-13-mainsequence-6-upgrade.md`: executable
  runtime, Index-contract, migration, verification, and platform rollout plan
  for Python 3.13 and Main Sequence 6.

## ADRs

- `docs/adr/0000-core-pricing-library-boundary.md`
- `docs/adr/0001-asset-registration-identity-migration.md`
- `docs/adr/0002-curve-and-fixing-architecture-refactor.md`
- `docs/adr/0003-migration-first-metatable-lifecycle.md`
- `docs/adr/0004-mexican-government-bond-curve-bootstrap.md`
- `docs/adr/0005-valmer-tiie-irs-mxn-curve-source.md`
- `docs/adr/0006-valmer-usd-sofr-curve-source.md`
- `docs/adr/0007-banxico-tiie-cete-fixings.md`
- `docs/adr/0008-usd-mxn-cross-currency-discount-curve.md`
- `docs/adr/0009-external-reference-rate-observations.md`
- `docs/adr/cli/0004-valmer-connectors-cli-design.md`
