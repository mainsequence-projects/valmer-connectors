# Documentation Summary

## Core Pages

- `docs/index.md`: documentation entry point and responsibility map.
- `docs/introduction.md`: project overview and current operational surface.
- `docs/source-import.md`: Valmer source hydration paths for Artifact buckets,
  local folders, OneDrive Graph, MetaTable sources, and debug files.
- `docs/data-nodes.md`: DataNode output boundary and
  `ValmerVectorPricesStorage` contract.
- `docs/markets.md`: AssetTable registration, Valmer asset identity,
  `ValmerAssetDetailsTable`, and extension-library asset registration
  boundaries.
- `docs/pricing.md`: pricing runtime bootstrap, target-bond hydration, and curve
  publication.
- `docs/instruments.md`: Valmer row-to-`msm_pricing` instrument mapping.
- `docs/metatable-query-optimization.md`: current thin-projection MetaTable
  read contract for hot Valmer validation paths.
- `docs/agent-skills.md`: copying bundled Valmer Codex skills into downstream
  host projects.
- `docs/dashboards.md`: Streamlit dashboard pages and monitoring purpose.
- `docs/deployment.md`: deployment and verification commands.
- `docs/new-version-migration.md`: migration status against current SDK and
  `ms-markets` architecture.

## Implementation Plans

- `docs/implementation/metatable-source-import-plan.md`: plan for adding a
  one-or-many MetaTable-backed Valmer vector source alongside local files and
  Artifact buckets.
- `docs/implementation/curve-resolution-and-asset-patching-plan.md`: plan for
  migrating Valmer curve/index resolution to market-data-set curve bindings and
  patching existing asset pricing details through the normal vector DataNode.

## ADRs

- `docs/adr/0000-core-pricing-library-boundary.md`
- `docs/adr/0001-asset-registration-identity-migration.md`
- `docs/adr/0002-curve-and-fixing-architecture-refactor.md`
- `docs/adr/0003-migration-first-metatable-lifecycle.md`
- `docs/adr/0004-mexican-government-bond-curve-bootstrap.md`
- `docs/adr/0005-valmer-tiie-irs-mxn-curve-source.md`
- `docs/adr/0006-valmer-usd-sofr-curve-source.md`
- `docs/adr/0007-banxico-tiie-cete-fixings.md`
- `docs/adr/cli/0004-valmer-connectors-cli-design.md`
