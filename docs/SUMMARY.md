# Documentation Summary

## Core Pages

- `docs/index.md`: documentation entry point and responsibility map.
- `docs/introduction.md`: project overview and current operational surface.
- `docs/source-import.md`: Valmer source selection, platform bucket import, and
  local `DEBUG_ARTIFACT_PATH` import.
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
- `docs/dashboards.md`: Streamlit dashboard pages and monitoring purpose.
- `docs/deployment.md`: deployment and verification commands.
- `docs/new-version-migration.md`: migration status against current SDK and
  `ms-markets` architecture.

## ADRs

- `docs/adr/0000-core-pricing-library-boundary.md`
- `docs/adr/0001-asset-registration-identity-migration.md`
- `docs/adr/0002-curve-and-fixing-architecture-refactor.md`
- `docs/adr/0003-migration-first-metatable-lifecycle.md`
- `docs/adr/0004-mexican-government-bond-curve-bootstrap.md`
- `docs/adr/cli/0004-valmer-connectors-cli-design.md`
