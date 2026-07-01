# Valmer Connectors Documentation

This is the MkDocs-compatible documentation root for `valmer-connectors`.

The documentation is organized by responsibility. Do not use one page to infer
another workflow boundary.

For asset registration, use [Markets And Asset Details](markets.md). That page
defines the Valmer target-bond scope and the contract other extension libraries
must follow when registering assets from different vector sources.

## Architecture Pages

- [Introduction](introduction.md)
- [Source Import](source-import.md)
- [DataNodes](data-nodes.md)
- [Markets And Asset Details](markets.md)
- [Pricing](pricing.md)
- [Instrument Mapping](instruments.md)
- [MetaTable Query Optimization](metatable-query-optimization.md)
- [Agent Skills](agent-skills.md)
- [Dashboards](dashboards.md)
- [Deployment](deployment.md)
- [New Version Migration](new-version-migration.md)

## Implementation Plans

- [MetaTable Source Import Plan](implementation/metatable-source-import-plan.md)
- [Curve Resolution And Asset Patching Plan](implementation/curve-resolution-and-asset-patching-plan.md)

## Workflow Boundaries

```text
Valmer source files
    |
    v
Source Import
    |
    v
Asset and Detail Sync
    |
    +------------------------+
    |                        |
    v                        v
DataNode Publication      Pricing Hydration
    |                        |
    v                        v
vector_de_precios_valmer  current pricing details
                             |
                             v
                         curve/index runtime
```

## ADRs

- [ADR 0000: Core Pricing Library Boundary](adr/0000-core-pricing-library-boundary.md)
- [ADR 0001: Asset Registration Identity Migration](adr/0001-asset-registration-identity-migration.md)
- [ADR 0002: Curve and Fixing Architecture Refactor](adr/0002-curve-and-fixing-architecture-refactor.md)
- [ADR 0003: Migration-First MetaTable Lifecycle](adr/0003-migration-first-metatable-lifecycle.md)
- [ADR 0004: Mexican Government Bond Curve Bootstrap](adr/0004-mexican-government-bond-curve-bootstrap.md)
- [ADR 0005: Valmer TIIE IRS MXN Curve Source](adr/0005-valmer-tiie-irs-mxn-curve-source.md)
- [ADR 0004: Valmer Connectors CLI Design](adr/cli/0004-valmer-connectors-cli-design.md)

## Operational Files

- `.agents/tasks.md` tracks open agent work when that file exists in the
  checkout.
- `.agents/journal.md` and related `.agents/` files preserve project-state
  history when those files exist and the maintenance flow updates them.
