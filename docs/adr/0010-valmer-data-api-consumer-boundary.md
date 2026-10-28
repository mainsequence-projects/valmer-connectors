# ADR 0010: Serve Consumers Through A Valmer Data API, Not The Package

## Status

Accepted / Implemented Locally; Deployment Pending

Drafted 2026-10-28 from a review of this repository, of the two projects that
install `valmer-connectors`, and of ms-markets ADR 0047.

Implemented on 2026-10-28, on branch `adr-0010-valmer-data-api` here and on
branch `valmer-data-api` in each consumer. Nothing is pushed or deployed. While
implementing it:

- The data API is `src/valmer_connectors/data_api/` behind
  `api/valmer_data_api/main.py`, declared as `valmer-data-api` with its own
  `data_api_image` step and a `deploy_data_api` step after `migrate`;
  `docs/data-api.md` is its contract. The backend refused the first version,
  whose deploy step used the control plane's image step.
- Its release name is taken to be `valmer_data_api`, from the source path, as
  the Markets API is `api` from `api/main.py`. Consumers can override it with
  `VALMER_DATA_API_RELEASE_NAME`. Confirm the name after the first push.
- The open question on a client package is decided: each consumer carries its
  own small client module (discovery and the routes it calls). No client
  distribution exists, so no consumer installs anything from this repository.
- mexicofundcompetition keeps checking its fund share-class Assets against the
  Markets asset registry, as it already did; it needs no data API route for
  that.
- marketsanalytics reads through its own client,
  `src/marketsanalytics/platform/valmer_api.py`. Its eight fixed-income routes
  that need only Valmer data no longer wait for the core runtime. The
  Mexico-US spread reads its reference rates through the Markets Index dataset
  routes. Overnight indexes come from
  `src/marketsanalytics/local_ms_markets/overnight_indexes.py` until ms-markets
  owns them, and `local_valmer/` is gone. Its lock dropped 17 packages; Ruff,
  which only Valmer brought in, is now in its dev group.
- Each consumer's client was run against the data API application in process,
  with fake tables: every route they call answered, through the request
  identity and the request models.

## Date

2026-10-28

## Success Criteria

- No repository other than this one declares `valmer-connectors` as a
  dependency or imports `valmer_connectors`.
- Other applications read Valmer data for request/response use through the
  Valmer data API.
- A pipeline in another application that consumes a Valmer time series
  references its table by identifier, without importing a Valmer storage
  class.
- No consumer workload holds a MetaTables role on `ValmerAssetDetailsTable`,
  and no consumer process attaches a Valmer table to its ms-markets engine.
- A Valmer release that changes only this repository's internals leaves every
  consumer's `uv.lock` unchanged, and no consumer's ms-markets pin follows
  Valmer's.

## Context

### How Consumers Use The Package

Two projects install the package from a Git tag. Both were reviewed at their
`main` heads on 2026-10-28.

`marketsanalytics` (0.1.16, `9df19cd`) installs tag `v0.2.3` and pins
`ms-markets[portfolios]==3.0.2`:

- Seven services under `api/app/services/fixed_income_analytics/` back ten
  routes. All seven attach Valmer's tables with
  `ensure_valmer_asset_detail_runtime()`. Six of them compile SQL against
  Valmer's table classes (`ValmerVectorPricesStorage.__table__`,
  `ValmerAssetDetailsTable.__table__` and their columns) and run it with
  `compile_markets_statement` and `execute_markets_operation`; `bonds.py`
  also joins ms-markets' `AssetTable`. The seventh, `mexico_us_spread.py`,
  reads `ValmerDailyIndexValuesStorage` through a `TimeIndexTableRef`.
- The Mexico Fund Competition portfolio source
  (`api/app/services/portfolio_sources/mexico_fund_competition/snapshot.py`)
  reads portfolio values and weights through the Markets API, but latest
  quotes and maturity fields through `valmer_connectors.queries`.
- `src/marketsanalytics/local_valmer/` wraps
  `valmer_connectors.queries.vector_quotes` for the valuation services, and
  passes `resolve_valmer_overnight_index` to curve rebuilding for the curve
  analytics and CETE relative-value services.
- `src/marketsanalytics/platform/runtime.py` composes its one
  `msm.start_engine` configuration from the private
  `valmer_connectors.instruments.bootstrap._valmer_markets_models`.
- Its workflow already declares `reader` on the `valmer-connectors` branch.

`mexicofundcompetition` (0.1.27, `a5e37c9`) installs tag `v0.2.3` and pins
`ms-markets[portfolios]==3.0.2`:

- `src/fundcompetition/platform/runtime.py` attaches only
  `ValmerVectorPricesStorage` to its ms-markets engine.
- `src/fundcompetition/portfolios/configuration.py` builds each portfolio's
  valuation source with
  `TimeIndexTableRef.from_meta_table(ValmerVectorPricesStorage.get_meta_table())`.
- `src/fundcompetition/fund_configuration/service.py` reads fund price
  history with `read_valmer_history`, and its
  `valuation_identifier_candidates` guesses `51_<fund>_A` and `52_<fund>_A`.
- `src/fundcompetition/extraction/parsers/actinver.py` builds the
  `tipovalor_emisora_serie` identifier itself, with a comment that it matches
  `build_valmer_unique_identifier`.
- Its workflow declares no `reader` on the `valmer-connectors` branch.

Both projects define a `VECTOR_IDENTIFIER` constant that no source module
reads. The frontends (`ValmerConnectorsMonitor`, the Markets Analytics site
and the Mexico Fund Competition site) call their own applications' APIs and
import nothing from this repository. `.agents/tasks.md` also names a
`qisfactory` consumer; it is not in the workbench and was not reviewed.

### What The Coupling Costs

- **Versions move in lockstep.** This project pins the exact ms-markets
  release that the Environment's Markets deployment runs, and every consumer
  inherits that pin. marketsanalytics re-pinned twice in 14 hours only to
  follow Valmer: `40156ed` (2026-10-27) took ms-markets 3.0.1 with Valmer
  v0.2.1, and `9df19cd` (2026-10-28) took 3.0.2 with v0.2.3.
- **Consumers depend on Valmer's physical schema.** A Valmer revision that
  renames or splits a column breaks marketsanalytics at runtime, and no test
  in this repository sees it. The expand-first rule protects this project's
  own deployments, not SQL compiled in another repository.
- **Consumers depend on Valmer's process composition.** `msm.start_engine`
  accepts one configuration per process, so a consumer that attaches Valmer's
  tables must take Valmer's model list. That is why marketsanalytics imports
  a private function.
- **Access is table-shaped.** Each consumer workload needs MetaTables roles on
  Valmer's tables. Valmer data is licensed, and there is no single place to
  decide who may read it or to record who did.
- **Consumers install an application.** QuantLib, arch, scipy, streamlit,
  plotly, xlrd and ruff reach every consumer environment.
- **Boundaries mix inside one consumer.** The Mexico Fund Competition
  portfolio source reads Markets data through the Markets API and Valmer data
  in-process.

### The Platform Rule This Applies

ms-markets ADR 0047, which ms-markets 3.0 implements, keeps the in-process
path (importing the package, attaching tables, compiling SQL) for the owner of
the tables: its API, its migration Job and its updater Jobs. Every other
application reaches that data through an API. The ADR sorts tables into three
tiers:

| Tier | Written by | Read by other applications |
| --- | --- | --- |
| Shared registries | Markets, through its API | through the Markets API |
| Time-index facts | the producer, in its own namespace | by table uid, through the Markets API, or as the person |
| Extension-owned tables | the provider, in its own namespace | through the provider's own API |

This project already follows that rule as a consumer of Markets (`AGENTS.md`
and "Markets API Access" in `docs/deployment.md`). It does not yet follow it as
a provider. ADR 0000 defines this project as a connector that imports and
normalizes Valmer data and maps it into core pricing.
`docs/implementation/valmer-query-helper-promotion-plan.md` later added
`valmer_connectors.queries` for other projects to import; that is the path
this ADR replaces.

The pieces the decision needs already exist:

- The control plane reads Valmer's own tables by identifier through the
  MetaTables client, without the markets runtime
  (`src/valmer_connectors/control_plane/service.py`).
- `msm.client` finds the Markets API in the caller's Environment and obtains
  its URL and token through the platform's release lookup.
- The Markets API serves Index dataset values by table uid
  (`GET /api/v1/index/{uid}/datasets/{meta_table_uid}/values/`) and hydrates
  sidecar tables on asset lookups (`GET /api/v1/asset/by-uids/?include=`).
  Both routes exist in ms-markets 3.0.2.

References:

- ms-markets ADR 0047, `docs/ADR/0047-markets-api-application-boundary.md` in
  the ms-markets repository
- `msm.client.discovery` in ms-markets 3.0.2
- "Teams And Namespaces" and "Markets API Access" in `docs/deployment.md`
- `docs/control-plane.md`

## Decision

### 1. valmer-connectors Is An Application

Only this repository's processes import `valmer_connectors`: its Jobs, its
migration provider, its CLI and its APIs. No other repository depends on the
`valmer-connectors` distribution.

This project's public surface is its data API, the identifier rule of
ADR 0001, and the identifiers and columns of the tables that decision 4
publishes. Python modules are internal and may change in any release.

### 2. The Tier Decides How Other Applications Read Valmer Data

| Valmer data | Tier | How other applications read it |
| --- | --- | --- |
| `ValmerAssetDetailsTable` | Extension-owned (one-to-one sidecar on `AssetTable.uid`) | The Valmer data API |
| `ValmerVectorPricesStorage` | Time-index fact | The Valmer data API for request/response reads; a table reference for pipeline inputs (decision 4) |
| Discount curves and Index fixings (`valmer_discount_curves_instance()`, `valmer_index_fixings_instance()`) | Instances of Markets storage contracts | Through the default pricing market-data set, whose bindings name the tables |
| Daily Index values (`ValmerDailyIndexValuesStorage`) | Instance of the Markets daily Index values contract | Markets' Index dataset routes, which find the table through the Index dataset catalog |
| Overnight-index conventions (`resolve_valmer_overnight_index`) | Market conventions (F-TIIE, SOFR), not Valmer data | ms-markets conventions (see "Curve Conventions") |

Curves, fixings and daily Index values are already published for Markets to
find, so a consumer reads them without naming Valmer.

### 3. Request/Response Reads Go Through A Valmer Data API

A new FastAPI release serves read models over Valmer's own tables. Its route
families follow what consumers read today:

- the valuation dates available in the vector;
- filter facets (security type, issuer, series) with instrument counts;
- bond search with server-side filtering, sorting and paging;
- vector history for a list of identifiers and columns;
- the latest observation per identifier as of a date;
- a universe snapshot on one valuation date: vector columns joined with asset
  details for a filtered set, which the bond comparison, CETE and M-bond
  screens and daily movers read;
- asset details and maturity fields by Valmer identifier or Asset uid, and
  identifier alias resolution.

Rules:

- Tabular responses use the `core.tabular_frame@v1` frame, as the Markets API
  does. Inputs are lists of identifiers or uids with bounded batch sizes, and
  results are paged.
- There is no generic SQL or table-passthrough route. ms-markets ADR 0047
  excludes one for the same reason: it recreates direct table access over
  HTTP.
- Analytics stay with the consumer. The API serves Valmer data filtered,
  joined and paged; spreads, relative value and the Mexico-US spread remain
  marketsanalytics computations.
- The queries that join Valmer's tables move here, next to the schema, so a
  Valmer revision and the queries it affects change in one commit and are
  tested together. `valmer_connectors.queries` becomes the API's
  implementation. Bond search needs no ms-markets `AssetTable` join: it reads
  `asset_uid` from the details row.

### 4. Pipeline Inputs Reference Tables By Identifier

When another application's time-index pipeline consumes a Valmer series, such
as a portfolio's valuation source or a pricing run's curves, it references the
table instead of calling the API. It finds the MetaTable by identifier
(`TimeIndexMetaTable.filter(identifier=...)`) and wraps it in
`TimeIndexTableRef`, as the control plane does. Copying the series over HTTP
into another table would duplicate it and hide the dependency from the
platform.

For each table read this way, this project publishes the identifier, the
index names and the columns with their types, serves them from a dataset
discovery route on the data API, and changes them only by addition. A
consumer that reads a table this way holds `data_viewer` on that table alone,
granted by the Valmer Team, as "Teams And Namespaces" in `docs/deployment.md`
already describes for the curve, fixing and daily Index value tables.

### 5. The Contract Is The Wire Format

Routes live under `/api/v1/` and change only by addition; removing or renaming
a field needs a new version. A consumer depends on the routes and on the
published table contracts, never on a Python module of this project.

Identifiers keep the ADR 0001 rule, `tipovalor_emisora_serie`. A consumer may
build one from those three fields without importing
`build_valmer_unique_identifier`, citing ADR 0001 as its source. Whether an
identifier exists is answered by the registry that holds it: the Markets asset
registry for Assets, the data API for Valmer detail rows.

### 6. Discovery And Access Follow The Markets API

- A consumer finds the data API's release by repository (`valmer-connectors`)
  and release name in its own Environment, the way `msm.client.discovery`
  finds the Markets API: `ResourceRelease.filter_admin`, then
  `resolve_runtime_access` for the URL and token. `VALMER_DATA_API_URL`, with
  an optional `VALMER_DATA_API_TOKEN`, selects a local API;
  `VALMER_DATA_API_RELEASE_UID`, `VALMER_DATA_API_RELEASE_NAME` and
  `VALMER_DATA_API_REPOSITORY_NAME` pin or rename the release to find.
- Each consumer resource that calls the API declares `reader` on the
  `valmer-connectors` branch in its workflow.
- The API installs the SDK request identity once, as the control plane does
  (`install_request_identity` in `create_app`), and learns the caller only
  from it. It runs its queries as its own workload User, so request/response
  readers hold no roles on Valmer's tables.
- Licensing rules, meaning which Teams or people may read Valmer data, are
  enforced in the API once they are defined. The API logs the caller of each
  request.

### 7. A Separate Release In The Same Workflow File

The data API is a new `fastapi` resource in
`.mainsequence/workflows/valmer-control-plane-api.yaml`, the repository's
only workflow file, with its own key (working name `valmer-data-api`) and
source path `api/valmer_data_api/main.py`:

- The new key creates a new target, which is intended. The file path and the
  existing keys do not change.
- Its deploy step waits for the `migrate` step, as `deploy_api` does.
- It declares `reader` on the `MetaTables` branch, and on the
  `MainSequenceMarkets` branch if it reads Markets registries. It declares no
  Secret, Constant or Bucket and launches no Job.
- Its workload User joins the Valmer Team, which grants it `data_viewer` on
  the tables it reads.
- Business logic lives under `src/valmer_connectors/data_api/`, and route
  handlers stay thin, as for the control plane.

It is not part of the control-plane API because:

- it serves applications and the people behind them, not operators, and the
  operator rule (`VALMER_CONTROL_PLANE_OPERATOR_UIDS`) does not apply to it;
- it makes a compatibility promise to other repositories that the control
  plane, whose only client is `ValmerConnectorsMonitor`, does not make;
- a heavy search must not slow the operator console, and the two releases
  scale separately.

## Curve Conventions

`msm_pricing` rebuilds a curve from key nodes and needs a QuantLib overnight
index for each node's `floating_index`. Today a consumer passes
`resolve_valmer_overnight_index` as `overnight_index_resolver`. The resolver
maps F-TIIE and SOFR, which are market conventions, not Valmer data.

The target is that ms-markets resolves these overnight indexes from its own
Index conventions, so no consumer passes a provider callback. That change
belongs to ms-markets. Until it ships, a consumer defines the two overnight
indexes itself; the duplicate is a market convention, not a dependency on this
project.

## Target State Per Consumer

### marketsanalytics

- `valmer-connectors` leaves `dependencies` and `[tool.uv.sources]`, and the
  ms-markets pin follows only the Markets deployment.
- The ten fixed-income routes, the Mexico Fund Competition portfolio source
  and the valuation services call the data API.
- `mexico_us_spread.py` reads its reference rates through Markets' Index
  dataset routes.
- Curve scenarios and single-security pricing keep reading curves through the
  market-data set bindings and take overnight indexes as "Curve Conventions"
  describes.
- `src/marketsanalytics/local_valmer/` and the `_valmer_markets_models` call
  are removed, and the engine configuration attaches no Valmer table.
- Its workload drops its MetaTables roles on `ValmerAssetDetailsTable` and the
  vector, and keeps `data_viewer` on the curve and fixing tables it prices
  with.

### mexicofundcompetition

- `valmer-connectors` leaves `dependencies` and `[tool.uv.sources]`.
- `portfolios/configuration.py` builds the valuation source from the vector's
  MetaTable, found by identifier, and `platform/runtime.py` attaches no Valmer
  table.
- `fund_configuration/service.py` reads fund price history from the data API.
- The Actinver parser and `valuation_identifier_candidates` build identifiers
  by the ADR 0001 rule; the candidates are checked against the Markets asset
  registry, as before.
- The workflow adds `reader` on the `valmer-connectors` branch to each
  resource that calls the API. The resources that build portfolios keep
  `data_viewer` on the vector.

### valmer-connectors

- Adds the data API and its dataset discovery route (decisions 3, 4, 6
  and 7).
- Keeps `valmer_connectors.queries` as internal code behind the API.
- Once no other project installs the package, its dependency list affects
  only this project's image, and the package never needs publishing to a
  package index.

## Migration Path

Each step leaves every consumer working (expand first).

1. Ship the data API next to the current package. Consumers keep their
   imports.
2. Move mexicofundcompetition. Its two call sites are enough to prove
   discovery, access and the table-reference path end to end.
3. Move marketsanalytics one route family at a time: bonds, bond comparison,
   the CETE and M-bond screens, daily movers, the Mexico-US spread, then the
   Mexico Fund Competition portfolio source and the valuation services.
   Before switching a family, compare its rows from the package and from the
   API for the same valuation date.
4. When neither consumer imports `valmer_connectors`, remove the package from
   both, drop their roles on Valmer's details and vector tables, and retire
   the guidance that "Superseded Guidance" lists.

## Implementation Tasks

### valmer-connectors

- [x] Add `src/valmer_connectors/data_api/` with the route families of
  decision 3, built on `valmer_connectors.queries` and on the queries now in
  marketsanalytics' fixed-income services.
- [x] Add the dataset discovery route of decision 4.
- [x] Add `api/valmer_data_api/main.py` and install the SDK request identity
  once.
- [x] Declare the `valmer-data-api` resource and its deploy step in the
  workflow file.
- [ ] Add the data API's workload User to the Valmer Team after the first push.
- [x] Test each route against its response model, including page limits and
  batch sizes.
- [x] Document the data API in `docs/` and add it to `mkdocs.yml`.

### mexicofundcompetition

- [x] Add `reader` on the `valmer-connectors` branch where the API is called.
- [x] Build the valuation source from the vector's MetaTable, found by
  identifier.
- [x] Read fund price history from the data API.
- [x] Cite ADR 0001 for identifier construction.
- [x] Remove `VECTOR_IDENTIFIER` and the `valmer-connectors` dependency.

### marketsanalytics

- [x] Move each route family in the order of "Migration Path" step 3.
- [x] Read the Mexico-US spread reference rates through Markets' Index
  dataset routes.
- [x] Take overnight indexes as "Curve Conventions" describes.
- [x] Remove `src/marketsanalytics/local_valmer/`, the
  `_valmer_markets_models` call, `VECTOR_IDENTIFIER` and the
  `valmer-connectors` dependency.

### ms-markets

- [ ] Resolve overnight indexes for curve rebuilding from Index conventions.

## Validation Evidence Required

This ADR is implemented only when the following evidence exists:

- Neither consumer's `pyproject.toml`, `uv.lock` or `requirements.txt` names
  `valmer-connectors`, and neither imports `valmer_connectors`.
- Each moved route family returned the same rows through the API as through
  the package for one valuation date before the switch.
- A deployed workload of each consumer reached the data API through the
  release lookup, and a request without a platform credential was refused.
- No consumer workload holds a MetaTables role on `ValmerAssetDetailsTable`.
- A Valmer release that changed only internals deployed without a change to
  either consumer's `uv.lock`.

## Consequences

- This project takes on a versioned public API, batch routes and their tests.
  ms-markets ADR 0047 makes the same trade for Markets: central work instead
  of lockstep releases across consumers.
- Valmer schema changes become local. A column change and the queries it
  affects ship together, constrained only by the API contract and the table
  contracts of decision 4.
- Consumers choose their ms-markets release without regard to Valmer's.
- Request/response reads add a network hop, and bulk reads need paging.
  Pipeline inputs keep reading tables directly.
- Until the SDK documents forwarding the requester from one application to
  another, the API sees the consumer's workload as its caller, not the person
  behind it. Per-person licensing rules and audit wait for that.

## Non-Goals

This ADR does not decide to:

- add a generic SQL or table-passthrough route;
- move analytics (spreads, relative value, daily movers, the Mexico-US
  spread) into this project;
- serve pipeline inputs over HTTP;
- change Valmer's table schemas, identifiers or namespace;
- change the control-plane API, its operator rule or `ValmerConnectorsMonitor`;
- set Valmer licensing rules; it only places where they are enforced;
- publish the package to PyPI or another package index.

## Open Questions

- **Licensing policy.** Which Teams or people may read Valmer data through the
  API, and what is recorded for each read.
- **Asset-details hydration.** Whether Markets' `include` on asset lookups,
  which needs the Markets workload to hold `data_viewer` on
  `ValmerAssetDetailsTable`, should serve lookups by uid instead of a data API
  route.

## Superseded Guidance

This ADR supersedes:

- `docs/implementation/valmer-query-helper-promotion-plan.md`, which made
  `valmer_connectors.queries` a package for other projects to import;
- "Public Valmer Detail Query API" in `docs/markets.md`, which tells
  downstream projects to import `valmer_connectors.queries`;
- "Agent Skills" in `README.md`, whose `copy-valmer-skills` command assumes
  that downstream projects install this package.

It does not change:

- ADR 0000's boundary between this connector and core pricing;
- ADR 0001's identifier rule, which decision 5 publishes as a contract;
- ADR 0003's migration-first lifecycle;
- how this project reaches Markets (ms-markets ADR 0047 and `AGENTS.md`).
