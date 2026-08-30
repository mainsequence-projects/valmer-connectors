# ADR 0001: Migrate Valmer Asset Registration Identity

## Status

Accepted / Implemented

## Date

2026-05-27

## Context

Valmer rows are published and priced by `unique_identifier`, but the project has
historically built that identifier, registered assets, and refreshed pricing
details inside `ImportValmer` in `src/valmer_connectors/data_nodes/nodes.py`.

Current Main Sequence and `ms-markets` behavior makes those boundaries explicit:

- `unique_identifier` is the stable platform identity for asset-indexed
  TimeIndexTableUpdaters, pricing details, dashboards, portfolios, and downstream analytics.
- Valmer source rows are organization-owned custom market instruments, not
  public-master assets such as FIGI-backed equities.
- Core bond pricing instruments in `msm_pricing` require the attached asset to
  expose `asset_type == msm.constants.ASSET_TYPE_BOND`.
- Current pricing details live in the core `msm_pricing` pricing-details tables
  and are written through `msm_pricing.api.add_many_pricing_details(...)`.

ADR 0000 owns the pricing-library boundary and the pricing-detail write path.
This ADR owns the Valmer asset identity and asset registration boundary.

References:

- Main Sequence assets documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/markets/assets/>
- Main Sequence assets and pricing details documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/instruments/assets_and_pricing_details/>
- Current typed asset API:
  `msm.api.assets.Asset`
- Current core pricing write API:
  `msm_pricing.api.add_many_pricing_details(...)`

## Decision

The canonical Valmer asset identity remains:

- source fields: `tipovalor`, `emisora`, `serie`
- `unique_identifier`: `tipovalor_emisora_serie`

The canonical Valmer bond asset registration target is:

- API: `msm.api.assets.Asset`
- asset type: `msm.constants.ASSET_TYPE_BOND`

Do not use `asset_type="mexican_fixed_income"`. That value is incompatible with
the current core bond pricing validation.

This migration introduced:

- `src/valmer_connectors/instruments/asset_identity.py` for Valmer identity construction, typed
  asset lookup, and idempotent typed asset upsert
- `src/valmer_connectors/meta_tables/valmer_asset_details.py` for Valmer source fields that
  describe the asset rather than a daily price observation
- batched typed lookup helpers for Valmer asset existence and UID projection
- pricing refresh planning that reads current pricing details from
  `AssetCurrentPricingDetailsTable`

`msm.api.assets.Bond.upsert(...)` is not required for this migration because it
requires issuer and currency detail rows that this Valmer adapter does not
derive as part of asset registration. Issuer and currency enrichment remains
outside the current registration boundary.

## Registration Boundary

Asset registration means:

- derive or receive a stable Valmer `unique_identifier`
- ensure a row exists in core `AssetTable`
- set `asset_type=msm.constants.ASSET_TYPE_BOND` so core bond pricing
  instruments can be attached
- return typed `msm.api.assets.Asset` rows keyed by `unique_identifier`

Asset registration does not mean:

- publishing Valmer price bars
- selecting target bonds for pricing
- building QuantLib or `msm_pricing` instrument objects
- writing `instrument_dump`
- making dashboard pricing-health decisions
- refactoring curve or reference-rate asset lookup

Pricing hydration remains a separate phase:

- choose the subset of Valmer assets that the project can currently price
- build a normalized pricing payload and concrete instrument from source terms
- decide whether existing pricing details are missing or stale
- persist timestamped pricing details and reconcile current rows through
  `msm_pricing.api.add_many_pricing_details(...)`

Asset detail persistence is also separate from price-vector publishing:

- `ValmerAssetDetailsTable.asset_uid` is a 1:1 foreign key to `AssetTable.uid`
- the table stores static Valmer descriptive/vendor fields for the asset
- the TimeIndexTableUpdater stores the daily price, analytics observation, ratings, and other
  fields that can change over time

## Implementation Tasks

- [x] Create `src/valmer_connectors/instruments/asset_identity.py`.
- [x] Move Valmer `unique_identifier` construction out of
  `ImportValmer._concatenate_artifacts_content(...)`.
- [x] Replace asset lookup in
  `ImportValmer._sync_asset_registry_and_pricing(...)` with the typed Valmer
  asset resolver.
- [x] Replace legacy custom asset registration with typed Asset upsert using
  `msm.constants.ASSET_TYPE_BOND`.
- [x] Split registration planning from pricing-refresh planning.
- [x] Use `AssetCurrentPricingDetailsTable` lookups for pricing-refresh checks.
- [x] Keep pricing-detail writes isolated and routed through
  `msm_pricing.api.add_many_pricing_details(...)`.
- [x] Replace `build_valuation_position_from_sheet(...)` asset lookup with the
  typed Valmer asset resolver.
- [x] Replace dashboard Valmer asset lookup and pricing-health inspection with
  typed asset lookup plus `AssetCurrentPricingDetailsTable` reads.
- [x] Add `src/valmer_connectors/meta_tables/valmer_asset_details.py` with
  `ValmerAssetDetailsTable`.
- [x] Persist repeated Valmer asset descriptors into
  `ValmerAssetDetailsTable` after Asset registration.
- [x] Remove static repeated asset-descriptor columns from the Valmer
  price-vector TimeIndexTableUpdater output.
- [x] Re-enrich dashboard reads from `ValmerAssetDetailsTable` when descriptor
  fields are needed for display or target-bond checks.
- [x] Move AssetTable and pricing side effects out of
  `ImportValmer.get_asset_list()` and into the explicit
  `ImportValmer.prepare_for_update()` phase.

## Non-Goals

This ADR does not decide to:

- change Valmer asset identity away from `tipovalor_emisora_serie`
- map Valmer bonds to FIGI, ISIN, or another public master identifier
- create an `AssetCategory` for the Valmer universe
- create an `AssetTranslationTable`
- migrate portfolio construction
- change which Valmer rows are target bonds
- enrich Valmer assets into `BondDetailsTable`
- refactor curve or reference-rate registration

## Verification Evidence

- run syntax checks for touched modules
- run focused asset identity tests if local test dependencies are available
- inspect that no Valmer bond asset path calls:
  - `msc.Asset.query`
  - `msc.Asset.filter`
  - `msc.Asset.batch_get_or_register_custom_assets`
- verify `ImportValmer.prepare_for_update()` can resolve or upsert Valmer
  assets as typed `msm.api.assets.Asset` rows
- verify `ImportValmer.get_asset_list()` only returns the already prepared
  asset scope
- verify sample Valmer assets have
  `asset_type == msm.constants.ASSET_TYPE_BOND`
- verify pricing-detail hydration writes through
  `msm_pricing.api.add_many_pricing_details(...)`
