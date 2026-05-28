# ADR 0002: Refactor Valmer Curves And Fixings To ms-markets Pricing Architecture

## Status

Accepted / Implemented

## Date

2026-05-27

## Success Criteria

This ADR is successful when it gives the implementation agent a precise plan to
move the Valmer curve path onto the current `ms-markets` / `msm_pricing`
architecture without creating another asset-indexed or constant-driven curve
system.

The implementation that follows this ADR must:

- model reference-rate classification as `msm.api.indices.IndexType` rows
- model reference-rate identities as `msm.api.indices.Index` rows with
  `index_type=INDEX_TYPE_INTEREST_RATE`
- model pricing conventions as `IndexConventionDetails` rows keyed by
  `index_uid`
- model curve identities as `Curve` rows keyed by `curve_unique_identifier`
- register the current pricing schema graph, including
  `PricingMarketDataBindingTable`
- publish curve observations through `msm_pricing.data_nodes.DiscountCurvesNode`
- publish real fixing observations through `msm_pricing.data_nodes.FixingRatesNode`
  only when a real fixing source exists
- keep Valmer CSV download and vendor parsing in this connector
- remove the old standalone asset-indexed curve path

This ADR is the refactor plan and implementation checklist. Checked tasks
document work already completed; unchecked tasks are still required before the
curve refactor is complete.

## Context

The repository currently has two curve concepts mixed together:

- a legacy standalone Valmer curve DataNode that previously lived in
  `src/data_nodes/nodes.py`
- a standard discount-curve runner in `scripts/update_tiie_zero_curve.py`
- Valmer curve parsing in `src/instruments/rates_curves.py`
- constant and runtime registry setup in `src/instruments/bootstrap.py`
- dashboard curve reads in `dashboards/valmer_monitor/valmer_dashboard.py`

The legacy path treats the TIIE curve like an asset-indexed DataNode:

- `MexDerTIIE28Zero.get_asset_list()` calls `msc.Asset.get("TIIE_28")`
- the curve output is indexed by `time_index` and `unique_identifier`
- curve identity comes from Main Sequence constants such as
  `ZERO_CURVE__VALMER_TIIE_28`
- reference index setup uses old `mainsequence.instruments.pricing_models`
  registry APIs

That is no longer the right architecture.

The current `ms-markets` pricing architecture separates static identities from
time-varying observations:

- `msm.api.indices.IndexType` / `msm.models.IndexTypeTable`
  define the canonical index classification keys. TIIE and CETE reference
  rates must use the built-in `INDEX_TYPE_INTEREST_RATE` classification.
- `msm.api.indices.Index` / `msm.models.IndexTable`
  define canonical reference index identities such as TIIE 28D, TIIE 91D,
  TIIE 182D, TIIE overnight, and CETE tenors. The current API requires
  `unique_identifier`, `index_type`, and `display_name`.
- `msm_pricing.api.index_convention_details.IndexConventionDetails` /
  `IndexConventionDetailsTable`
  store pricing conventions keyed 1:1 by `index_uid`.
- `msm_pricing.api.curves.Curve` / `CurveTable`
  store curve identities keyed by `unique_identifier` and linked by foreign key
  to `IndexConventionDetailsTable.index_uid`.
- `msm_pricing.bootstrap.create_pricing_schemas(...)`
  registers the pricing MetaTable dependency graph in order:
  `AssetTable`, `IndexTypeTable`, `IndexTable`,
  `IndexConventionDetailsTable`, `CurveTable`,
  `AssetCurrentPricingDetailsTable`, and `PricingMarketDataBindingTable`.
- `msm.data_nodes.indices.IndexTimestampedDataNode`
  is the base for timestamped index facts keyed by
  `INDEX_UNIQUE_IDENTIFIER_DIMENSION`.
- `msm_pricing.data_nodes.FixingRatesNode`
  publishes fixing facts with columns `time_index`, `unique_identifier`, and
  `rate`.
- `msm_pricing.data_nodes.DiscountCurvesNode`
  publishes compressed curve observations with columns `time_index`,
  `curve_unique_identifier`, and `curve`.
- `msm_pricing.data_nodes.curve_codec`
  owns curve payload compression and decompression.

## Decision

Refactor the Valmer curve path to the current pricing architecture:

1. Use `IndexType` to ensure the built-in `interest_rate` type exists.
2. Use `Index` for Mexican reference-rate identity.
3. Use `IndexConventionDetails` for pricing convention payloads.
4. Use `Curve` for static curve identity and interpolation policy.
5. Use `DiscountCurvesNode` for Valmer TIIE zero-curve observations.
6. Use `FixingRatesNode` only for real fixing observations.

The Valmer connector should not register TIIE, CETE, or curves as `Asset`
rows. Curves and fixings are not asset-indexed data in this architecture.

The Valmer connector should also stop using Main Sequence constants as the
canonical identity layer for curves and reference indexes. Constants may remain
temporarily as backwards-compatible aliases during migration, but the durable
identities must be the `Index.unique_identifier` and `Curve.unique_identifier`
stored in MetaTables.

## Target Data Model

### Reference Indexes

Create or upsert the built-in interest-rate `IndexType` before creating
reference index rows:

- `IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())`

Create or upsert `Index` rows for the Mexican reference rates this connector
needs:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`

Each row should include:

- `unique_identifier`
- `index_type`: `INDEX_TYPE_INTEREST_RATE`
- `display_name`
- `description`, when useful
- `provider`, only when the row is provider-owned
- `metadata_json`, only for stable non-pricing labels that are not already
  model columns

The TIIE and CETE rows are Mexican reference-rate identities, not Valmer
identities. Do not persist Valmer source details, tenor, calendar,
business-day convention, day-count convention, settlement days, or other
pricing mechanics on the `Index` row. Those belong in
`IndexConventionDetails`.

### Index Convention Details

Create or upsert `IndexConventionDetails` rows keyed by `index_uid`.

The `convention_dump` should contain the pricing convention details needed to
build QuantLib indexes, such as:

- index family, for example `TIIE` or `CETE`
- tenor days
- settlement days
- business-day convention
- day-count convention
- calendar
- currency
- end-of-month policy

The exact serialization should use the current core
`IndexConventionDetails.serialization_format`; the Valmer connector should not
invent a second format.

### Curves

Create or upsert a `Curve` row for the Valmer TIIE curve:

- `unique_identifier`: `VALMER_TIIE_28`
- `display_name`: `Valmer TIIE 28 zero curve`
- `curve_type`: `discount`
- `index_uid`: the `Index.uid` for the relevant TIIE index, which must already
  have a matching `IndexConventionDetails.index_uid`
- `interpolation_method`: the chosen core-supported interpolation identifier
- `compounding`: the chosen core-supported compounding identifier
- `source`: `valmer`
- `metadata_json`: Valmer source filename/URL details if useful

The static curve identity and interpolation policy belong in `CurveTable`. The
daily curve points belong in `DiscountCurvesNode`.

## Target DataNodes

### Discount Curves

The Valmer TIIE curve should publish through:

- `msm_pricing.data_nodes.CurveConfig`
- `msm_pricing.data_nodes.DiscountCurvesNode`
- `msm_pricing.data_nodes.DiscountCurveBuilder`

The output frame should contain:

- `time_index`
- `curve_unique_identifier`
- `curve`

The builder may return raw `curve` dictionaries. `DiscountCurvesNode` owns
compression through `msm_pricing.data_nodes.curve_codec`.

The builder must not:

- compress curve payloads itself
- call `msc.Asset.get(...)`
- publish using asset `unique_identifier`
- depend on Main Sequence constants for durable curve identity
- use `asset_time_statistics`

### Fixings

Fixings should publish through:

- `msm_pricing.data_nodes.IndexFixingConfiguration`
- `msm_pricing.data_nodes.FixingRatesNode`
- `msm_pricing.data_nodes.IndexFixingBuilder`

The output frame should contain:

- `time_index`
- `unique_identifier`, where this is `Index.unique_identifier`
- `rate`

This connector should not synthesize fixings from the Valmer zero curve. If a
Valmer, Banxico, or other source provides actual historical fixing observations,
add a fixing builder for that source. Until then, this repository should not
claim it owns fixing publication.

## Current Code To Refactor

### Remove Or Deprecate Legacy Standalone Curve Node

`src/data_nodes/nodes.py` previously contained `MexDerTIIE28Zero`.

Handle it in this order:

1. Move real Valmer TIIE curve publication to `DiscountCurvesNode`.
2. After the runner and dashboard use `DiscountCurvesNode`, either delete
   `MexDerTIIE28Zero` or leave it only as a temporary compatibility wrapper
   with an explicit deprecation note.
3. Do not add new callers of `MexDerTIIE28Zero` after this refactor starts.

Reasons:

- it uses `msc.Asset.get(unique_identifier="TIIE_28")`
- it publishes by asset `unique_identifier`
- it duplicates Valmer CSV parsing already present in
  `src/instruments/rates_curves.py`
- it owns curve compression that belongs in `msm_pricing.data_nodes.curve_codec`

### Refactor Valmer Curve Builder

`src/instruments/rates_curves.py::build_tiie_valmer(...)` should become the
only Valmer provider parser for the MexDer TIIE curve.

It should:

- download `MEXDERSWAP_IRSTIIEPR.csv`
- parse Valmer source columns
- normalize `asof_yyMMdd` into `time_index`
- normalize rates into decimals
- produce curve point dictionaries keyed by days to maturity
- return a frame acceptable to `DiscountCurvesNode`

It should not:

- register curve or index identities
- compress the curve payload
- query platform assets
- use constants as durable identity
- decide interpolation or compounding policy

### Replace Legacy Bootstrap

`src/instruments/bootstrap.py` currently owns:

- Main Sequence constants for reference-rate and curve identity
- `DISCOUNT_CURVE_BUILDERS` registration
- old `register_index_spec(...)` calls
- local QuantLib convention construction

Replace this with a pricing-bootstrap module that:

- ensures pricing schemas through `msm_pricing.bootstrap.create_pricing_schemas`
- upserts the built-in `interest_rate` `IndexType`
- upserts `Index` rows
- upserts `IndexConventionDetails` rows
- upserts `Curve` rows
- exposes a builder map for `DiscountCurvesNode`
- exposes a builder map for `FixingRatesNode` only when real fixing builders
  exist

The bootstrap may keep a temporary constants compatibility layer, but only as
aliases to MetaTable identities during migration.

### Update Curve Runner

`scripts/update_tiie_zero_curve.py` currently imports the old
`mainsequence.instruments.interest_rates.etl.nodes` path.

It should import from `msm_pricing.data_nodes`:

- `CurveConfig`
- `DiscountCurvesNode`

It should:

- bootstrap `IndexType`, `Index`, `IndexConventionDetails`, and `Curve` rows
- instantiate `CurveConfig(curve_unique_identifier="VALMER_TIIE_28")`
- attach `build_tiie_valmer` through `DiscountCurvesNode.set_curve_builder(...)`
- execute the node through the normal DataNode update path

### Update Dashboard Reads

`dashboards/valmer_monitor/valmer_dashboard.py` should read the canonical
discount-curve DataNode and decode with
`msm_pricing.data_nodes.curve_codec.decompress_string_to_curve`.

It should not reference:

- `MexDerTIIE28Zero`
- `valmer_mexder_tiie28_zero_curve`
- old curve constants as the durable identity

## Implementation Tasks

- [x] Add a curve/index pricing bootstrap module used by Valmer, likely
  `src/instruments/curve_bootstrap.py`.
- [x] Upsert the built-in `interest_rate` `IndexType` through
  `INDEX_TYPE_INTEREST_RATE_DEFINITION`.
- [x] Upsert required `Index` rows for TIIE and CETE reference indexes with
  `index_type=INDEX_TYPE_INTEREST_RATE`.
- [x] Upsert `IndexConventionDetails` rows for each supported reference index.
- [x] Upsert `Curve` row `VALMER_TIIE_28` with interpolation, compounding,
  source, and metadata.
- [x] Refactor `build_tiie_valmer(...)` to implement the
  `DiscountCurveBuilder` contract and return uncompressed curve dictionaries.
- [x] Replace `scripts/update_tiie_zero_curve.py` with the new
  `msm_pricing.data_nodes.DiscountCurvesNode` import path and configuration.
- [x] Remove `register_etl_builders(...)` and old
  `mainsequence.instruments.pricing_models` index-spec registration from
  `src/instruments/bootstrap.py`.
- [x] After the runner/dashboard moved to `DiscountCurvesNode`, delete
  `MexDerTIIE28Zero` or leave it only as an explicitly deprecated temporary
  wrapper.
- [x] Update dashboard curve health to read the canonical discount-curve
  DataNode and decode through core `curve_codec`.
- [x] Do not add a `FixingRatesNode` builder because this repository still has
  no source of actual fixing observations.
- [x] Update documentation in `docs/data-nodes.md`, `docs/instruments.md`, and
  dashboard docs to describe Curve and Index MetaTable identities instead of
  constants and asset-indexed curve rows.

## Non-Goals

This ADR does not:

- implement the curve refactor
- create a fixing builder without a real fixing source
- derive fixings from the Valmer zero curve
- change Valmer bond asset registration
- change Valmer bond pricing-detail hydration
- migrate historical rows from `valmer_mexder_tiie28_zero_curve`
- define a Banxico OTR curve adapter
- move Valmer source parsing into `ms-markets`

## Migration Notes

The old standalone table `valmer_mexder_tiie28_zero_curve` should be treated as
legacy. New writes should go to the canonical discount-curves DataNode from
`msm_pricing`.

The identity dimension changes from:

- old: `unique_identifier`
- new: `curve_unique_identifier`

Dashboard, script, and validation code must be updated accordingly.

Reference-rate lookup changes from:

- old: Main Sequence constants and old pricing registry
- new: `IndexType`, `Index`, `IndexConventionDetails`, and `Curve`
  MetaTables

## Verification Plan

Do not mark the implementation complete until these checks pass:

- pricing schemas register or attach with `IndexTypeTable`, `IndexTable`,
  `IndexConventionDetailsTable`, `CurveTable`, and
  `PricingMarketDataBindingTable`
- `IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())`
  returns or preserves an `interest_rate` row
- `Index` rows exist for supported TIIE and CETE reference indexes
- every supported TIIE and CETE `Index` row has
  `index_type=INDEX_TYPE_INTEREST_RATE`
- `IndexConventionDetails` rows exist for supported pricing indexes
- `Curve.get_by_unique_identifier("VALMER_TIIE_28")` returns a curve row
- the Valmer TIIE builder returns a frame with `time_index`,
  `curve_unique_identifier`, and `curve`
- `DiscountCurvesNode` writes compressed curve rows through the core codec
- dashboard curve health reads the canonical discount-curve path
- no curve or fixing code calls `msc.Asset.get("TIIE_28")`
- no curve or fixing code publishes TIIE curves as asset-indexed data
- no code path claims fixing publication unless it writes real observations
