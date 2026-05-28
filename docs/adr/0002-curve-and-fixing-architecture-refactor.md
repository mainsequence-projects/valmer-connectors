# ADR 0002: Refactor Valmer Curves And Fixings To ms-markets Pricing Architecture

## Status

Proposed

## Date

2026-05-27

## Success Criteria

This ADR is successful when it gives the implementation agent a precise plan to
move the Valmer curve path onto the current `ms-markets` / `msm_pricing`
architecture without creating another asset-indexed or constant-driven curve
system.

The implementation that follows this ADR must:

- model reference-rate identities as `msm.api.indices.Index` rows
- model pricing conventions as `IndexConventionDetails` rows keyed by
  `index_uid`
- model curve identities as `Curve` rows keyed by `curve_unique_identifier`
- publish curve observations through `msm_pricing.data_nodes.DiscountCurvesNode`
- publish real fixing observations through `msm_pricing.data_nodes.FixingRatesNode`
  only when a real fixing source exists
- keep Valmer CSV download and vendor parsing in this connector
- remove the old standalone asset-indexed curve path

This ADR only creates the refactor plan. It does not execute the refactor.

## Context

The repository currently has two curve concepts mixed together:

- a legacy standalone Valmer curve DataNode in `src/data_nodes/nodes.py`
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

- `msm.api.indices.Index` / `msm.models.IndexTable`
  define canonical reference index identities such as TIIE 28D, TIIE 91D,
  TIIE 182D, TIIE overnight, and CETE tenors.
- `msm_pricing.api.index_convention_details.IndexConventionDetails` /
  `IndexConventionDetailsTable`
  store pricing conventions keyed 1:1 by `index_uid`.
- `msm_pricing.api.curves.Curve` / `CurveTable`
  store curve identities keyed by `unique_identifier` and linked by foreign key
  to `IndexConventionDetailsTable.index_uid`.
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

1. Use `Index` for reference-rate identity.
2. Use `IndexConventionDetails` for pricing convention payloads.
3. Use `Curve` for static curve identity and interpolation policy.
4. Use `DiscountCurvesNode` for Valmer TIIE zero-curve observations.
5. Use `FixingRatesNode` only for real fixing observations.

The Valmer connector should not register TIIE, CETE, or curves as `Asset`
rows. Curves and fixings are not asset-indexed data in this architecture.

The Valmer connector should also stop using Main Sequence constants as the
canonical identity layer for curves and reference indexes. Constants may remain
temporarily as backwards-compatible aliases during migration, but the durable
identities must be the `Index.unique_identifier` and `Curve.unique_identifier`
stored in MetaTables.

## Target Data Model

### Reference Indexes

Create or upsert `Index` rows for the reference rates this connector needs:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`

Each row should include:

- `unique_identifier`
- `display_name`
- `provider`, when useful
- `metadata_json`, for Valmer/Mexico labels that should not become model
  columns

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
- `curve_type`: `zero`
- `index_uid`: the `IndexConventionDetails.index_uid` for the relevant TIIE
  index
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

`src/data_nodes/nodes.py` currently contains `MexDerTIIE28Zero`.

That class should be removed or left as a temporary compatibility wrapper only
after the dashboard and scripts have moved to `DiscountCurvesNode`.

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

- bootstrap `Index`, `IndexConventionDetails`, and `Curve` rows
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

- [ ] Add a curve/index pricing bootstrap module for Valmer, likely
  `src/instruments/curve_bootstrap.py`.
- [ ] Upsert required `Index` rows for TIIE and CETE reference indexes.
- [ ] Upsert `IndexConventionDetails` rows for each supported reference index.
- [ ] Upsert `Curve` row `VALMER_TIIE_28` with interpolation, compounding,
  source, and metadata.
- [ ] Refactor `build_tiie_valmer(...)` to implement the
  `DiscountCurveBuilder` contract and return uncompressed curve dictionaries.
- [ ] Replace `scripts/update_tiie_zero_curve.py` with the new
  `msm_pricing.data_nodes.DiscountCurvesNode` import path and configuration.
- [ ] Remove `register_etl_builders(...)` and old
  `mainsequence.instruments.pricing_models` index-spec registration from
  `src/instruments/bootstrap.py`.
- [ ] Remove or explicitly deprecate `MexDerTIIE28Zero`.
- [ ] Update dashboard curve health to read the canonical discount-curve
  DataNode and decode through core `curve_codec`.
- [ ] Add a real `FixingRatesNode` builder only if a source of actual fixing
  observations is introduced.
- [ ] Update documentation in `docs/data-nodes.md`, `docs/instruments.md`, and
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
- new: `Index`, `IndexConventionDetails`, and `Curve` MetaTables

## Verification Plan

Do not mark the implementation complete until these checks pass:

- pricing schemas register or attach with `IndexTable`,
  `IndexConventionDetailsTable`, and `CurveTable`
- `Index` rows exist for supported TIIE and CETE reference indexes
- `IndexConventionDetails` rows exist for supported pricing indexes
- `Curve.get_by_unique_identifier("VALMER_TIIE_28")` returns a curve row
- the Valmer TIIE builder returns a frame with `time_index`,
  `curve_unique_identifier`, and `curve`
- `DiscountCurvesNode` writes compressed curve rows through the core codec
- dashboard curve health reads the canonical discount-curve path
- no curve or fixing code calls `msc.Asset.get("TIIE_28")`
- no curve or fixing code publishes TIIE curves as asset-indexed data
- no code path claims fixing publication unless it writes real observations
