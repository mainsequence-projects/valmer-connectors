# ADR 0002: Refactor Valmer Curves And Fixings To ms-markets Pricing Architecture

## Status

Accepted / Implemented

## Date

2026-05-27

## Current Contract

This ADR records the implemented Valmer curve and fixing architecture. The
project uses the current `ms-markets` / `msm_pricing` model and does not create
another asset-indexed or constant-driven curve system.

The implemented contract is:

- model reference-rate classification as `msm.api.indices.IndexType` rows
- model reference-rate identities as `msm.api.indices.Index` rows with
  `index_type=INDEX_TYPE_INTEREST_RATE`
- model pricing conventions as `IndexConventionDetails` rows keyed by
  `index_uid`
- model curve identities as `Curve` rows keyed by `unique_identifier`
- attach the current pricing schema graph needed for index conventions, curves,
  current pricing details, discount curves, and fixings
- publish curve observations through `msm_pricing.data_nodes.DiscountCurvesNode`
- publish real fixing observations through `msm_pricing.data_nodes.FixingRatesNode`
  only when a real fixing source exists
- keep Valmer CSV download and vendor parsing in this connector
- exclude standalone asset-indexed curve publication from active writes

## Context

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
- `msm_pricing.bootstrap.attach_pricing_schemas(models=[...])`
  attaches the explicit already-migrated pricing MetaTable dependency graph:
  `AssetTable`, `IndexTypeTable`, `IndexTable`,
  `IndexConventionDetailsTable`, `CurveTable`,
  `AssetCurrentPricingDetailsTable`, and pricing updater output tables.
- `msm.data_nodes.indices.IndexTimestampedDataNode`
  is the base for timestamped index facts keyed by
  `INDEX_UNIQUE_IDENTIFIER_DIMENSION`.
- `msm_pricing.data_nodes.FixingRatesNode`
  publishes fixing facts with columns `time_index`, `unique_identifier`, and
  `rate`.
- `msm_pricing.data_nodes.DiscountCurvesNode`
  publishes compressed curve observations from builder frames with columns
  `time_index`, `curve_identifier`, `curve`, and `key_nodes`.
- `msm_pricing.data_nodes.curve_codec` and
  `msm_pricing.data_nodes.curves.key_nodes`
  own curve and key-node payload compression/decompression.

## Decision

The Valmer curve path uses the current pricing architecture:

1. Use `IndexType` to ensure the built-in `interest_rate` type exists.
2. Use `Index` for Mexican reference-rate identity.
3. Use `IndexConventionDetails` for pricing convention payloads.
4. Use `Curve` for static curve identity and interpolation policy.
5. Use `DiscountCurvesNode` for Valmer TIIE OIS curve observations.
6. Use `FixingRatesNode` only for real fixing observations.

The Valmer connector does not register TIIE, CETE, or curves as `Asset`
rows. Curves and fixings are not asset-indexed data in this architecture.

The durable identity layer for curves and reference indexes is the MetaTable
identity: `Index.unique_identifier` and `Curve.unique_identifier`.

## Target Data Model

### Reference Indexes

The bootstrap upserts the built-in interest-rate `IndexType` before creating
reference index rows:

- `IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())`

The bootstrap upserts `Index` rows for the Mexican reference rates this
connector needs:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`

Each row includes:

- `unique_identifier`
- `index_type`: `INDEX_TYPE_INTEREST_RATE`
- `display_name`
- `description`, when useful
- `provider`, only when the row is provider-owned
- `metadata_json`, only for stable non-pricing labels that are not already
  model columns

The TIIE and CETE rows are Mexican reference-rate identities, not Valmer
identities. Their `provider` is `Banco de Mexico`; Valmer is only a curve or
vector source for observations that reference those indexes. Do not persist
Valmer source details, tenor, calendar, business-day convention, day-count
convention, settlement days, or other pricing mechanics on the `Index` row.
Those belong in `IndexConventionDetails`.

### Index Convention Details

The bootstrap upserts `IndexConventionDetails` rows keyed by `index_uid`.

The `convention_dump` contains the pricing convention details needed to build
QuantLib indexes, such as:

- index family, for example `TIIE` or `CETE`
- tenor days
- settlement days
- business-day convention
- day-count convention
- calendar
- currency
- end-of-month policy

The serialization uses the current core
`IndexConventionDetails.serialization_format`; the Valmer connector does not
invent a second format.

### Curves

The bootstrap upserts a `Curve` row for the Valmer TIIE curve:

- `unique_identifier`: `VALMER_TIIE_OVERNIGHT`
- `display_name`: `Valmer TIIE overnight OIS curve`
- `curve_type`: `projection`
- `interpolation_method`: the chosen core-supported interpolation identifier
- `compounding`: the chosen core-supported compounding identifier
- `source`: `valmer`
- `metadata_json`: Valmer source filename/URL details if useful

Do not put `index_uid` on the curve row. TIIE selector indexes resolve to
`VALMER_TIIE_OVERNIGHT` through `PricingMarketDataSetCurveBinding`.

The static curve identity and interpolation policy belong in `CurveTable`. The
daily curve points belong in `DiscountCurvesNode`.

## Target TimeIndexTableUpdaters

### Discount Curves

The Valmer TIIE curve publishes through:

- `msm_pricing.data_nodes.CurveConfig`
- `msm_pricing.data_nodes.DiscountCurvesNode`
- `msm_pricing.data_nodes.DiscountCurveBuilder`

The output frame should contain:

- `time_index`
- `curve_identifier`
- `curve`
- `key_nodes`

The builder returns raw `curve` dictionaries and uncompressed `key_nodes`
source-owned JSON. `DiscountCurvesNode` owns compression through the
`msm_pricing` curve and key-node codecs. Valmer attaches source-specific
key-node validators before compression.

Because Valmer publishes this curve as a daily dataset, the Valmer runtime must
set the imported `DiscountCurvesNode` storage cadence to `1d` before pricing
runtime attachment or curve updates.

The builder must not:

- compress curve payloads itself
- call `msc.Asset.get(...)`
- publish using asset `unique_identifier`
- depend on Main Sequence constants for durable curve identity
- use `asset_time_statistics`

### Fixings

Real fixings publish through:

- `msm_pricing.data_nodes.IndexFixingConfiguration`
- `msm_pricing.data_nodes.FixingRatesNode`
- `msm_pricing.data_nodes.IndexFixingBuilder`

The output frame should contain:

- `time_index`
- `unique_identifier`, where this is `Index.unique_identifier`
- `rate`

This connector does not synthesize fixings from the Valmer OIS curve. Until a
real Valmer, Banxico, or other fixing source exists, this repository does not
claim fixing publication.

## Valmer TIIE Builder

`src/valmer_connectors/instruments/rates_curves.py::build_tiie_irs_mxn_valmer(...)`
is the Valmer provider parser and OIS bootstrapper for the TIIE curve.

It:

- downloads `IRS_MXN_CURVE.csv`
- resolves the Valmer benchmark date through the AJAX flow used by
  `https://www.valmer.com.mx/en/`, selecting the `Indices_Benchmarks` date
  record
- includes only `Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO` rows
- builds QuantLib OIS helpers for observed domestic FTIIE swap quotes
- produces curve point dictionaries keyed by days to maturity
- produces `CurveKeyNode`-compatible source quote provenance in `key_nodes`
- returns a frame acceptable to `DiscountCurvesNode`

It does not:

- register curve or index identities
- compress the curve payload
- compress the key-node payload
- query platform assets
- use constants as durable identity
- decide interpolation or compounding policy

## Pricing Bootstrap

`src/valmer_connectors/instruments/curve_bootstrap.py`:

- attaches pricing runtime through
  `msm_pricing.bootstrap.attach_pricing_schemas(models=[...])`
  with explicit pricing MetaTable models
- upserts the built-in `interest_rate` `IndexType`
- upserts `Index` rows
- upserts `IndexConventionDetails` rows
- upserts `Curve` rows
- relies on `DiscountCurvesNode` runtime configuration in
  `src/valmer_connectors/services/curve_update.py`
- does not register curve or index identity through Main Sequence constants

## Dashboard Reads

`dashboards/valmer_monitor/valmer_dashboard.py` reads the canonical
discount-curve TimeIndexTableUpdater and decodes through the core curve codec. It does not
read a standalone Valmer TIIE curve table.

## Implementation Tasks

- [x] Add a curve/index pricing bootstrap module used by Valmer:
  `src/valmer_connectors/instruments/curve_bootstrap.py`.
- [x] Upsert the built-in `interest_rate` `IndexType` through
  `INDEX_TYPE_INTEREST_RATE_DEFINITION`.
- [x] Upsert required `Index` rows for TIIE and CETE reference indexes with
  `index_type=INDEX_TYPE_INTEREST_RATE`.
- [x] Upsert `IndexConventionDetails` rows for each supported reference index.
- [x] Upsert `Curve` row `VALMER_TIIE_OVERNIGHT` with interpolation, compounding,
  source, and metadata.
- [x] Refactor `build_tiie_irs_mxn_valmer(...)` to implement the
  `DiscountCurveBuilder` contract and return uncompressed curve dictionaries
  plus uncompressed `key_nodes` provenance.
- [x] Run Valmer TIIE publication through
  `src/valmer_connectors/services/curve_update.py` and
  `msm_pricing.data_nodes.DiscountCurvesNode`.
- [x] Use `src/valmer_connectors/instruments/bootstrap.py` only as the
  runtime-bootstrap entry point delegating pricing setup to `curve_bootstrap.py`.
- [x] Remove the standalone asset-indexed TIIE curve path from active writes.
- [x] Update dashboard curve health to read the canonical discount-curve
  TimeIndexTableUpdater and decode through core `curve_codec`.
- [x] Do not add a `FixingRatesNode` builder because this repository still has
  no source of actual fixing observations.
- [x] Update documentation in `docs/time-index-table-updates.md`, `docs/instruments.md`, and
  dashboard docs to describe Curve and Index MetaTable identities instead of
  constants and asset-indexed curve rows.

## Non-Goals

This ADR does not:

- create a fixing builder without a real fixing source
- derive fixings from the Valmer OIS curve
- change Valmer bond asset registration
- change Valmer bond pricing-detail hydration
- define a Banxico OTR curve adapter
- move Valmer source parsing into `ms-markets`

## Verification Plan

Do not mark the implementation complete until these checks pass:

- pricing schemas register or attach with `IndexTypeTable`, `IndexTable`,
  `IndexConventionDetailsTable`, `CurveTable`, current pricing detail tables,
  and pricing updater output tables
- `IndexType.upsert(**INDEX_TYPE_INTEREST_RATE_DEFINITION.as_payload())`
  returns or preserves an `interest_rate` row
- `Index` rows exist for supported TIIE and CETE reference indexes
- every supported TIIE and CETE `Index` row has
  `index_type=INDEX_TYPE_INTEREST_RATE`
- `IndexConventionDetails` rows exist for supported pricing indexes
- `Curve.get_by_unique_identifier("VALMER_TIIE_OVERNIGHT")` returns a curve row
- the Valmer TIIE builder returns a frame with `time_index`,
  `curve_identifier`, `curve`, and `key_nodes`
- `DiscountCurvesNode` writes compressed curve and key-node rows through the
  core codecs
- dashboard curve health reads the canonical discount-curve path
- no curve or fixing code calls `msc.Asset.get("TIIE_28")`
- no curve or fixing code publishes TIIE curves as asset-indexed data
- no code path claims fixing publication unless it writes real observations
