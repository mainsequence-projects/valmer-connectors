# ADR 0000: Move Generic Pricing Machinery To Core ms-markets

## Status

Proposed

## Date

2026-05-27

## Success Criteria

This ADR is successful when it gives the next implementation agent enough
precision to separate Valmer data import from generic pricing machinery.

The implementation that follows this ADR must:

- keep this project focused on importing Valmer `VectorAnalitico` and Valmer
  curve source files
- move reusable bond, schedule, curve, reference-index, and pricing-detail
  behavior into the core `ms-markets` pricing library
- define which static pricing objects should become MetaTables with foreign
  keys to `msm.models.assets.core.AssetTable`
- keep time-varying prices, curves, and fixings as DataNode output unless a
  separate platform design explicitly moves them
- leave ADR 0001 to handle asset identity and registration after this pricing
  boundary is clear

## Context

This repository is a Valmer connector. Its durable responsibility should be:

- importing Valmer source artifacts and files
- normalizing Valmer source columns
- preserving Valmer vendor data in `vector_de_precios_valmer`
- mapping Valmer labels into core pricing inputs
- calling core pricing services exposed by `ms-markets`

The current project does more than that. It also contains generic fixed-income
pricing machinery: QuantLib date conversion, coupon schedule reconciliation,
bond construction, curve serialization, reference index registration, and
pricing-detail hydration. Those are not Valmer-specific. They belong in the
shared pricing layer so future vendors and applications can reuse the same
instrument model and asset-linked pricing definitions. Valmer/Mexico-specific
instrument classification and convention selection stay in this connector.

This ADR predates ADR 0001 because asset registration and pricing hydration are
separate concerns. ADR 0001 should migrate asset identity and registration. This
ADR defines where the pricing machinery should live before pricing hydration is
refactored.

References:

- Main Sequence assets documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/markets/assets/>
- Main Sequence instruments asset/pricing details documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/instruments/assets_and_pricing_details/>
- Main Sequence instrument market data and registration documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/instruments/market_data_and_registration/>
- Local `AssetTable` source of truth:
  `.venv/lib/python3.11/site-packages/msm/models/assets/core.py:18-42`

## Current Pricing Audit

### Valmer Import Surface That Should Stay Here

These paths are Valmer-vendor ingestion and should stay in this repository:

- Valmer source column model:
  `src/valmer_connectors/data_nodes/nodes.py:145-632`
- Valmer artifact discovery and file concatenation:
  `src/valmer_connectors/data_nodes/nodes.py:1027-1098`
- Valmer identity derivation from source fields:
  `src/valmer_connectors/data_nodes/nodes.py:1075-1087`
- Valmer vector output construction:
  `src/valmer_connectors/data_nodes/nodes.py:1279-1318`
- Valmer source table metadata:
  `src/valmer_connectors/data_nodes/nodes.py:1320-1327`
- Valmer header normalization:
  `src/valmer_connectors/instruments/vector_to_asset.py:994-1000`
- Valmer target-universe selection:
  `src/valmer_connectors/data_nodes/nodes.py:901-948`
- Valmer label to reference-index mapping:
  `src/valmer_connectors/settings.py:5-24`
- Valmer MexDer curve download and CSV parsing:
  `src/valmer_connectors/instruments/rates_curves.py:8-52`

The Valmer project should keep adapter-level mapping code such as
`SUBYACENTE_TO_INDEX_MAP`. The core library should not import Valmer Spanish
column names, artifact names, bucket names, vendor file formats, or
Valmer/Mexico-specific alias taxonomy.

### Generic QuantLib And Schedule Machinery To Move

Current locations:

- QuantLib date and schedule helpers:
  `src/valmer_connectors/instruments/vector_to_asset.py:45-136`
- Vendor-sheet schedule reconciliation:
  `src/valmer_connectors/instruments/vector_to_asset.py:142-314`
- Coupon counting, cashflow table diagnostics, and schedule auto-fix logic:
  `src/valmer_connectors/instruments/vector_to_asset.py:317-580`

Target core responsibility:

- expose reusable QuantLib date conversion through core utilities
- expose a normalized schedule-reconciliation API that does not accept a
  Valmer `pd.Series`
- accept explicit inputs such as valuation date, maturity date, coupon
  frequency, expected future coupon count, elapsed coupon days, settlement
  convention, calendar, and day-count convention
- return a QuantLib schedule and diagnostics that can be reused by Valmer,
  Banxico, internal data, or any future fixed-income adapter

Valmer-specific handling that should stay here:

- parsing Valmer `fecha`, `fechavcto`, `freccpn`, `cuponesxcobrar`, and
  `diastransccpn`
- deciding whether the Valmer sheet is authoritative for coupon-count matching
- recording source-field diagnostics for Valmer import QA

### Bond Construction And Pricing Checks Boundary

Current locations:

- Valmer row to instrument builder:
  `src/valmer_connectors/instruments/vector_to_asset.py:586-747`
- Future cashflow extraction:
  `src/valmer_connectors/instruments/vector_to_asset.py:753-786`
- Valmer/Mexico-specific convention resolver:
  `src/valmer_connectors/instruments/vector_to_asset.py:794-858`
- Vendor price check loop:
  `src/valmer_connectors/instruments/vector_to_asset.py:860-991`
- Position-building demo or validation helper:
  `src/valmer_connectors/instruments/vector_to_asset.py:1003-1044`

Target core responsibility:

- build `ZeroCouponBond`, `FixedRateBond`, and `FloatingRateBond` from a
  normalized instrument-pricing payload
- own the reusable bond pricing lifecycle, analytics comparison, cashflow
  extraction, and content hash behavior
- expose a provider-neutral price-check framework that compares vendor prices
  with model prices

Valmer-specific handling that should stay here:

- mapping `tipovalor`, `emisora`, `serie`, `reglacupon`, `subyacente`,
  `tasacupon`, `sobretasa`, and `valornominalactualizado` into the normalized
  core payload
- selecting Valmer/Mexico-specific conventions such as calendar,
  business-day convention, settlement days, day count, reference-index family,
  and tenor defaults
- preserving Valmer raw clean price, dirty price, accrued interest, current
  coupon, and vendor yield in the Valmer vector DataNode
- deciding which Valmer instruments are supported by this connector today

The local core pricing package already owns bond model classes:

- `msm_pricing.instruments.base_instrument.InstrumentModel`:
  `.venv/lib/python3.11/site-packages/msm_pricing/instruments/base_instrument.py:13-105`
- `msm_pricing.instruments.bond.Bond` and bond fields:
  `.venv/lib/python3.11/site-packages/msm_pricing/instruments/bond.py:71-160`

The migration should extend that core surface instead of keeping a second
Valmer-only bond factory.

### Curve And Reference-Index Machinery To Split

Current locations:

- Duplicate curve compression and decompression:
  `src/valmer_connectors/data_nodes/nodes.py:665-714`
- Valmer standalone TIIE curve DataNode:
  `src/valmer_connectors/data_nodes/nodes.py:719-782`
- Valmer curve builder:
  `src/valmer_connectors/instruments/rates_curves.py:8-52`
- Curve and rate constants:
  `src/valmer_connectors/instruments/bootstrap.py:9-27`
- Discount-curve builder registration:
  `src/valmer_connectors/instruments/bootstrap.py:51-61`
- Mexican TIIE and CETE index spec registration:
  `src/valmer_connectors/instruments/bootstrap.py:64-148`
- Dashboard curve decoding:
  `dashboards/valmer_monitor/valmer_dashboard.py:344-389`

Core already has the generic curve codec and curve DataNode surface:

- `msm_pricing.data_nodes.curve_codec.compress_curve_to_string` and
  `decompress_string_to_curve`:
  `src/msm_pricing/data_nodes/curve_codec.py`
- `msm_pricing.data_nodes.curves.DiscountCurvesNode`:
  `src/msm_pricing/data_nodes/curves.py`
- `msm_pricing.data_nodes.index_fixings.FixingRatesNode`:
  `src/msm_pricing/data_nodes/index_fixings.py`
- generic `IndexSpec` and `ibor_spec` shape:
  `.venv/lib/python3.11/site-packages/msm_pricing/models/indices_builders.py:1-84`

Target core responsibility:

- own curve payload encoding and decoding
- own discount-curve and fixing-rate DataNode base behavior
- own curve interpolation, compounding, day-count, and extrapolation choices
  needed by pricing engines
- own reference-index definition shape, while this connector keeps
  Valmer/Mexico-specific index and curve alias policy

Valmer-specific handling that should stay here:

- downloading `MEXDERSWAP_IRSTIIEPR.csv`
- decoding Valmer CSV columns and date semantics
- mapping Valmer curve source names to core curve constants
- registering the Valmer provider builder with the core discount-curve builder
  registry

No explicit interpolation implementation was found in this repository. That is
another signal that interpolation policy should be introduced in core next to
curve construction and pricing engines, not inside the Valmer adapter.

### Pricing Hydration To Move Behind Core Service

Current location:

- mixed registration and pricing orchestration:
  `src/valmer_connectors/data_nodes/nodes.py:1124-1224`
- pricing decision logic:
  `src/valmer_connectors/data_nodes/nodes.py:950-1025`
- current instrument payload build:
  `src/valmer_connectors/data_nodes/nodes.py:1177-1197`
- current legacy asset pricing-detail write:
  `src/valmer_connectors/data_nodes/nodes.py:1216-1220`

This path is not asset registration. It is pricing-detail hydration. It should
be replaced by a core `ms-markets` service that accepts:

- `asset_uid` or resolved `AssetTable.uid`
- normalized instrument-pricing payload
- source provider metadata
- pricing details date or effective date
- content hash or instrument hash

The Valmer adapter should call that service after it has imported the latest
VectorAnalitico rows and selected the target bonds.

## Decision

Move reusable pricing machinery out of this repository and into core
`ms-markets`/`msm_pricing`.

This repository remains a Valmer adapter. It will not own reusable bond pricing,
curve interpolation, QuantLib schedule behavior, or asset-linked
instrument-pricing persistence. It will own Valmer/Mexico-specific instrument
classification, convention selection, and alias mapping.

Core pricing will own:

- QuantLib date, period, schedule, and cashflow utilities
- schedule reconciliation from normalized expected coupon data
- fixed-income bond construction from normalized payloads
- reference-index specs and index-to-curve relationships
- curve payload codecs, curve interpolation policy, and pricing curve
  construction
- provider-neutral price-check analytics
- asset-linked pricing-definition MetaTables
- a service for creating or updating pricing definitions for an asset

The Valmer adapter will own:

- artifact and source-file acquisition
- Valmer column normalization
- VectorAnalitico-specific field validation
- Valmer-specific source identity construction
- Valmer-to-core field mapping
- Valmer provider metadata and source diagnostics
- Valmer dashboard monitoring

## Existing Core Pricing Surfaces

The sibling `ms-markets` checkout already contains the core pricing package and
the static pricing MetaTables this ADR originally proposed. The migration should
use those surfaces instead of adding duplicate Valmer-local models.

Covered package boundary:

- `src/msm_pricing/README.md` documents `msm_pricing` as the public package for
  pricing services, instruments, static pricing MetaTables, and DataNodes.
- `src/msm_pricing/meta_tables.py:24-109` exposes
  `pricing_sqlalchemy_models()` and `register_pricing_meta_tables(...)`.
- `pricing_sqlalchemy_models()` returns `AssetTable`, `IndexTable`,
  `IndexConventionDetailsTable`, `CurveTable`, and
  `AssetCurrentPricingDetailsTable`, so pricing registration is explicitly
  separated from base market asset registration.

### Asset-Linked Instrument Pricing Definition

Covered by `AssetCurrentPricingDetailsTable` in
`src/msm_pricing/models/pricing_details.py:20-58`.

The table is the current core equivalent of the originally proposed
`AssetInstrumentPricingDefinitionTable`:

- `asset_uid` is both the primary key and a foreign key to `AssetTable.uid`.
- `instrument_type`, `instrument_dump`, `pricing_details_date`,
  `serialization_format`, `pricing_package_version`, `source`, and
  `metadata_json` store the serialized pricing definition.
- `src/msm_pricing/api/pricing_details.py:43-214` exposes the API model and
  upsert behavior by `asset_uid`.
- `src/msm_pricing/api/instruments.py:18-66` persists and reloads current
  instrument pricing details from assets.

The remaining migration question is not table creation. It is whether Valmer
should write through the existing `AssetCurrentPricingDetails` API directly, or
through a thin service wrapper that also records Valmer source diagnostics.

### Reference Index Definitions And Fixings

Covered by the existing index object plus pricing-specific convention details:

- `src/msm/models/indices.py:19-52` defines `IndexTable`, the base index object.
- `src/msm_pricing/models/index_convention_details.py:19-52` defines
  `IndexConventionDetailsTable`, keyed by `index_uid` with a foreign key to
  `IndexTable.uid`, and stores the serialized convention dump.
- `src/msm/data_nodes/indices/timestamped.py:41-136` defines the index
  identifier foreign key helper and `IndexTimestampedDataNode`.
- `src/msm_pricing/data_nodes/index_fixings.py:111-159` defines
  `FixingRatesNode` for timestamped index fixings.

This covers the `ReferenceIndexDefinitionTable` / `IndexPricingDetails`
concept for this ADR. Valmer should not introduce a parallel index-definition
MetaTable.

### Curve Definitions And Curve Time Series

Covered by `CurveTable` and the pricing curve DataNodes:

- `src/msm_pricing/models/curves.py:21-73` defines `CurveTable` with
  `unique_identifier`, `curve_type`, `index_uid`, `interpolation_method`,
  `compounding`, `source`, and metadata.
- `src/msm_pricing/data_nodes/curves.py:159-245` defines
  `CurveTimestampedDataNode` and `DiscountCurvesNode` for time-varying curve
  values.

This covers the originally proposed `DiscountCurveDefinitionTable`. Curve
points remain DataNode output; static curve identity and interpolation policy
belong to `CurveTable`.

### Instrument Dependencies On Indexes And Curves

Do not add a separate `InstrumentReferenceIndexLinkTable` for this migration.
The current core shape already represents the dependency chain through the
serialized instrument pricing details, `IndexTable`,
`IndexConventionDetailsTable`, and `CurveTable`.

A separate link table would only be justified later if the platform needs a
queryable dependency graph independent of the serialized instrument dump. That
is not required for moving Valmer pricing behavior out of this connector.

## Proposed Migration

1. Use the existing core APIs in `msm_pricing` for pricing MetaTable
   registration, instrument serialization, current asset pricing details,
   index convention details, and fixings.
2. Add core schedule reconciliation that accepts provider-neutral inputs rather
   than a Valmer `pd.Series`.
3. Keep Valmer/Mexico-specific convention selection in this project and pass
   normalized convention outputs into the core pricing APIs.
4. Do not add duplicate core pricing MetaTables for this migration; use the
   existing `AssetCurrentPricingDetailsTable` and
   `IndexConventionDetailsTable`.
5. Add or select a core service wrapper around the existing
   `AssetCurrentPricingDetails` API if Valmer needs one place to attach source
   diagnostics before writing current asset pricing details.
6. Refactor `src/valmer_connectors/instruments/vector_to_asset.py` so it becomes a Valmer row
   mapper and validation adapter, not a bond pricing library.
7. Keep `ImportValmer.prepare_for_update()` as the explicit orchestration
   boundary before `ImportValmer.run(...)`. Inside that preparation phase,
   `_sync_asset_registry_and_pricing(...)` keeps the Valmer asset/detail sync
   and current pricing hydration visible instead of hiding them in
   `get_asset_list()`.

## Implementation Tasks

Curve-related implementation tasks are intentionally deferred to a later curve
refactor.

- [x] Add this ADR and link it before ADR 0001 in the documentation navigation.
- [x] In core `ms-markets`, define the public package location for pricing
  services and static pricing MetaTables. Covered by `src/msm_pricing/README.md`
  and `src/msm_pricing/meta_tables.py`.
- [x] In core `ms-markets`, add an asset-linked current pricing definition table
  with `asset_uid` as a foreign key to `AssetTable.uid`. Covered by
  `AssetCurrentPricingDetailsTable`; the earlier ADR name
  `AssetInstrumentPricingDefinitionTable` should not be introduced as a
  duplicate table.
- [x] In core `ms-markets`, add reference index pricing details for reference
  rate assets such as TIIE and CETE. Covered by `IndexTable`,
  `IndexConventionDetailsTable`, `IndexTimestampedDataNode`, and
  `FixingRatesNode`.
- [x] In core `msm_pricing`, expose provider-neutral schedule reconciliation
  currently implemented in `src/valmer_connectors/instruments/vector_to_asset.py:142-580`.
  Implemented by
  `msm_pricing.pricing_engine.coupon_schedules.compute_coupon_schedule_force_match(...)`
  in the sibling `ms-markets` checkout; this project now keeps
  `compute_sheet_schedule_force_match(...)` as a Valmer row adapter.
- [x] In core `msm_pricing`, expose provider-neutral cashflow extraction and
  price-check analytics currently implemented in
  `src/valmer_connectors/instruments/vector_to_asset.py:753-991`. Implemented by
  `msm_pricing.pricing_engine.bond_analytics.compare_bond_to_market_quote(...)`
  in the sibling `ms-markets` checkout; this project now maps Valmer row fields
  into that core comparison helper.
- [x] Keep Mexican fixed-income convention helpers in this project. Do not move
  Valmer/Mexico-specific convention selection from
  `src/valmer_connectors/instruments/vector_to_asset.py` or `src/valmer_connectors/instruments/bootstrap.py` into
  core `msm_pricing`; those rules are adapter policy, not base pricing-library
  behavior.
- [x] In this project, convert Valmer source rows into normalized core pricing
  payloads before constructing instruments. Implemented in
  `src/valmer_connectors/instruments/vector_to_asset.py` with `CoreBondPricingPayload`,
  `valmer_row_to_core_bond_pricing_payload(...)`, and
  `build_instrument_from_core_bond_pricing_payload(...)`; Valmer-specific
  convention and alias policy stays in this project.
- [x] In this project, replace direct calls to
  `asset.add_instrument_pricing_details_from_ms_instrument(...)` at
  `src/valmer_connectors/data_nodes/nodes.py:1216-1220` with the core pricing-definition
  service. Implemented with
  `msm_pricing.api.instruments.persist_current_pricing_details(...)`.
- [x] In this project, keep `SUBYACENTE_TO_INDEX_MAP` as a Valmer/Mexico
  adapter alias map. Do not move this taxonomy into core `msm_pricing`.

## Non-Goals

- Do not move Valmer artifact bucket handling into core.
- Do not move Valmer source column names or Spanish header normalization into
  core.
- Do not move Valmer/Mexico-specific instrument convention selection,
  reference-index alias mapping, or supported-instrument taxonomy into core.
- Do not change asset identity or asset registration in this ADR; that is ADR
  0001.
- Do not replace price history DataNodes with MetaTables.
- Do not claim all Valmer instruments are supported. The existing supported
  target universe remains explicit until core pricing expands.
- Do not create portfolio construction behavior in this migration.

## Validation Plan

The migration is valid only when all of these checks pass:

- Valmer vector import still publishes `vector_de_precios_valmer` with the same
  source fields and OHLC mapping.
- Supported Valmer bonds produce the same instrument hash before and after the
  migration, or any hash differences are explained by an intentional core
  serialization change.
- Supported Valmer bonds produce materially equivalent model dirty price, clean
  price, accrued interest, current coupon, and future coupon count.
- Pricing definitions are persisted through core MetaTables linked to
  `AssetTable.uid`.
- Dashboard pricing-health paths use the new core surfaces.

## Consequences

This migration makes the Valmer connector thinner and makes pricing behavior
reusable by other vendors.

It also creates a dependency ordering: ADR 0001 owns asset identity and
registration, while this ADR owns the current pricing-definition upsert. The
Valmer adapter now calls the core pricing-definition service instead of the
legacy asset instance method.
