# Instrument Mapping

This page documents the Valmer row-to-instrument adapter. It does not document
updater output table, AssetTable registration, or curve publication.

## Owner File

The adapter lives in:

- `src/valmer_connectors/instruments/vector_to_asset.py`

The main entry point is:

- `build_qll_bond_from_row(...)`

That function converts one normalized Valmer source row into one
`msm_pricing.instruments` object.

## Mapping Flow

```text
normalized Valmer row
    |
    v
get_instrument_conventions(...)
    |
    +-- calendar
    +-- business-day convention
    +-- settlement days
    +-- day-count convention
    |
    v
build_qll_bond_from_row(...)
    |
    v
valmer_row_to_core_bond_pricing_payload(...)
    |
    v
build_instrument_from_core_bond_pricing_payload(...)
    |
    v
msm_pricing instrument
```

The adapter builds a provider-neutral `CoreBondPricingPayload`, which is a
local compatibility alias for `msm_pricing.instruments.BondInstrumentTerms`,
before creating the `msm_pricing` instrument. That keeps Valmer row parsing
separate from core pricing object construction.

## Supported Instrument Classes

The current adapter can produce:

- `msi.ZeroCouponBond`
- `msi.FixedRateBond`
- `msi.FloatingRateBond`

Supported rows are selected before instrument construction by:

- `ImportValmer._get_target_bonds(...)`

Rows that are not selected are ignored by the default vector update. They can
only be registered after the project has an explicit Valmer asset-type
classifier for the broader source universe.

Do not extend asset registration by widening `build_qll_bond_from_row(...)` or
the pricing adapter. A different vector or non-bond Valmer family needs its own
asset classifier first. After the classifier assigns `unique_identifier` and an
explicit `asset_type`, that extension can register assets through its own
service layer and then call its own instrument/pricing adapter if pricing
hydration is required.

## Convention Rules

`get_instrument_conventions(...)` is the project-local convention boundary. It
owns Mexican market convention selection and currently supports only the
configured `MPS` branch.

Do not move these Valmer/Mexico source conventions into core `msm_pricing`
unless the same rule is provider-neutral and reusable outside this project.

## Vendor Alias Mapping

`SUBYACENTE_TO_INDEX_MAP` in `src/valmer_connectors/settings.py` maps Valmer
source labels to canonical Mexican reference index identifiers.

Examples:

- `TIIE28` -> `TIIE_28`
- `TIIE91` -> `TIIE_91`
- `Tasa TIIE Fondeo 1D` -> `TIIE_OVERNIGHT`
- `CETE28` -> `CETE_28`

This is source-adapter policy, not core pricing policy.

This map must only point to real `Index` rows. Do not map any Valmer label to
`MXN_GOVERNMENT_BOND`; that identifier is the Valmer government curve identity,
not an index. M Bono benchmark pricing details may use real CETE selector
indexes such as `CETE_28` or `CETE_182`. Z-spread valuation resolves those
selectors to `VALMER_MXN_GOVERNMENT_BOND` through
`PricingMarketDataSetCurveBinding` with `quote_side="mid"`.

TIIE floaters keep their floating indexes as real TIIE selector indexes, for
example `TIIE_28` or `TIIE_182`. Projection and TIIE z-spread curve selection
also go through explicit market-data-set curve bindings.

## Schedule Construction

Coupon schedule reconciliation is delegated to the shared pricing utility now
imported from `msm_pricing.pricing_engine.bond_utils.coupon_schedules`.

The Valmer adapter passes source-specific arguments into that utility. The core
utility owns provider-neutral schedule reconciliation; this project owns the
source-row interpretation.

## Extension Points

Change the smallest layer that owns the behavior:

- New Valmer benchmark label: update `SUBYACENTE_TO_INDEX_MAP`.
- New eligible family for pricing hydration: update
  `ImportValmer._get_target_bonds(...)`.
- New currency or market convention: update `get_instrument_conventions(...)`.
- New product branch: update `valmer_row_to_core_bond_pricing_payload(...)` and
  `build_instrument_from_core_bond_pricing_payload(...)`.

After changing this page's behavior, also review `pricing.md` because pricing
hydration decides when the adapter is called.
