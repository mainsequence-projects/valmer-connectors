# Instruments

## Bootstrap Module

The project now keeps runtime wiring in:

- `src/instruments/bootstrap.py`

`register_all()` does three things explicitly:

- seeds project-owned constants
- registers the Valmer discount-curve ETL builder
- registers pricing-runtime `IndexSpec` entries for the TIIE family

## Pricing Runtime Registration

The TIIE pricing registrations use:

- curve constant: `ZERO_CURVE__VALMER_TIIE_28`
- index constants: `REFERENCE_RATE__TIIE_OVERNIGHT`,
  `REFERENCE_RATE__TIIE_28`, `REFERENCE_RATE__TIIE_91`,
  `REFERENCE_RATE__TIIE_182`

If `ZERO_CURVE__BANXICO_M_BONOS_OTR` already exists in the runtime, the same
bootstrap also registers CETE pricing indices against that external curve.

## Vector-To-Asset Flow

The asset-hydration path is split across two files:

- `src/data_nodes/nodes.py` decides which source rows should receive pricing
  details and pushes those updates into MainSequence assets.
- `src/instruments/vector_to_asset.py` converts one normalized Valmer row into
  one `mainsequence.instruments` object.

The flow is:

1. `ImportValmer` normalizes the vendor sheet headers with
   `normalize_column_name(...)` and derives `unique_identifier` as
   `tipovalor_emisora_serie`.
2. `_prepare_latest_inputs(...)` keeps the latest Valmer row per
   `unique_identifier`.
3. `_get_target_bonds(...)` narrows that latest snapshot to the subset the
   pricing logic currently supports.
4. `_get_uids_to_update(...)` decides which assets are missing and which
   existing assets need pricing-detail refresh.
5. `_register_and_update_pricing(...)` calls:
   `get_instrument_conventions(...)` to choose QuantLib conventions, then
   `build_qll_bond_from_row(...)` to build the actual bond object.
6. The built instrument is attached to the asset with
   `asset.add_instrument_pricing_details_from_ms_instrument(...)`.

That means the source vector ingestion and the pricing-detail hydration are
related but not identical responsibilities. `ImportValmer.update()` publishes
the source table; `ImportValmer.get_asset_list()` performs the asset/pricing
side effects.

## Instrument Classes Produced

`build_qll_bond_from_row(...)` maps supported Valmer rows into:

- `msi.ZeroCouponBond`
- `msi.FixedRateBond`
- `msi.FloatingRateBond`

Two mapping decisions are currently intentional and should be read as project
scope, not accidental behavior:

- `MC_BONOS` and `MP_BONOS` are intentionally treated as zero-coupon
  instruments by the current mapping logic.
- instrument conventions are currently implemented only for `monedaemision ==
  "MPS"`, so non-`MPS` rows are out of scope until explicit support is added

## Where To Extend The Mapping

When you need to extend the vector-to-asset path, change the layer that owns
the behavior instead of adding ad hoc conditionals everywhere.

### 1. Add A New Vendor Benchmark Mapping

If Valmer introduces a new `subyacente` label that should resolve to an
existing MainSequence index, update:

- `src/settings.py`
- `SUBYACENTE_TO_INDEX_MAP`

This is the right place for label-to-index resolution. Do not hardcode those
lookups deeper inside the bond builder unless the mapping depends on more than
the raw vendor label.

### 2. Include A New Instrument Family In Pricing Hydration

If a row family should start receiving pricing details, update:

- `ImportValmer._get_target_bonds(...)` in `src/data_nodes/nodes.py`

This function defines the supported pricing surface. If a family is not
selected here, it can still land in `vector_de_precios_valmer`, but it will not
get a pricing-detail update.

### 3. Add A New Currency Or Convention Set

If you need to support a new `monedaemision` or a different market-convention
package, update:

- `get_instrument_conventions(...)`

That is the single place that decides:

- calendar
- business-day convention
- settlement days
- day-count convention

Keep this function explicit and fail-fast. A new currency should be added here
before changing the builder itself.

### 4. Add A New Instrument Construction Rule

If the row needs a different instrument class or a different branch inside the
existing classes, update:

- `build_qll_bond_from_row(...)`

That function owns:

- zero-coupon vs fixed-rate vs floating-rate routing
- special handling for issuer/type edge cases
- coupon frequency selection
- benchmark or floating index assignment
- schedule construction

This is where a genuinely new product type or coupon rule should be introduced.

### 5. Extend The Validation Surface

After changing the mapping, validate at the right level:

- `run_price_check(...)` to compare modeled values against the sheet
- `build_position_from_sheet(...)` to generate a position-style output from a
  local vendor file
- `python scripts/validate_runtime.py` to confirm the runtime wiring still
  works against the configured backend

If the change affects the published source table shape or the target-bond
selection, also update `docs/data-nodes.md` and `README.md`.

The helper `build_position_from_sheet()` now imports a valid local
`PROJECT_BUCKET_NAME` instead of referencing the removed
`src.data_connectors.settings` module.

## Runtime Validation

Use:

```bash
python scripts/validate_runtime.py
```

The script registers the runtime, loads the TIIE 28 index curve without
hydrating fixings, and prices one sample fixed-rate bond as a smoke test.

## Remaining Limitation

This repository still does not ship local fixing-rate ETL builders, so
production runtime still depends on platform-side fixings or a sibling project
that owns those ETLs.

The repository also does not yet implement non-`MPS` convention handling in
`get_instrument_conventions(...)`. Today those rows fail fast with
`NotImplementedError`, which is intentional until that support is designed and
implemented.

## Extension Rule Of Thumb

If you are extending the repo and you are unsure where the change belongs:

- if it changes which rows are eligible, modify `_get_target_bonds(...)`
- if it changes how a field maps to an index, modify `SUBYACENTE_TO_INDEX_MAP`
- if it changes market conventions, modify `get_instrument_conventions(...)`
- if it changes the instrument type or schedule logic, modify
  `build_qll_bond_from_row(...)`

That keeps the vector-to-asset path understandable and prevents the pricing
rules from being spread across unrelated files.
