# Curve Resolution And Asset Patching Plan

This document records the Valmer curve-resolution fix, the local implementation
state, and the remaining live platform validation steps. Live platform updates
remain separate from local code and documentation changes.

## Goal

Fix Valmer pricing and curve resolution so the project follows the current
`ms-markets` curve model:

```text
instrument index uid
    -> pricing market-data-set curve binding
    -> curve uid
    -> curve building details
    -> DiscountCurvesNode observations
```

The implementation must:

- stop creating `MXN_GOVERNMENT_BOND` as an `Index`
- keep real TIIE and CETE reference indexes
- create independent Valmer curve rows
- bind real index selectors to those curves through
  `PricingMarketDataSetCurveBinding.upsert_index_curve_selection(...)`
- create `CurveBuildingDetails` rows for every Valmer curve
- patch existing asset pricing details through the normal Valmer vector
  updater path, with explicit repair controls

## Sources Checked

The current source of truth is the copied `ms-markets` fixed-income curve skill
at `ms-markets==0.0.89` and the neighboring `ms-markets` checkout:

- `/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/docs/ADR/0035-pricing-curve-identity-and-market-data-curve-bindings.md`
- `/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/docs/knowledge/msm_pricing/curves.md`
- `/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/docs/knowledge/msm_pricing/market_data_sets.md`
- `/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/docs/knowledge/msm_pricing/runtime_resolution.md`
- `/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/examples/msm_pricing/bond_pricing_example/main.py`

Local startup state after refresh:

- `.agents/skills/mainsequence/PINNED_FROM.txt` is pinned to
  `mainsequence==4.4.25`.
- `.agents/skills/ms_markets/PINNED_FROM.txt` is pinned to
  `ms-markets==0.0.89`.
- The copied fixed-income curve skill documents the current
  `PricingMarketDataSetCurveBinding` and `CurveKeyNode` model.

## Resolved Gaps

### 1. Synthetic Government Bond Index Removed

The broken implementation created this synthetic index:

```text
MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER = "MXN_GOVERNMENT_BOND"
```

That was wrong under the fixed model. `MXN_GOVERNMENT_BOND` is a curve family
and valuation curve identity. It is not a rate index, fixing index, or coupon
index.

Current local code removes it from:

- `MEXICAN_REFERENCE_INDEX_DEFINITIONS`
- `MEXICAN_INDEX_CONVENTION_DEFINITIONS`
- `VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION`

### 2. Curve Creation No Longer Uses `index_uid`

The old curve definitions resolved an index first:

```text
upsert_valmer_tiie_curve(...)
upsert_valmer_mxn_government_bond_curve(...)
```

The current `ms-markets` `Curve` API no longer accepts `index_uid` as curve
identity. Curve selection belongs in `PricingMarketDataSetCurveBinding`. For
index-scoped selectors, the public API is
`PricingMarketDataSetCurveBinding.upsert_index_curve_selection(...)`; use raw
`PricingMarketDataSetCurveBinding.upsert(...)` only for generic selectors such
as `currency`.

Current local code emits curve-only payloads and uses
`PricingMarketDataSetCurveBinding.upsert_index_curve_selection(...)` for
TIIE/CETE index selectors.

### 3. Runtime Attachment Includes Pricing Tables

`valmer_pricing_runtime_models()` attaches the market-data and build-detail
tables needed by runtime resolution:

- `CurveBuildingDetailsTable`
- `PricingMarketDataSetTable`
- `PricingMarketDataSetBindingTable`
- `PricingMarketDataSetCurveBindingTable`
- `DiscountCurvesStorage`
- `IndexFixingsStorage`

### 4. Valmer Seeds Build Details And Bindings

`bootstrap_valmer_curve_pricing()` creates:

- index type
- reference indexes
- index conventions
- curves
- `CurveBuildingDetails`
- `PricingMarketDataSet`
- `PricingMarketDataSetBinding`
- `PricingMarketDataSetCurveBinding`

Getting a usable curve requires all of these rows:

```text
PricingMarketDataSetBinding
    market_data_set + discount_curves -> DiscountCurvesStorage

PricingMarketDataSetCurveBinding
    market_data_set + role + selector + quote_side -> curve_uid

CurveBuildingDetails
    curve_uid -> zero-rate build policy

DiscountCurvesNode observations
    curve_identifier -> stored curve points
```

A `z_spread_base` binding alone is not the general mechanism for getting a
curve. It selects the benchmark curve for z-spread analytics only.

### 5. TIIE Curve Identity And Type

The new `resolve_quantlib_index(...)` path resolves floating indexes with
`role_key="projection"` and validates the selected curve has
`curve_type="projection"`.

Valmer TIIE bootstrapping must publish the overnight/OIS curve identity
`VALMER_TIIE_OVERNIGHT` with `curve_type="projection"`. `TIIE_28`, `TIIE_91`,
and `TIIE_182` remain index tenor/frequency selectors that can resolve to that
overnight/OIS curve through market-data-set curve bindings.

### 6. Existing Asset Pricing Details Need Rehydration

Persisted `AssetCurrentPricingDetails` rows store serialized instrument payloads.
Rows created before the fix may contain stale benchmark/floating index UID
choices or may rely on missing curve bindings.

The patch should not be a custom direct write. It should run through the normal
Valmer vector updater workflow:

```text
valmer-connectors vector update
    -> ImportValmer.prepare_for_update(...)
    -> _sync_asset_registry_and_pricing(...)
    -> build_qll_bond_from_row(...)
    -> add_many_pricing_details(...)
    -> ImportValmer.run(force_update=True)
```

The current service already calls
`prepare_for_update(force_pricing_update=True)` in its normal update/backfill
paths, but source rows can still be filtered out by the vector cursor before
pricing hydration. A repair run needs an explicit way to bypass that source-row
cursor filter.

## Target Model

### Real Indexes

Keep these as `Index` rows:

```text
TIIE_OVERNIGHT
TIIE_28
TIIE_91
TIIE_182
CETE_28
CETE_91
CETE_182
```

Remove this from index registration:

```text
MXN_GOVERNMENT_BOND
```

TIIE and CETE indexes are legitimate selectors because Valmer instruments can
reference them as floating or benchmark indexes. The government bond curve is
not a selector index.

### Curves

Create independent `Curve` rows:

| Curve | Curve Type | Currency | Purpose |
| --- | --- | --- | --- |
| `VALMER_TIIE_OVERNIGHT` | `projection` | `MXN` | Mid forward/projection curve for TIIE-indexed floaters |
| `VALMER_MXN_GOVERNMENT_BOND` | `discount` | `MXN` | Mid government discount and z-spread base curve for CETE/M Bono benchmark selectors |

Do not put `index_uid` on either curve.

Set `Curve.quote_side="mid"` on these Valmer curve rows. That marks the curve
identity as a mid curve, but it does not replace the binding quote side. The
curve binding must also be written with `quote_side="mid"`.

If the product later needs a separate TIIE discount curve and a TIIE projection
curve, create two curve rows. Do not overload one curve row with a physical type
that contradicts the resolver role.

### Curve Building Details

Each Valmer curve needs one `CurveBuildingDetails` row keyed by `curve_uid`.

Initial build-detail policy:

| Curve | Builder Details |
| --- | --- |
| `VALMER_TIIE_OVERNIGHT` | `builder_type="zero_rate_curve"`, `quote_convention="zero_rate"`, `rate_unit="decimal"`, `day_counter_code="Actual360"`, `calendar_code="Mexico"` for `CurveBuildingDetails`; index convention dumps use QuantLib calendar JSON `{"name": "Mexico"}`, `interpolation_method="log_linear_discount"`, `compounding="compounded_annual"`, `extrapolation_policy="enabled"` |
| `VALMER_MXN_GOVERNMENT_BOND` | same zero-rate build policy, matching the zero-rate points exported by `build_mxn_government_curve_frame(...)` |

The current Valmer builders already emit decimal zero rates:

- `src/valmer_connectors/instruments/rates_curves.py`
- `src/valmer_connectors/instruments/mexican_government_bond_curve.py`

The implementation must verify that `calendar_code` is accepted by
`msm_pricing.instruments.json_codec.calendar_from_json(...)`. The unsupported
literal string `"Mexico/BMV"` must not be persisted. Use `calendar_code="Mexico"`
where the schema requires a string calendar code, and use QuantLib calendar JSON
`{"name": "Mexico"}` inside convention dumps that are decoded by
`calendar_from_json(...)`.

### Market-Data Set Source Bindings

Seed the default pricing market-data set and source bindings:

```text
PricingMarketDataSet(set_key="default")

default + discount_curves
    -> DiscountCurvesStorage.get_meta_table_uid()

default + interest_rate_index_fixings
    -> IndexFixingsStorage.get_meta_table_uid()
```

The source binding only tells the runtime where to read curve/fixing
observations. It does not select which curve to use.

### Curve Selection Bindings

Use `quote_side="mid"` for these Valmer curve bindings. The new resolver has no
implicit `mid` fallback: a binding written as `mid` is found only by runtime
calls that also request `mid`.

For index-scoped selectors, seed bindings through:

```text
PricingMarketDataSetCurveBinding.upsert_index_curve_selection(...)
```

That helper writes the generic selector fields as `selector_type="index"` and
`selector_key=str(index.uid)`. Do not hand-author those fields for TIIE/CETE
index selections.

Required Valmer index-role bindings:

| Role | Selector | Curve |
| --- | --- | --- |
| `projection` | `index:<TIIE_OVERNIGHT.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `index:<TIIE_28.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `index:<TIIE_91.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `index:<TIIE_182.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `discount` | `index:<TIIE_OVERNIGHT.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `discount` | `index:<TIIE_28.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `discount` | `index:<TIIE_91.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `discount` | `index:<TIIE_182.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `index:<TIIE_OVERNIGHT.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `index:<TIIE_28.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `index:<TIIE_91.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `index:<TIIE_182.uid>:mid` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `index:<CETE_28.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `projection` | `index:<CETE_91.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `projection` | `index:<CETE_182.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `discount` | `index:<CETE_28.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `discount` | `index:<CETE_91.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `discount` | `index:<CETE_182.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `z_spread_base` | `index:<CETE_28.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `z_spread_base` | `index:<CETE_91.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `z_spread_base` | `index:<CETE_182.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |

Do not add `discount:currency:MXN:mid`. MXN currency is descriptive metadata,
not a discounting policy. Current floating-rate resolution in `msm_pricing`
requires both projection and discount roles for instruments carrying floating
index UIDs, so Valmer seeds the explicit TIIE and CETE index-role bindings
above. Fixed-rate and zero-coupon discount policy must also be explicit; it must
not be hidden behind a currency fallback.

Runtime calls must request the same side:

```text
floating/index projection: curve_quote_side="mid"
benchmark z-spread base:  benchmark_curve_quote_side="mid"
curve reference checks:   curve_quote_side="mid" and benchmark_curve_quote_side="mid"
```

## Implementation Tasks

Use this checklist as the implementation ledger. Mark an item `- [x]` only
when the local code/docs change and the stated validation are complete. Keep
live platform operations unchecked until the commands are actually run against
the target project.

- [x] Remove synthetic government-bond index.
- [x] Refactor Valmer curve rows so `Curve.upsert(...)` receives no
      `index_uid`; `VALMER_TIIE_OVERNIGHT` is `projection`,
      `VALMER_MXN_GOVERNMENT_BOND` is `discount`, and both use
      `quote_side="mid"`.
- [x] Seed `CurveBuildingDetails` for `VALMER_TIIE_OVERNIGHT` and
      `VALMER_MXN_GOVERNMENT_BOND`; verified `calendar_code="Mexico"` with the
      current `msm_pricing` JSON codec.
- [x] Seed the default `PricingMarketDataSet` source bindings for
      `DiscountCurvesStorage` and `IndexFixingsStorage`.
- [x] Seed explicit `mid` index curve bindings through
      `PricingMarketDataSetCurveBinding.upsert_index_curve_selection(...)`.
- [x] Attach resolver-required pricing models:
      `CurveBuildingDetailsTable`, `PricingMarketDataSetTable`,
      `PricingMarketDataSetBindingTable`,
      `PricingMarketDataSetCurveBindingTable`, `DiscountCurvesStorage`, and
      `IndexFixingsStorage`.
- [x] Correct and verify instrument index mapping: `SUBYACENTE_TO_INDEX_MAP`
      never maps to `MXN_GOVERNMENT_BOND`; M Bono benchmark selectors remain
      real CETE index UIDs; TIIE floaters remain real TIIE index UIDs.
- [x] Add normal TimeIndexTableUpdater repair controls:
      `VALMER_FORCE_PRICING_DETAILS_PATCH`,
      `VALMER_VECTOR_BYPASS_CURSOR_FILTER`,
      `--force-pricing-details-patch`, and `--bypass-vector-cursor-filter`.
- [x] Add focused local tests for no synthetic index, no curve `index_uid`,
      `CurveBuildingDetails`, source bindings, explicit `mid` curve bindings,
      real instrument index selectors, and TimeIndexTableUpdater repair controls.
- [x] Attach Valmer source-specific `key_nodes` semantic validators for
      `VALMER_TIIE_OVERNIGHT`, `VALMER_USD_SOFR_OVERNIGHT`, and
      `VALMER_MXN_GOVERNMENT_BOND` before core `DiscountCurvesNode`
      compression.
- [x] Run local validation:
      `py_compile`, `tests.test_curve_bootstrap`,
      `tests.test_valmer_instrument_index_uids`,
      `tests.test_valmer_vector_storage`,
      `tests.test_vector_update_service`,
      `tests.test_curve_update_service`,
      `tests.test_rates_curves`, and
      `tests.test_mxn_government_bond_curve`.
- [x] Update project docs:
      `docs/pricing.md`, `docs/instruments.md`, `docs/source-import.md`,
      `docs/new-version-migration.md`, and ADR 0004.
- [ ] Run live curve and asset patch:
      `valmer-connectors curves update-tiie-irs-mxn`;
      `valmer-connectors curves update-usd-sofr`;
      `valmer-connectors curves update-mxn-government`;
      `VALMER_FORCE_PRICING_DETAILS_PATCH=1
      VALMER_VECTOR_BYPASS_CURSOR_FILTER=1 valmer-connectors vector update`.
- [ ] Verify live curve observations exist for `VALMER_TIIE_OVERNIGHT`,
      `VALMER_USD_SOFR_OVERNIGHT`, and `VALMER_MXN_GOVERNMENT_BOND`.
- [ ] Verify live pricing-detail patch results: sample M Bono current pricing
      details use a real CETE benchmark index UID; TIIE projection resolves to
      `VALMER_TIIE_OVERNIGHT` with `curve_quote_side="mid"`; CETE z-spread resolves to
      `VALMER_MXN_GOVERNMENT_BOND` with
      `benchmark_curve_quote_side="mid"`.
- [ ] Clean old backend static rows after live payload references are gone:
      query for old `MXN_GOVERNMENT_BOND` index UID references, remove or
      deactivate the stale index/convention row, record cleanup evidence in
      `docs/new-version-migration.md`, and re-verify z-spread resolution.

## Implementation Phases

### Phase 0: Startup Hygiene

Before code changes:

1. Run `mainsequence code-repository update-sdk --path .`.
2. Compare `mainsequence --version` with
   `.agents/skills/mainsequence/PINNED_FROM.txt`.
3. Refresh `AGENTS.md` and `.agents/skills/mainsequence/` if the pin differs.
4. Re-check the copied fixed-income skill and confirm it documents the new
   `PricingMarketDataSetCurveBinding` model before implementing runtime code.

This phase may create scaffold-only diffs. Keep those separate from the actual
curve-resolution code change if possible.

### Phase 1: Refactor Static Curve Bootstrap

Files:

- `src/valmer_connectors/instruments/curve_bootstrap.py`
- `tests/test_curve_bootstrap.py`

Required changes:

1. Remove `MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER`.
2. Remove `MXN_GOVERNMENT_BOND` from `MEXICAN_REFERENCE_INDEX_DEFINITIONS`.
3. Remove its `MexicanIndexConventionDefinition`.
4. Remove `index_unique_identifier` from `ValmerCurveDefinition`.
5. Replace `to_curve_payload(index_uid=...)` with a curve-only payload.
6. Add `currency_code` and optional `quote_side` to curve definitions.
7. Set `VALMER_TIIE_OVERNIGHT.curve_type = "projection"`.
8. Keep `VALMER_MXN_GOVERNMENT_BOND.curve_type = "discount"`.
9. Add static definition objects for `CurveBuildingDetails`.
10. Add static definition objects for market-data source bindings and mid curve
    selection bindings.
11. Update `bootstrap_valmer_curve_pricing()` to seed indexes, conventions,
    curves, build details, market-data-set source bindings, and curve-selection
    bindings in one idempotent path.

Acceptance checks:

- no code path passes `index_uid` into `Curve.upsert(...)`
- no test expects `MXN_GOVERNMENT_BOND` in reference indexes
- no test expects a government bond index convention
- tests assert the CETE/TIIE curve binding matrix
- tests assert `quote_side="mid"` on Valmer curve bindings
- tests assert every curve has build details

### Phase 2: Attach The Correct Runtime Models

Files:

- `src/valmer_connectors/instruments/curve_bootstrap.py`
- `src/valmer_connectors/instruments/bootstrap.py`
- `tests/test_curve_bootstrap.py`

Required runtime model additions:

```text
CurveBuildingDetailsTable
PricingMarketDataSetTable
PricingMarketDataSetBindingTable
PricingMarketDataSetCurveBindingTable
DiscountCurvesStorage
IndexFixingsStorage
```

Do not add schema creation shortcuts. These tables are already owned by
`ms-markets` / `msm_pricing` migrations. Valmer should attach and seed rows
only after the core migrations have run.

Acceptance checks:

- `attach_pricing_schemas(...)` receives the new runtime model list
- `DiscountCurvesStorage.get_meta_table_uid()` is available before source
  binding upsert
- `IndexFixingsStorage.get_meta_table_uid()` is available before fixing source
  binding upsert

### Phase 3: Correct Instrument Mapping Boundaries

Files:

- `src/valmer_connectors/settings.py`
- `src/valmer_connectors/instruments/vector_to_asset.py`
- `tests/test_valmer_instrument_index_uids.py`
- `tests/test_valmer_vector_storage.py`

Rules:

1. `SUBYACENTE_TO_INDEX_MAP` may map Valmer labels to real TIIE/CETE indexes.
2. It must never map anything to `MXN_GOVERNMENT_BOND`.
3. M BONO pricing details may keep `benchmark_rate_index_uid` as `CETE_28`,
   `CETE_91`, or `CETE_182` when that is the Valmer benchmark selector.
4. M BONO z-spread resolution then uses the CETE selector binding to resolve
   `VALMER_MXN_GOVERNMENT_BOND`.
5. Floating-rate instruments must keep `floating_rate_index_uid` as the real
   TIIE index.
6. For floating-rate instruments, decide explicitly whether
   `benchmark_rate_index_uid` should also be populated. If yes, create the
   matching `z_spread_base` binding. If no, do not write it only because the old
   payload used to duplicate the floating index.

Acceptance checks:

- M BONO rows with `CETE_28`/`CETE_182` selectors serialize those real index
  UIDs
- no serialized instrument payload contains an index UID belonging to a fake
  `MXN_GOVERNMENT_BOND` row
- floating-rate TIIE payloads still carry valid `floating_rate_index_uid`
- z-spread tests resolve through `PricingMarketDataSetCurveBinding`, not
  through a curve-owned index
- z-spread tests pass `benchmark_curve_quote_side="mid"` when resolving Valmer
  mid bindings

### Phase 4: Add Explicit Repair Controls To The Normal TimeIndexTableUpdater Path

Files:

- `src/valmer_connectors/settings.py`
- `src/valmer_connectors/services/vector_update.py`
- `src/valmer_connectors/data_nodes/nodes.py`
- `src/valmer_connectors/cli/main.py`
- `tests/test_vector_update_service.py`
- `tests/test_valmer_vector_storage.py`
- `docs/source-import.md`
- `docs/pricing.md`

Existing controls:

| Control | Status | Purpose |
| --- | --- | --- |
| `VALMER_VECTOR_UPLOAD_DEBUG_PATH` | existing | local folder path consumed through `--local-bucket-path-env-var` |
| `VALMER_VECTOR_BUCKET_NAME` | existing | artifact bucket selection |
| `VALMER_VECTOR_FILE_BATCH_SIZE` | existing | local file batch size |
| `VALMER_PRICING_DETAILS_BATCH_SIZE` | existing | `add_many_pricing_details(...)` batch size |
| `VALMER_PER_PAGE` | existing | MetaTable/API operation page size |

New controls to add:

| Control | Type | Purpose |
| --- | --- | --- |
| `VALMER_FORCE_PRICING_DETAILS_PATCH` | env bool plus CLI flag | Force pricing detail rehydration for every selected target bond, even when current pricing details already exist |
| `VALMER_VECTOR_BYPASS_CURSOR_FILTER` | env bool plus CLI flag | Keep source rows even when `ValmerVectorPricesStorage` already has an equal or newer observation for that asset |

Why both are needed:

- `force_pricing_update=True` only decides what to do with rows that reach
  `_sync_asset_registry_and_pricing(...)`.
- `_filter_source_rows_from_last_vector_observation(...)` can remove rows
  before pricing hydration sees them.
- A full asset patch needs both controls.

Recommended CLI flags:

```text
valmer-connectors vector update \
  --force-pricing-details-patch \
  --bypass-vector-cursor-filter
```

The env vars should be equivalent so scheduled jobs and platform job configs can
run the same patch without changing command arguments.

Patch run example:

```bash
export VALMER_VECTOR_UPLOAD_DEBUG_PATH=/path/to/vector-folder
export VALMER_FORCE_PRICING_DETAILS_PATCH=1
export VALMER_VECTOR_BYPASS_CURSOR_FILTER=1
export VALMER_VECTOR_FILE_BATCH_SIZE=3
export VALMER_PRICING_DETAILS_BATCH_SIZE=1000
export VALMER_PER_PAGE=1000

valmer-connectors vector update \
  --local-bucket-path-env-var VALMER_VECTOR_UPLOAD_DEBUG_PATH
```

For bucket-sourced repair:

```bash
export VALMER_VECTOR_BUCKET_NAME="Hitorical Valmer Vector Analytico"
export VALMER_FORCE_PRICING_DETAILS_PATCH=1
export VALMER_VECTOR_BYPASS_CURSOR_FILTER=1
export VALMER_PRICING_DETAILS_BATCH_SIZE=1000
export VALMER_PER_PAGE=1000

valmer-connectors vector update
```

Acceptance checks:

- with both repair controls on, source rows are not dropped by the vector cursor
- `prepare_for_update(force_pricing_update=True)` is called
- `_get_pricing_refresh_uids(...)` logs forced refreshes for the selected target
  bond universe
- `_persist_valmer_pricing_details_batch(...)` writes timestamped rows through
  `add_many_pricing_details(...)`
- current pricing details are verified by readback after persist
- the TimeIndexTableUpdater still runs through `ImportValmer.run(force_update=True)`

### Phase 5: Backend Cleanup Of Old Static Rows

The existing backend may already contain:

```text
Index.unique_identifier = MXN_GOVERNMENT_BOND
IndexConventionDetails.index_family = MXN_GOVERNMENT_BOND
Curve VALMER_MXN_GOVERNMENT_BOND with old index ownership
```

Cleanup order:

1. Deploy the fixed bootstrap and bindings.
2. Run the asset pricing-details patch.
3. Query current pricing detail payloads for any remaining references to the
   old `MXN_GOVERNMENT_BOND` index UID.
4. Only after no instrument payload references it, remove or deactivate the old
   synthetic index/convention row using the supported `msm` row API.

Do not delete the old index first. If persisted instruments still reference it,
z-spread or benchmark-index hydration errors will become harder to diagnose.

### Phase 6: Curve Publication Validation

Commands:

```bash
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-mxn-government
```

Required validation:

- `Curve.get_by_unique_identifier("VALMER_TIIE_OVERNIGHT")` exists and has
  `curve_type="projection"`
- `Curve.get_by_unique_identifier("VALMER_USD_SOFR_OVERNIGHT")` exists and has
  `curve_type="projection"`
- `Curve.get_by_unique_identifier("VALMER_MXN_GOVERNMENT_BOND")` exists and
  has `curve_type="discount"`
- both curves have `CurveBuildingDetails`
- `DiscountCurvesNode` has latest observations for both curve identifiers
- TIIE projection binding resolves for `TIIE_28` and `TIIE_182` with
  `quote_side="mid"`
- CETE z-spread-base binding resolves for `CETE_28` and `CETE_182` with
  `quote_side="mid"`
- `MXN_GOVERNMENT_BOND` is absent from newly seeded index definitions

### Phase 7: Documentation Updates

Update these docs after the implementation:

- `docs/pricing.md`
- `docs/instruments.md`
- `docs/new-version-migration.md`
- `docs/adr/0004-mexican-government-bond-curve-bootstrap.md`

ADR 0004 currently says `MXN_GOVERNMENT_BOND` is an index required by the old
curve relationship model. That statement is now superseded. Do not silently
edit history as if it was always correct; add a supersession note that points to
the new curve-binding implementation.

## Test Plan

Minimum local tests after implementation:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_curve_bootstrap
PYTHONPATH=src .venv/bin/python -m unittest tests.test_valmer_instrument_index_uids
PYTHONPATH=src .venv/bin/python -m unittest tests.test_valmer_vector_storage
PYTHONPATH=src .venv/bin/python -m unittest tests.test_vector_update_service
PYTHONPATH=src .venv/bin/python -m unittest tests.test_curve_update_service
PYTHONPATH=src .venv/bin/python -m unittest tests.test_rates_curves
PYTHONPATH=src .venv/bin/python -m unittest tests.test_mxn_government_bond_curve
```

Specific assertions to add:

- reference index definitions exclude `MXN_GOVERNMENT_BOND`
- CETE indexes remain registered
- TIIE indexes remain registered
- `VALMER_TIIE_OVERNIGHT` curve payload has no `index_uid` and has
  `curve_type="projection"` and `quote_side="mid"`
- `VALMER_MXN_GOVERNMENT_BOND` curve payload has no `index_uid` and has
  `curve_type="discount"` and `quote_side="mid"`
- curve bootstrap seeds `CurveBuildingDetails` for both curves
- curve bootstrap seeds default market-data source bindings
- curve bootstrap seeds mid projection bindings for `TIIE_28` and `TIIE_182`
- curve bootstrap seeds mid z-spread-base bindings for `CETE_28` and `CETE_182`
- force-patch env vars reach the normal updater workflow
- bypass-cursor repair mode keeps otherwise-filtered source rows

## Live Verification Plan

Before live checks:

```bash
mainsequence code-repository current --debug
mainsequence code-repository refresh-token --path .
valmer-connectors runtime validate
```

Then verify static rows:

```text
Index rows:
  TIIE_OVERNIGHT, TIIE_28, TIIE_91, TIIE_182, CETE_28, CETE_91, CETE_182

No newly seeded Index row:
  MXN_GOVERNMENT_BOND

Curve rows:
  VALMER_TIIE_OVERNIGHT
  VALMER_MXN_GOVERNMENT_BOND

Build rows:
  one CurveBuildingDetails row for each curve

Market-data bindings:
  default + discount_curves -> DiscountCurvesStorage
  default + interest_rate_index_fixings -> IndexFixingsStorage

Curve bindings:
  projection:index:<TIIE_28.uid>:mid -> VALMER_TIIE_OVERNIGHT
  projection:index:<TIIE_182.uid>:mid -> VALMER_TIIE_OVERNIGHT
  discount:index:<TIIE_28.uid>:mid -> VALMER_TIIE_OVERNIGHT
  discount:index:<TIIE_182.uid>:mid -> VALMER_TIIE_OVERNIGHT
  projection:index:<CETE_28.uid>:mid -> VALMER_MXN_GOVERNMENT_BOND
  projection:index:<CETE_182.uid>:mid -> VALMER_MXN_GOVERNMENT_BOND
  discount:index:<CETE_28.uid>:mid -> VALMER_MXN_GOVERNMENT_BOND
  discount:index:<CETE_182.uid>:mid -> VALMER_MXN_GOVERNMENT_BOND
  z_spread_base:index:<CETE_28.uid>:mid -> VALMER_MXN_GOVERNMENT_BOND
  z_spread_base:index:<CETE_182.uid>:mid -> VALMER_MXN_GOVERNMENT_BOND
  no discount:currency:MXN:mid binding
  no discount:index:<USD_SOFR_OVERNIGHT.uid>:mid binding
```

Then run curve updates and asset patch:

```bash
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-mxn-government

export VALMER_FORCE_PRICING_DETAILS_PATCH=1
export VALMER_VECTOR_BYPASS_CURSOR_FILTER=1
valmer-connectors vector update
```

Success requires:

- both curve update commands complete
- the vector update completes
- pricing-detail logs show forced refreshes and current-row readback
- a sample M BONO current pricing detail has a CETE benchmark index UID, not a
  synthetic government-bond index UID
- sample CETE benchmark z-spread curve resolution selects
  `VALMER_MXN_GOVERNMENT_BOND` when `benchmark_curve_quote_side="mid"`
- sample TIIE floater projection resolution selects `VALMER_TIIE_OVERNIGHT` when
  `curve_quote_side="mid"`
- no market-data-set curve binding exists for `discount:currency:MXN:mid`
- no market-data-set curve binding exists for
  `discount:index:<USD_SOFR_OVERNIGHT.uid>:mid` unless a later policy explicitly
  adds USD SOFR discounting

## Non-Goals

This plan does not:

- create new updater output tables
- change Valmer vector storage row grain
- create a project-specific direct pricing-details patch script
- register `MXN_GOVERNMENT_BOND` as an index under another name
- move Valmer source parsing into `msm_pricing`
- expand the supported Valmer asset universe beyond the existing target-bond
  selection
