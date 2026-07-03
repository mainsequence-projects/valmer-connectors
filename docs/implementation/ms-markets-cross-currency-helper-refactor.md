# ms-markets Cross-Currency Helper Refactor

## Success Condition

`valmer-connectors` must stop owning generic QuantLib cross-currency helper
construction for `VALMER_MXN_USD_COLLATERAL_DISCOUNT`.

The finished refactor must leave Valmer source ownership here and move generic
curve reconstruction to `msm_pricing.pricing_engine.curves`:

- Valmer code still reads `IRS_MXN_CURVE.csv` and `IRS_USD_CURVE.csv`.
- Valmer code still selects `FX.USD.MXN`, USD/MXN FX swap rows, and
  `Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR` rows.
- Valmer code still owns source quote scaling, tenor normalization, source
  validation, and Valmer-specific provenance fields.
- ms-markets builds `ql.FxSwapRateHelper`,
  `ql.ConstNotionalCrossCurrencyBasisSwapRateHelper`, the `RateHelperVector`,
  and the `PiecewiseLogLinearDiscount` curve.
- The published storage row shape remains:

```text
time_index
curve_identifier
curve
key_nodes
```

## Upstream Verification

The upstream planning document exists at:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/docs/planning/implementation_tasks/cross_currency_helper_curve_reconstruction_upstream_plan.md
```

The source implementation is present in the sibling ms-markets checkout:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/src/msm_pricing/pricing_engine/curves/cross_currency_helpers.py
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/src/msm_pricing/pricing_engine/curves/cross_currency_key_nodes.py
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/src/msm_pricing/pricing_engine/curves/helper_resolution.py
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/src/msm_pricing/pricing_engine/curves/helper_key_nodes.py
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/src/msm_pricing/pricing_engine/curves/reconstruction.py
```

The ms-markets test coverage is present at:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b/tests/msm_pricing/pricing_engine/curves/test_cross_currency_helpers.py
```

That test file proves the supported path:

- `FxSwapRateHelperSpec` builds `ql.FxSwapRateHelper`.
- `ConstNotionalCrossCurrencyBasisSwapRateHelperSpec` builds
  `ql.ConstNotionalCrossCurrencyBasisSwapRateHelper`.
- `helper_specs_from_key_nodes(...)` accepts `fx_spot`,
  `fx_swap_rate_helper`, and
  `const_notional_cross_currency_basis_swap_rate_helper` nodes under
  `rate_helpers@v1`.
- cross-currency helper key nodes require `helper_runtime_resolver`.
- `reconstruct_curve_result_from_key_nodes(...)` bootstraps a curve and returns
  helper quote errors.
- `build_curve_from_curve_observation(...)` supports `rate_helpers@v1` context
  nodes.
- `msm_pricing.scenarios.curves.build_scenario_curve_handle(...)` forwards
  `helper_runtime_resolver`.
- connector-owned `valmer_xccy_helpers@v1` is rejected upstream.

The public import surface is available from `msm_pricing.pricing_engine.curves`
in the sibling source checkout:

```python
from msm_pricing.pricing_engine.curves import (
    ConstNotionalCrossCurrencyBasisSwapRateHelperSpec,
    FxSwapRateHelperSpec,
    MissingRateHelperDependencyError,
    StaticRateHelperRuntimeResolver,
    helper_specs_from_key_nodes,
    parse_cross_currency_key_node,
    reconstruct_curve_result_from_key_nodes,
)
```

The current `valmer-connectors` virtualenv does not yet expose those symbols
from its installed `ms-markets` package. The implementation must refresh the
path dependency before changing imports:

```bash
uv sync --reinstall-package ms-markets
```

Then verify:

```bash
.venv/bin/python -c "from msm_pricing.pricing_engine.curves import FxSwapRateHelperSpec, ConstNotionalCrossCurrencyBasisSwapRateHelperSpec, StaticRateHelperRuntimeResolver, reconstruct_curve_result_from_key_nodes; print(FxSwapRateHelperSpec.__name__, ConstNotionalCrossCurrencyBasisSwapRateHelperSpec.__name__, StaticRateHelperRuntimeResolver.__name__, reconstruct_curve_result_from_key_nodes.__name__)"
```

## Current Local Duplication

The local duplication is in:

```text
src/valmer_connectors/instruments/rates_curves.py
```

The function `_build_usd_mxn_xccy_curve_and_key_nodes(...)` currently owns
generic reconstruction that belongs to ms-markets:

- creates `ql.YieldTermStructureHandle` instances for dependency curves;
- creates `ql.JointCalendar`;
- creates `ql.Sofr`;
- creates a curve-attached FTIIE `ql.OvernightIndex`;
- creates `ql.QuoteHandle` and `ql.SimpleQuote` objects;
- creates `ql.FxSwapRateHelper`;
- creates `ql.ConstNotionalCrossCurrencyBasisSwapRateHelper`;
- calls `ql.PiecewiseLogLinearDiscount`;
- reads helper quote errors directly from local QuantLib helper instances.

The current key-node shape is already close to the ms-markets generic schema:

- `_build_usd_mxn_fx_spot_key_node(...)` emits `helper_type = "fx_spot"`.
- `_build_usd_mxn_fx_swap_key_node(...)` emits
  `helper_type = "fx_swap_rate_helper"`.
- `_build_usd_mxn_xccy_basis_key_node(...)` emits
  `helper_type = "const_notional_cross_currency_basis_swap_rate_helper"`.

The remaining local mismatch is metadata and reconstruction ownership:

- `src/valmer_connectors/instruments/curve_bootstrap.py` still declares
  `helper_schema = "valmer_xccy_helpers@v1"`.
- `src/valmer_connectors/instruments/curve_bootstrap.py` still declares
  `VALMER_MXN_USD_COLLATERAL_DISCOUNT` as `builder_type = "zero_rate_curve"`,
  `quote_convention = "zero_rate"`, and `rate_unit = "decimal"`.
- `src/valmer_connectors/instruments/curve_key_nodes.py` validates Valmer
  fields but does not first validate USD/MXN cross-currency nodes with
  `parse_cross_currency_key_node(...)`.
- `docs/pricing.md` says ms-markets does not yet support FX swap or
  cross-currency basis swap helper specs.
- `docs/adr/0008-usd-mxn-cross-currency-discount-curve.md` records the old
  local-only decision and the connector-owned `valmer_xccy_helpers@v1` schema.

## Files To Update

Update:

```text
src/valmer_connectors/instruments/rates_curves.py
src/valmer_connectors/instruments/curve_bootstrap.py
src/valmer_connectors/instruments/curve_key_nodes.py
tests/test_rates_curves.py
tests/test_curve_bootstrap.py
docs/pricing.md
docs/adr/0008-usd-mxn-cross-currency-discount-curve.md
docs/SUMMARY.md
```

Do not create a new core Valmer reconstruction module for cross-currency
helpers. The generic reconstruction module already exists upstream in
ms-markets.

Do not remove these Valmer source functions:

```text
read_tiie_irs_mxn_csv
read_usd_sofr_irs_csv
_select_usd_mxn_xccy_quotes
_normalize_usd_mxn_xccy_tenor
_build_usd_mxn_fx_spot_key_node
_build_usd_mxn_fx_swap_key_node
_build_usd_mxn_xccy_basis_key_node
_build_tiie_projection_curve_from_source
_build_usd_sofr_projection_curve_from_source
```

Remove these generic implementation responsibilities from
`_build_usd_mxn_xccy_curve_and_key_nodes(...)`:

```text
ql.FxSwapRateHelper
ql.ConstNotionalCrossCurrencyBasisSwapRateHelper
ql.PiecewiseLogLinearDiscount
manual helper list construction
manual QuantLib helper-vector bootstrapping
```

After this refactor, the local `_ql_period(...)` helper has no remaining caller
and must be removed from `rates_curves.py`.

## Refactor Design

### 1. Import ms-markets Cross-Currency Reconstruction

In `src/valmer_connectors/instruments/rates_curves.py`, import:

```python
from msm_pricing.pricing_engine.curves import (
    CurveObservationExportConfig,
    StaticRateHelperRuntimeResolver,
    build_rate_helpers,
    export_curve_observation_nodes,
    helper_specs_from_key_nodes,
    reconstruct_curve_result_from_key_nodes,
    reconstruct_curve_term_structure_from_key_nodes,
)
```

Keep `build_rate_helpers` and `helper_specs_from_key_nodes` for the existing
TIIE and USD SOFR helper-date enrichment path. Use
`reconstruct_curve_result_from_key_nodes(...)` for the USD/MXN cross-currency
curve because the result returns both the bootstrapped term structure and helper
quote errors.

### 2. Build a Runtime Resolver For USD/MXN XCCY

Add a local helper in `rates_curves.py`:

```text
_build_usd_mxn_xccy_runtime_resolver(...)
```

This helper must return `StaticRateHelperRuntimeResolver` with:

- `yield_curves[VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION.unique_identifier]`
  mapped to `ql.YieldTermStructureHandle(usd_sofr_curve)`;
- `indexes[USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER]` mapped to
  `ql.Sofr(ql.YieldTermStructureHandle(usd_sofr_curve))`;
- `indexes[TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER]` mapped to the same FTIIE
  overnight index semantics as the current local implementation:

```text
name = "FTIIE"
fixing_days = 0
currency = MXN
calendar = Mexico
day_counter = Actual360
forecast_curve = ql.YieldTermStructureHandle(tiie_projection_curve)
```

Also populate `overnight_indexes` with the same two index objects. This keeps
the resolver usable if future mixed helper nodes include OIS nodes.

Do not use the existing `resolve_valmer_overnight_index(...)` alone for the
cross-currency basis helpers. That resolver returns generic overnight indexes;
the cross-currency basis helper requires curve-attached SOFR and FTIIE indexes
matching the current local implementation.

### 3. Rebuild The USD/MXN XCCY Function Around Key Nodes

Rewrite `_build_usd_mxn_xccy_curve_and_key_nodes(...)` so the body does this:

1. Build `helper_key_nodes` from `fx_swap_quotes` and `basis_quotes`.
2. Build the `fx_spot` context node.
3. Concatenate `key_nodes = [spot_node, *helper_key_nodes]`.
4. Build the `StaticRateHelperRuntimeResolver`.
5. Call `reconstruct_curve_result_from_key_nodes(...)` with:

```text
key_nodes = key_nodes
valuation_date = _ql_date(valuation_ts)
day_counter = "Actual360"
bootstrap_method = "piecewise_log_linear_discount"
extrapolation = True
helper_schema = "rate_helpers@v1"
helper_runtime_resolver = runtime_resolver
```

6. Zip `helper_key_nodes` with `result.helpers` to populate:

```text
maturity_date
earliest_date
pillar_date
```

7. Zip `helper_key_nodes` with `result.helper_quote_errors` to populate:

```text
quote_error
```

8. Return `result.term_structure` and the full `key_nodes` list.

The function must keep the existing error wrapper:

```text
ValmerUsdMxnXccyCurveError("Unable to build Valmer USD/MXN cross-currency curve helpers.")
```

The function must keep temporary QuantLib evaluation-date handling around the
ms-markets reconstruction call so helper dates and quote errors are evaluated
against `valuation_ts`.

### 4. Keep Existing Valmer Quote Semantics

Do not change the economic interpretation of existing key nodes:

- FX spot remains USD/MXN spot.
- FX swap `quote` remains scaled forward points:

```text
quote = source_quote / 10000
source_quote_unit = "raw_points"
point_scale = 10000
```

- CCS `quote` remains the decimal spread:

```text
quote = source_quote / 100
source_quote_unit = "percent"
quote_unit = "decimal"
```

- CCS basis remains on the USD SOFR leg:

```text
basis_side = "USD_SOFR"
basis_sign = "positive_quote_means_sofr_plus_spread"
is_basis_on_fx_base_currency_leg = True
```

The current `quote_unit = "mxn_per_usd"` on FX spot and FX swap nodes is an
explicit direct pair unit and is accepted by ms-markets forward-points
normalization as a provider-normalized pair quote. Do not convert the already
scaled `quote` back to raw points.

### 5. Change Curve Building Details To Canonical Helper Curve

Update
`src/valmer_connectors/instruments/curve_bootstrap.py` for
`VALMER_MXN_USD_COLLATERAL_DISCOUNT`:

```text
builder_type = "rate_helper_curve"
quote_convention = "helper_quote"
rate_unit = "helper_unit"
builder_payload["helper_schema"] = "rate_helpers@v1"
```

Keep these existing output fields in `builder_payload`:

```text
output_quote_convention = "zero_rate"
output_rate_unit = "decimal"
output_quote_type = "zero_rate"
output_quote_unit = "decimal"
```

Keep these dependency fields:

```text
dependency_curves = [
    "VALMER_TIIE_OVERNIGHT",
    "VALMER_USD_SOFR_OVERNIGHT",
]
```

Keep all Valmer instrument rules, including:

```text
excluded_fx_tenors
tenor_normalization
FX_SWAP.point_scale
CONSTANT_NOTIONAL_CCS.basis_side
CONSTANT_NOTIONAL_CCS.basis_sign
CONSTANT_NOTIONAL_CCS.notional_style
CONSTANT_NOTIONAL_CCS.is_basis_on_fx_base_currency_leg
```

### 6. Validate Canonical Schema Before Valmer Extensions

Update
`src/valmer_connectors/instruments/curve_key_nodes.py`:

```python
from msm_pricing.pricing_engine.curves import (
    parse_bond_helper_key_node,
    parse_cross_currency_key_node,
)
```

In `validate_usd_mxn_xccy_key_nodes(...)`, call
`parse_cross_currency_key_node(node)` for each node before the Valmer-specific
checks. This makes canonical generic schema validation run first while keeping
the local checks for:

- source family;
- `asset_identifier`;
- `quote_source`;
- `quote_side`;
- USD/MXN FX identity;
- scaled source quote fields;
- basis side and sign;
- Valmer calendar;
- tenor normalization;
- helper date fields;
- quote error fields.

Do not remove Valmer-specific validation. ms-markets validates generic helper
shape; this package validates that Valmer emitted the expected USD/MXN source
contract.

## Tests To Update

Update `tests/test_curve_bootstrap.py`:

- assert `VALMER_MXN_USD_COLLATERAL_DISCOUNT` has
  `builder_type = "rate_helper_curve"`;
- assert `quote_convention = "helper_quote"`;
- assert `rate_unit = "helper_unit"`;
- assert `builder_payload["helper_schema"] == "rate_helpers@v1"`;
- keep existing assertions for `tenor_normalization`, `point_scale`,
  `basis_side`, and `is_basis_on_fx_base_currency_leg`.

Update `tests/test_rates_curves.py`:

- keep the fixture bootstrap test for 17 key nodes;
- keep assertions for 7 FX swap helpers and 9 CCS helpers;
- keep assertions for scaled FX points and scaled CCS basis quotes;
- keep assertions for tenor normalization from `182M` to `15Y` and `364M` to
  `30Y`;
- keep the quote-error tolerance assertion;
- add assertions that each USD/MXN xccy key node parses through
  `parse_cross_currency_key_node(...)`;
- add an assertion that
  `helper_specs_from_key_nodes(row["key_nodes"], helper_runtime_resolver=...)`
  returns 16 helper specs when supplied with the same runtime resolver shape
  used by the builder.

Add one targeted regression test in `tests/test_rates_curves.py`:

```text
test_usd_mxn_xccy_builder_uses_ms_markets_reconstruction
```

The test must monkeypatch
`valmer_connectors.instruments.rates_curves.reconstruct_curve_result_from_key_nodes`
and assert it is called with:

```text
helper_schema = "rate_helpers@v1"
helper_runtime_resolver = StaticRateHelperRuntimeResolver
```

Do not assert against private ms-markets helper implementation details.

## Documentation Updates

Update `docs/pricing.md`:

- remove the statement that ms-markets does not support FX swap or
  cross-currency basis swap helper specs;
- state that `VALMER_MXN_USD_COLLATERAL_DISCOUNT` is now a
  `rate_helper_curve` using ms-markets `rate_helpers@v1` cross-currency helper
  nodes;
- state that Valmer still owns row selection, quote scaling, and provenance.

Update `docs/adr/0008-usd-mxn-cross-currency-discount-curve.md`:

- mark the original local-only helper schema decision as superseded;
- replace `valmer_xccy_helpers@v1` with `rate_helpers@v1`;
- keep the business contract for USD/MXN spot, FX swaps, CCS basis side, CCS
  basis sign, source quote scaling, and dependency curves.

Update `docs/SUMMARY.md` to include this implementation plan.

## Validation Commands

After the implementation, run:

```bash
uv sync --reinstall-package ms-markets
.venv/bin/python -c "from msm_pricing.pricing_engine.curves import FxSwapRateHelperSpec, ConstNotionalCrossCurrencyBasisSwapRateHelperSpec, StaticRateHelperRuntimeResolver, reconstruct_curve_result_from_key_nodes; print('ok')"
.venv/bin/python -m ruff check src/valmer_connectors/instruments/rates_curves.py src/valmer_connectors/instruments/curve_key_nodes.py src/valmer_connectors/instruments/curve_bootstrap.py tests/test_rates_curves.py tests/test_curve_bootstrap.py
.venv/bin/python -m unittest tests.test_rates_curves tests.test_curve_bootstrap
git diff --check
```

If the ms-markets path dependency is still stale after `uv sync`, reinstall the
path package directly:

```bash
uv pip install --reinstall ../mainsequencemarkets-21f6783c-041a-4631-80ef-934e1dfa3d2b
```

Then rerun the import check and tests.
