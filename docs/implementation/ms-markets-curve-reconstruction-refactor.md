# ms-markets Curve Reconstruction Refactor

## Implementation Status

Implemented in `valmer-connectors`.

Created:

- `src/valmer_connectors/instruments/curve_reconstruction.py`
- `tests/test_valmer_curve_reconstruction.py`

Refactored:

- `src/valmer_connectors/instruments/rates_curves.py`
- `src/valmer_connectors/instruments/curve_bootstrap.py`
- `src/valmer_connectors/instruments/curve_key_nodes.py`
- `tests/test_rates_curves.py`
- `tests/test_curve_bootstrap.py`
- `docs/pricing.md`

Validated with:

```bash
.venv/bin/python -m ruff check src/valmer_connectors/instruments/rates_curves.py src/valmer_connectors/instruments/curve_reconstruction.py src/valmer_connectors/instruments/curve_key_nodes.py src/valmer_connectors/instruments/curve_bootstrap.py tests/test_rates_curves.py tests/test_curve_bootstrap.py tests/test_valmer_curve_reconstruction.py
.venv/bin/python -m unittest tests.test_rates_curves tests.test_curve_bootstrap tests.test_valmer_curve_reconstruction
```

## Goal

Refactor the Valmer TIIE and USD SOFR curve builders so
`valmer-connectors` stops owning generic QuantLib rate-helper construction.

After the refactor:

- `valmer-connectors` owns Valmer source ingestion, Valmer row selection,
  Valmer-specific key-node provenance, and Valmer overnight index resolution.
- `ms-markets` owns generic helper-key-node validation, quote normalization,
  QuantLib rate-helper construction, rate-helper-vector construction,
  piecewise log-linear discount bootstrapping, curve observation export, and
  scenario curve reconstruction.
- downstream projects such as `mexicofundcompetition` call
  `msm_pricing.scenarios.curves` APIs directly and pass the Valmer
  overnight-index resolver when the shocked curve is helper based.

Success means the Valmer curve DataNode still publishes the same storage shape:

```text
time_index
curve_identifier
curve
key_nodes
```

but the generic curve build is imported from `msm_pricing` instead of being
implemented in `valmer_connectors.instruments.rates_curves`.

## Verified Upstream Surface

The current `ms-markets` checkout exposes the required public imports from:

```text
msm_pricing.pricing_engine.curves
msm_pricing.scenarios.curves
msm_pricing.pricing_engine.resolvers
```

Use these imports in `valmer-connectors`:

```python
from msm_pricing.pricing_engine.curves import (
    CurveObservationExportConfig,
    build_rate_helpers,
    export_curve_observation_nodes,
    helper_specs_from_key_nodes,
    reconstruct_curve_handle_from_key_nodes,
    reconstruct_curve_term_structure_from_key_nodes,
    parse_bond_helper_key_node,
)
```

Use these imports in downstream scenario code:

```python
from msm_pricing.scenarios.curves import (
    CurveBumpSpec,
    CurveScenario,
    build_scenario_curve_handle,
    bump_key_nodes,
    price_curve_scenario,
)
```

`msm_pricing.pricing_engine.curves` now owns:

- `OISRateHelperSpec` with extended OIS fields:
  `settlement_days`, `payment_convention`, `payment_frequency`,
  `payment_calendar_code`, `fixed_payment_frequency`,
  `fixed_calendar_code`, `averaging_method`, `end_of_month`,
  observation-shift fields, and date-generation fields.
- `InterestRateFutureHelperSpec` and the
  `sofr_future_rate_helper` key-node schema.
- price quote normalization through `key_node_price(...)`.
- rate quote normalization through `key_node_decimal_rate(...)`.
- `helper_specs_from_key_nodes(...)` with explicit
  `overnight_index` or `overnight_index_resolver`.
- `reconstruct_curve_handle_from_key_nodes(...)`.
- `reconstruct_curve_term_structure_from_key_nodes(...)`.
- `export_curve_observation_nodes(...)`.

`msm_pricing.scenarios.curves.build_scenario_curve_handle(...)` accepts
`overnight_index` and `overnight_index_resolver`, and routes helper-based
`CurveBuildingDetails` rows through the common curve observation resolver.

`msm_pricing.scenarios.curves.price_curve_scenario(...)` also accepts
`overnight_index` and `overnight_index_resolver`. Its high-level scenario loop
forwards both arguments through
`_build_scenario_handles_by_identifier(...)` into
`build_scenario_curve_handle(...)`, so the high-level pricing scenario path no
longer drops the Valmer resolver.

## Generic Coverage Review

`ms-markets` covers every generic requirement for the TIIE and USD SOFR
rate-helper refactor. `valmer-connectors` must not keep fallback copies of
these behaviors.

| Generic requirement | ms-markets owner |
| --- | --- |
| Helper key-node schema for OIS swaps | `msm_pricing.pricing_engine.curves.OISRateHelperKeyNode` |
| Helper key-node schema for SOFR futures | `msm_pricing.pricing_engine.curves.InterestRateFutureHelperKeyNode` |
| Overnight deposit helper schema | `msm_pricing.pricing_engine.curves.OvernightDepositHelperKeyNode` |
| Percent, decimal, and futures-price quote normalization | `key_node_decimal_rate(...)`, `key_node_price(...)` |
| Tenor parsing into QuantLib periods | `ql_period_from_tenor(...)` |
| Conversion from key nodes to helper specs | `helper_specs_from_key_nodes(...)` |
| QuantLib `OISRateHelper` construction | `build_ois_rate_helper(...)` |
| QuantLib `SofrFutureRateHelper` construction | `build_interest_rate_future_helper(...)` |
| QuantLib overnight deposit helper construction | `build_overnight_deposit_helper(...)` |
| Rate-helper vector construction | `build_rate_helper_vector(...)` |
| Piecewise log-linear discount bootstrapping | `reconstruct_curve_handle(...)`, `reconstruct_curve_term_structure(...)` |
| Rebuild from helper specs | `reconstruct_curve_handle_from_helper_specs(...)`, `reconstruct_curve_term_structure_from_helper_specs(...)` |
| Rebuild from helper key nodes | `reconstruct_curve_handle_from_key_nodes(...)`, `reconstruct_curve_term_structure_from_key_nodes(...)` |
| Curve observation export | `export_curve_observation_nodes(...)` |
| Persisted `CurveBuildingDetails` adapter | `reconstruct_curve_from_curve_building_details(...)` |
| Helper-curve build-detail detection | `is_rate_helper_curve_build(...)` |
| Direct shocked helper-curve reconstruction | `msm_pricing.scenarios.curves.build_scenario_curve_handle(...)` |
| High-level shocked scenario pricing with resolver propagation | `msm_pricing.scenarios.curves.price_curve_scenario(...)` |

There is no remaining generic TIIE/USD SOFR rate-helper gap to patch in
`valmer-connectors`. If implementation exposes a missing generic need, the fix
belongs upstream in `ms-markets`; the Valmer package should only adapt Valmer
source rows and resolver policy into the generic ms-markets contracts.

## Generic Parts To Remove From `valmer-connectors`

The following generic machinery currently embedded in
`src/valmer_connectors/instruments/rates_curves.py` moves to direct imports
from `msm_pricing`:

- tenor string to QuantLib `Period` conversion;
- key-node quote unit normalization;
- `RateHelperVector` construction;
- QuantLib `OISRateHelper` construction;
- QuantLib `SofrFutureRateHelper` construction;
- overnight deposit helper construction;
- piecewise log-linear discount curve bootstrap;
- zero-rate curve observation export;
- rebuild-from-key-nodes API used by downstream scenario code.

Delete these local functions during the implementation:

```text
build_tiie_discount_curve_from_key_nodes
_build_tiie_ois_helpers
_build_tiie_ois_helpers_from_key_nodes
_build_rate_helper_vector
_build_usd_sofr_helpers
_build_sofr_future_helper
_build_sofr_ois_helper
_build_usd_sofr_rate_helper_vector
_build_overnight_deposit_helper
_bootstrap_tiie_discount_curve
_bootstrap_usd_sofr_discount_curve
_export_tiie_zero_rate_points
_export_usd_sofr_zero_rate_points
_ql_period_from_tenor
_key_node_decimal_rate
_ql_month_from_token
```

Delete these local wrapper dataclasses because they only exist to carry local
QuantLib helper objects:

```text
ValmerTiieOisHelper
ValmerUsdSofrHelper
```

Keep these source quote dataclasses because they represent Valmer feed parsing,
not generic curve construction:

```text
ValmerIrsMxnQuote
ValmerUsdSofrFutureQuote
ValmerUsdSofrOisQuote
```

## Valmer-Specific Parts That Remain Local

Keep the following behavior in `valmer-connectors`:

- Valmer source URLs and source file names;
- CSV parsing for `IRS_MXN_CURVE.csv` and `IRS_USD_CURVE.csv`;
- Valmer benchmark-date parsing;
- row-family classification:
  `domestic_ois`, `cross_currency`, `sofr_future`, `sofr_ois`,
  `fedfunds_ois`, and `fedfunds_sofr_basis`;
- selection of domestic FTIIE OIS rows;
- selection of USD SOFR futures and USD SOFR OIS rows;
- exclusion of Fed Funds and cross-currency rows;
- Valmer source metadata in `key_nodes`;
- Valmer curve and index identifiers;
- Valmer active-future policy for SOFR futures;
- QuantLib overnight-index resolver for Valmer identifiers.

The MXN government bond curve is outside this TIIE/USD SOFR rate-helper
refactor. If reusable bond-helper curve reconstruction is extracted later, the
generic construction surface must also live in `ms-markets`; Valmer should only
adapt source bond rows into that generic contract.

## Files Created By This Refactor

Create this source file:

```text
src/valmer_connectors/instruments/curve_reconstruction.py
```

Responsibilities:

- expose `resolve_valmer_overnight_index(floating_index, node)`;
- return a QuantLib FTIIE overnight index for
  `TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER`;
- return `ql.Sofr()` for `USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER`;
- raise a clear `ValueError` for unknown Valmer floating indexes;
- keep Mexico-calendar and FTIIE-index construction here if those helpers are
  still needed by the resolver.

Create this test file:

```text
tests/test_valmer_curve_reconstruction.py
```

Responsibilities:

- verify `resolve_valmer_overnight_index(...)` maps TIIE and USD SOFR
  identifiers to QuantLib overnight indexes;
- verify unknown identifiers fail;
- verify Valmer-produced TIIE key nodes can be passed to
  `reconstruct_curve_handle_from_key_nodes(...)` with
  `resolve_valmer_overnight_index`;
- verify Valmer-produced USD SOFR key nodes can be passed to
  `reconstruct_curve_handle_from_key_nodes(...)` with
  `resolve_valmer_overnight_index` when required SOFR fixings are available or
  the active future set avoids missing-fixing dates.

Create this documentation file:

```text
docs/implementation/ms-markets-curve-reconstruction-refactor.md
```

## Files Modified In `valmer-connectors`

Modify:

```text
src/valmer_connectors/instruments/rates_curves.py
```

Required changes:

- import generic curve helper functions from
  `msm_pricing.pricing_engine.curves`;
- import `resolve_valmer_overnight_index` from
  `valmer_connectors.instruments.curve_reconstruction`;
- remove the local helper wrapper dataclasses;
- keep the Valmer source quote dataclasses;
- rewrite `_build_tiie_key_nodes(...)` so it emits generic
  `ois_rate_helper` key nodes directly from `ValmerIrsMxnQuote`;
- rewrite `_build_usd_sofr_key_nodes(...)` so it emits generic
  `sofr_future_rate_helper` and `ois_rate_helper` key nodes directly from the
  parsed source quotes;
- call `helper_specs_from_key_nodes(...)` and `build_rate_helpers(...)` from
  `msm_pricing` when the builder needs helper dates for key-node provenance;
- call `reconstruct_curve_term_structure_from_key_nodes(...)` to build the
  QuantLib term structure directly from the generic key-node payloads;
- call `export_curve_observation_nodes(...)` to produce output zero-rate
  points;
- convert exported observation nodes into the existing `curve` mapping:
  `{days_to_maturity: zero}`;
- remove `build_tiie_discount_curve_from_key_nodes(...)` from this module.

The TIIE builder should use this output export config:

```python
CurveObservationExportConfig(
    quote_convention="zero_rate",
    rate_unit="decimal",
    day_counter_code="Actual360",
    compounding="compounded",
    compounding_frequency="annual",
)
```

The USD SOFR builder should use the same output export config.

Modify:

```text
src/valmer_connectors/instruments/curve_bootstrap.py
```

Required changes for the TIIE and USD SOFR curve build-detail definitions:

- change `builder_type` to `rate_helper_curve`;
- change `quote_convention` to `helper_quote`;
- change `rate_unit` to `helper_unit`;
- change `bootstrap_method` to `piecewise_log_linear_discount`;
- set `builder_payload["helper_schema"] = "rate_helpers@v1"`;
- set `builder_payload["output_quote_convention"] = "zero_rate"`;
- set `builder_payload["output_rate_unit"] = "decimal"`;
- keep `builder_payload["implied_front_zero_days"] = [1]`;
- keep Valmer source metadata such as `source_file`, source row patterns, and
  excluded source row patterns.

MXN government bond helpers were intentionally left out of this earlier
rate-helper refactor. After ms-markets added generic bond-helper reconstruction,
the Valmer MXN government curve definition should also use
`builder_type = "rate_helper_curve"` with
`builder_payload["helper_schema"] = "rate_helpers@v1"` and source-specific
CETES/M Bonos adaptation kept local.

Modify:

```text
src/valmer_connectors/instruments/curve_key_nodes.py
```

Required changes:

- validate the new generic OIS fields emitted by TIIE and USD SOFR OIS nodes:
  `settlement_days`, `payment_convention`, `payment_frequency`,
  `fixed_payment_frequency`, `payment_calendar_code`, `fixed_calendar_code`,
  `end_of_month`, and `averaging_method`;
- validate the new generic SOFR future fields:
  `future_family` and `convexity_adjustment`;
- keep source-family validation for domestic FTIIE, USD SOFR, and Fed Funds
  exclusion;
- keep `maturity_date`, `earliest_date`, and `pillar_date` validation after
  those dates are populated from `msm_pricing` helper construction.

Modify:

```text
tests/test_rates_curves.py
```

Required changes:

- remove the import of `build_tiie_discount_curve_from_key_nodes`;
- remove `test_build_tiie_discount_curve_from_key_nodes_rebuilds_source_curve`;
- add assertions that TIIE OIS key nodes include the generic OIS fields listed
  above;
- add assertions that USD SOFR OIS key nodes include the generic OIS fields
  listed above;
- add assertions that USD SOFR future key nodes include `future_family="sofr"`
  and `convexity_adjustment=0.0`;
- keep shape tests for the published DataNode frame.

Modify:

```text
tests/test_curve_bootstrap.py
```

Required changes:

- update expected TIIE builder type from `ois_swap_helper_bootstrap` to
  `rate_helper_curve`;
- update expected USD SOFR builder type from
  `sofr_futures_ois_helper_bootstrap` to `rate_helper_curve`;
- assert `builder_payload["helper_schema"] == "rate_helpers@v1"`;
- assert `quote_convention == "helper_quote"`;
- assert `rate_unit == "helper_unit"`;
- assert `bootstrap_method == "piecewise_log_linear_discount"`;
- assert `builder_payload["output_quote_convention"] == "zero_rate"`;
- assert `builder_payload["output_rate_unit"] == "decimal"`.

Modify:

```text
docs/pricing.md
docs/data-nodes.md
docs/SUMMARY.md
docs/index.md
mkdocs.yml
```

Required changes:

- document that Valmer TIIE and USD SOFR helper-based curves are reconstructed
  through `msm_pricing.pricing_engine.curves`;
- document that the Valmer package provides the overnight-index resolver;
- add this implementation plan to the docs summary, MkDocs navigation, and
  documentation index.

## Key-Node Contract After The Refactor

### TIIE OIS Key Nodes

Each TIIE node emitted from `IRS_MXN_CURVE.csv` must include:

```text
asset_identifier
instrument_type = overnight_indexed_swap
helper_type = ois_rate_helper
quote
quote_type = par_swap_rate
quote_unit = decimal
quote_side = mid
quote_source = IRS_MXN_CURVE.csv
source_quote
source_quote_unit = percent
tenor
floating_index = TIIE_OVERNIGHT
settlement_days = 1
payment_convention = ModifiedFollowing
payment_frequency = EveryFourthWeek
fixed_payment_frequency = EveryFourthWeek
payment_calendar_code = {"name": "Mexico", "market": 0}
fixed_calendar_code = {"name": "Mexico", "market": 0}
end_of_month = False
averaging_method = Compound
day_counter = Actual360
maturity_date
earliest_date
pillar_date
```

`settlement_days`, calendars, payment convention, payment frequencies, and
averaging method are required because they encode the old local QuantLib
constructor behavior as portable key-node data.

### USD SOFR Future Key Nodes

Each SOFR future node emitted from `IRS_USD_CURVE.csv` must include:

```text
asset_identifier
instrument_type = sofr_future
helper_type = sofr_future_rate_helper
quote
quote_type = futures_price
quote_unit = price
quote_side = mid
quote_source = IRS_USD_CURVE.csv
implied_rate
implied_rate_unit = decimal
contract_code
reference_month
reference_year
reference_frequency
future_family = sofr
convexity_adjustment = 0.0
maturity_date
earliest_date
pillar_date
```

The quote remains the futures price. Do not convert it to an implied rate before
passing it to `msm_pricing`; `key_node_price(...)` owns that quote convention.

### USD SOFR OIS Key Nodes

Each USD SOFR OIS node emitted from `IRS_USD_CURVE.csv` must include:

```text
asset_identifier
instrument_type = overnight_indexed_swap
helper_type = ois_rate_helper
quote
quote_type = par_swap_rate
quote_unit = decimal
quote_side = mid
quote_source = IRS_USD_CURVE.csv
source_quote
source_quote_unit = percent
tenor
floating_index = USD_SOFR_OVERNIGHT
settlement_days = 2
payment_convention = ModifiedFollowing
payment_frequency = Annual
fixed_payment_frequency = Annual
payment_calendar_code = {"name": "UnitedStates", "market": 6}
fixed_calendar_code = {"name": "UnitedStates", "market": 6}
end_of_month = False
averaging_method = Compound
day_counter = Actual360
maturity_date
earliest_date
pillar_date
```

Use `{"name": "UnitedStates", "market": 6}` for SOFR calendars because
QuantLib encodes the SOFR United States market as `6`.

## Builder Flow After The Refactor

The TIIE and USD SOFR builders follow the same local flow:

1. Parse the Valmer source CSV.
2. Select supported Valmer source rows.
3. Convert selected Valmer source rows into generic helper-shaped key-node
   dictionaries.
4. Resolve overnight indexes with
   `resolve_valmer_overnight_index(floating_index, node)`.
5. Convert key nodes to helper specs with
   `helper_specs_from_key_nodes(...)` only when helper dates are needed.
6. Build QuantLib helpers with `build_rate_helpers(...)` only when helper dates
   are needed for key-node provenance.
7. Enrich key nodes with `earliest_date`, `maturity_date`, and `pillar_date`
   from the helpers built by `msm_pricing`.
8. Reconstruct the curve term structure with
   `reconstruct_curve_term_structure_from_key_nodes(...)`.
9. Export curve observation nodes with `export_curve_observation_nodes(...)`.
10. Convert exported nodes to the existing `curve` mapping and return the
    DataFrame indexed by `time_index` and `curve_identifier`.

The flow must not call local QuantLib `OISRateHelper`,
`SofrFutureRateHelper`, `RateHelperVector`, or
`PiecewiseLogLinearDiscount` constructors.

## Downstream `mexicofundcompetition` Refactor

After `valmer-connectors` emits generic helper-shaped key nodes and exposes the
Valmer resolver, update:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/src/fundcompetition/local_valmer/curve_scenarios.py
```

Required changes:

- remove the import of
  `valmer_connectors.instruments.rates_curves.build_tiie_discount_curve_from_key_nodes`;
- remove the TIIE-only branch that detects Valmer TIIE key nodes and rebuilds
  them through the Valmer package;
- use `price_curve_scenario(...)` directly from
  `msm_pricing.scenarios.curves` for full scenario pricing;
- use `build_scenario_curve_handle(...)` directly from
  `msm_pricing.scenarios.curves` only for focused handle-building tests or
  lower-level custom workflows;
- import `resolve_valmer_overnight_index(...)` from
  `valmer_connectors.instruments.curve_reconstruction`;
- pass `overnight_index_resolver=resolve_valmer_overnight_index` whenever a
  helper-based Valmer curve is shocked;
- do not add another local TIIE or SOFR curve reconstruction branch;
- keep project-specific scenario orchestration, reporting, and portfolio logic
  in `fundcompetition`.

Update:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/tests/test_curve_bump.py
```

Required changes:

- remove assertions tied to the local Valmer TIIE rebuild wrapper;
- assert that full scenario pricing uses `price_curve_scenario(...)` with
  `overnight_index_resolver=resolve_valmer_overnight_index`;
- keep focused handle-building assertions only when the test directly exercises
  `build_scenario_curve_handle(...)`.

## Platform Data Handling

Changing `CurveBuildingDetails` to `rate_helper_curve` changes the runtime
interpretation of `key_nodes`. Existing published curve observations created
before this refactor may not contain the new generic OIS fields.

Implementation must perform one of these explicit platform actions:

- republish the latest Valmer TIIE and USD SOFR curve observations after the
  code change, then use latest-curve workflows only; or
- backfill affected historical Valmer curve observations so historical scenario
  dates also contain the new key-node fields.

Do not silently reinterpret old TIIE or USD SOFR key nodes with default
settlement days, calendars, and payment conventions. The refactor must preserve
the old Valmer constructor choices as explicit key-node data.

For USD SOFR futures, preserve the current active-future policy:

```text
active_future_policy = exclude_without_hydrated_sofr_fixings
```

If the implementation includes SOFR futures whose accrual period requires
historical SOFR fixings, those fixings must be hydrated before exporting zero
rates or reconstructing scenario curves.

## Validation Commands

Run these local checks in `valmer-connectors`:

```bash
.venv/bin/python -m pytest tests/test_rates_curves.py tests/test_curve_bootstrap.py tests/test_valmer_curve_reconstruction.py
.venv/bin/python -m pytest tests/test_curve_key_nodes.py
```

Run this focused import check:

```bash
.venv/bin/python -c "from inspect import signature; from msm_pricing.pricing_engine.curves import reconstruct_curve_handle_from_key_nodes, reconstruct_curve_term_structure_from_key_nodes, parse_bond_helper_key_node; from msm_pricing.scenarios.curves import build_scenario_curve_handle, price_curve_scenario; assert 'overnight_index_resolver' in signature(price_curve_scenario).parameters; print('ok')"
```

After downstream cleanup, run these checks in `mexicofundcompetition`:

```bash
.venv/bin/python -m pytest tests/test_curve_bump.py
rg -n "build_tiie_discount_curve_from_key_nodes|build_curve_handle_from_bumped_key_nodes" src tests
```

The final `rg` command must return no active code references.

## Non-Goals

This refactor does not:

- move Valmer source-file parsing into `ms-markets`;
- move Valmer asset registration into `ms-markets`;
- change `ValmerVectorPricesStorage`;
- change public vector quote query helpers;
- change Valmer asset-detail query helpers;
- redesign MXN government bond curve bootstrapping;
- add a new scenario API to `valmer-connectors`;
- make `ms-markets` infer an overnight index from a curve name or provider
  name.
