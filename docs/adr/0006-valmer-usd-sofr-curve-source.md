# ADR 0006: Build Valmer USD SOFR Curve From IRS USD Curve Source

## Status

Implemented locally; platform validation pending.

## Date

2026-07-01

## Success Criteria

This ADR defines the implemented USD SOFR curve source and bootstrap design.
The implementation must:

- parse `IRS_USD_CURVE.csv` as a two-column market quote file
- source the valuation date explicitly from the Valmer English homepage
- build a first pure USD SOFR overnight/OIS curve from CME SOFR futures and USD
  SOFR OIS swap rows
- use native QuantLib SOFR helpers instead of treating the source quotes as
  direct zero-rate nodes
- keep Fed Funds OIS and Fed Funds/SOFR basis rows out of the first SOFR curve
- create or verify explicit `Index`, `IndexConventionDetails`, `Curve`,
  `CurveBuildingDetails`, and market-data-set curve bindings before runtime
  pricing claims are made
- publish the bootstrapped curve through `DiscountCurvesStorage`
- emit source quote provenance in `key_nodes`
- fail closed when the source date, required quote families, or helper
  construction cannot be verified

Live platform publication and pricing-resolution validation remain pending.

## Current Gap

The project now has a Valmer TIIE OIS builder that treats
`IRS_MXN_CURVE.csv` as market quotes and bootstraps a QuantLib OIS curve. The
attached USD source has the same broad shape: it is not a direct curve file,
and its rows must become QuantLib instruments before the final zero-rate curve
is stored.

The USD source is different from the TIIE source in two important ways:

- it includes exchange-traded SOFR futures before the swap tail
- it includes Fed Funds and Fed Funds/SOFR basis rows that belong to separate
  curve decisions

Parsing all rows as one curve would mix SOFR, Fed Funds, and basis instruments
and would produce an invalid curve.

## Source Data

The active Valmer benchmark file is:

```text
https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_USD_CURVE.csv
```

The local test fixture is `data/IRS_USD_CURVE.csv`. The file is headerless and
has two columns:

| Column | Meaning |
| --- | --- |
| `instrument_identifier` | Dot-delimited Valmer benchmark instrument key |
| `quote` | Numeric source quote |

Observed source families in the attached file:

| Row Pattern | Example | Quote Meaning For This ADR | SOFR Curve Use |
| --- | --- | --- | --- |
| `Future.USD.CME.CME SR1 EOM.<MMM>.<YY>` | `Future.USD.CME.CME SR1 EOM.JUL.26,96.355000` | CME one-month SOFR futures price | Include when the contract is usable for the valuation date |
| `Future.USD.CME.CME SR3 IMM.<MMM>.<YY>` | `Future.USD.CME.CME SR3 IMM.SEP.26,96.085350` | CME three-month SOFR futures price | Include when the contract is usable for the valuation date |
| `Swap.<tenor>.USD.SOFR.1D/1Y.SOFR` | `Swap.10Y.USD.SOFR.1D/1Y.SOFR,4.01375000` | USD SOFR OIS par rate, quoted in percent | Include |
| `Swap.<tenor>.USD.FEDFUNDS.1D/1Y.FEDFUNDS1` | `Swap.1Y.USD.FEDFUNDS.1D/1Y.FEDFUNDS1,3.98201300` | Fed Funds OIS par rate | Exclude |
| `Swap.USD.<tenor>.FEDFUNDS.1D/SOFR.1D.SOFR` | `Swap.USD.10Y.FEDFUNDS.1D/SOFR.1D.SOFR,-0.04500000` | Fed Funds/SOFR basis quote | Exclude |

The first SOFR implementation must include only:

```text
Future.USD.CME.CME SR1 EOM.<MMM>.<YY>
Future.USD.CME.CME SR3 IMM.<MMM>.<YY>
Swap.<tenor>.USD.SOFR.1D/1Y.SOFR
```

The observed SOFR swap tenors are:

```text
4Y, 5Y, 7Y, 10Y, 12Y, 15Y, 20Y, 25Y, 30Y, 40Y, 50Y
```

Preserve source tenor tokens when creating QuantLib `Period` instances. Do not
round or relabel source tenors in key-node provenance.

## Valuation Date

`IRS_USD_CURVE.csv` has no valuation-date column. The curve builder must not
infer the valuation date from runtime clock time, file download time, or the
first source instrument.

Decision:

- resolve the source date through the same Valmer English homepage AJAX
  contract used for the TIIE IRS source: open
  `https://www.valmer.com.mx/en/`, then read
  `https://www.valmer.com.mx/public/getInsumoVectorGubernamental.do`
- parse the `Indices_Benchmarks` record's `fecha` using `DD/MM/YYYY`
- on production updates, check this source date before downloading
  `IRS_USD_CURVE.csv`; if the source date is not greater than the latest
  persisted `VALMER_USD_SOFR_OVERNIGHT` observation, return an empty frame and
  do not download the CSV
- emit the SOFR curve row with that valuation date localized to UTC at start
  of day
- fail the update when the homepage context, AJAX date endpoint, or date parse
  fails
- do not fall back to `datetime.utcnow()` or source download time

## QuantLib Instruments

The SOFR curve builder must create QuantLib market helpers, not direct
zero-rate nodes.

### SOFR Futures

For `Future.USD.CME.CME SR1 EOM.<MMM>.<YY>` rows:

| Field | Value |
| --- | --- |
| QuantLib helper | `ql.SofrFutureRateHelper` |
| Price | source quote as futures price |
| Reference month/year | parsed from `<MMM>.<YY>` |
| Reference frequency | `ql.Monthly` |
| Convexity adjustment | `0.0` unless a confirmed source is added |
| Pillar | `ql.Pillar.LastRelevantDate` |

For `Future.USD.CME.CME SR3 IMM.<MMM>.<YY>` rows:

| Field | Value |
| --- | --- |
| QuantLib helper | `ql.SofrFutureRateHelper` |
| Price | source quote as futures price |
| Reference month/year | parsed from `<MMM>.<YY>` |
| Reference frequency | `ql.Quarterly` |
| Convexity adjustment | `0.0` unless a confirmed source is added |
| Pillar | `ql.Pillar.LastRelevantDate` |

Futures prices imply rates through the normal `100 - price` convention, but the
helper input is the source price. Key nodes should preserve both the source
price and the implied decimal rate.

### Active Futures And Fixings

A SOFR future whose accrual period has already started at the valuation date
requires historical SOFR fixings. QuantLib will fail if those fixings are
missing.

Decision:

- include an already-started SOFR futures contract only when the implementation
  can hydrate all required historical SOFR fixings into the QuantLib SOFR index
- otherwise exclude that active future explicitly and record the exclusion in
  diagnostics or test-visible output
- never synthesize missing SOFR fixings from the futures price, adjacent
  futures, or the bootstrapped curve

Using a 2026-06-30 valuation date with the attached file, the
`Future.USD.CME.CME SR3 IMM.JUN.26` contract is already active and requires
fixings from its accrual start. The `SR1 EOM.JUL.26` and later SR3 futures are
usable without historical fixing hydration because their accrual periods start
after the valuation date.

### SOFR OIS Swaps

For `Swap.<tenor>.USD.SOFR.1D/1Y.SOFR` rows, create one helper per source row:

| Field | Value |
| --- | --- |
| QuantLib helper | `ql.OISRateHelper` |
| Tenor | parsed from `Swap.<tenor>...` |
| Rate | source quote divided by `100` |
| Index | `ql.Sofr()` |
| Settlement days | `2` |
| Payment convention | `ql.ModifiedFollowing` |
| Fixed payment frequency | `ql.Annual` |
| Averaging | compound overnight averaging |
| Pillar | `ql.Pillar.LastRelevantDate` |

Use the QuantLib SOFR index calendar/day-count conventions through `ql.Sofr()`
instead of reimplementing SOFR calendar logic in the parser.

### Bootstrap

Bootstrap the curve with:

```text
ql.PiecewiseLogLinearDiscount
```

using:

- included SOFR futures helpers
- included SOFR OIS swap helpers
- `ql.Actual360()`
- extrapolation enabled only after construction, when required by the existing
  discount-curve export path

Export zero rates keyed by strictly positive days to maturity. The stored zero
rates must come from the bootstrapped QuantLib curve, not from copying futures
implied rates or swap par rates directly.

## Curve Identity And Bindings

Introduce explicit USD SOFR pricing identities:

```text
USD_SOFR_OVERNIGHT
    -> Index row and index-convention selector

VALMER_USD_SOFR_OVERNIGHT
    -> Curve row and DiscountCurvesStorage curve_identifier
```

Do not create separate curve identities for `SR1`, `SR3`, or each swap tenor.
Those are construction instruments, not runtime curve selectors.

Do not attach curve identity to `AssetTable`. Curve selection must remain a
`PricingMarketDataSetCurveBinding` decision.

Initial market-data-set bindings:

| Role | Selector | Curve |
| --- | --- | --- |
| `projection` | `USD_SOFR_OVERNIGHT` | `VALMER_USD_SOFR_OVERNIGHT` |

This follows the existing TIIE OIS pattern: the helper-bootstrapped overnight
curve is modeled as the projection curve for the reference index, while the
stored observations still use `DiscountCurvesStorage`.

The bootstrap cleanup removes any Valmer-owned legacy
`discount:index:<USD_SOFR_OVERNIGHT.uid>:mid` binding. Keeping that row would
turn USD SOFR discounting into an unreviewed seed default instead of an explicit
pricing policy decision.

If a product later needs SOFR as an explicit discount or z-spread benchmark
role, add a separate `PricingMarketDataSetCurveBinding` decision. Do not create
another curve identity unless the market-data policy actually requires a
separate curve.

## Index Convention Details

Seed `USD_SOFR_OVERNIGHT` as a real interest-rate `Index` row.

Target convention details:

| Field | Target |
| --- | --- |
| `index_family` | `SOFR` |
| `currency_code` | `USD` |
| `period` | `1D` |
| `settlement_days` | `0` |
| `day_counter_code` | `Actual360` |
| `business_day_convention` | `ModifiedFollowing` |
| `fixings_unique_identifier` | `USD_SOFR_OVERNIGHT` |

The implementation should map this convention to `ql.Sofr()` for runtime
QuantLib index resolution. If the current calendar JSON codec does not already
have a specific SOFR calendar token, the implementation must either add the
codec support or document the accepted existing token before seeding live rows.

## Curve Building Details

Create `CurveBuildingDetails` for `VALMER_USD_SOFR_OVERNIGHT` with helper
bootstrap semantics.

Target shape:

| Field | Target |
| --- | --- |
| `builder_type` | `sofr_futures_ois_helper_bootstrap` |
| `quote_convention` | `key_node_quote` |
| `rate_unit` | `key_node_unit` |
| `day_counter_code` | `Actual360` |
| `calendar_code` | SOFR-compatible United States calendar token |
| `interpolation_method` | `log_linear_discount` |
| `compounding` | `compounded_annual` |
| `bootstrap_method` | `quantlib_piecewise_log_linear_discount` |

The builder payload should document:

- source file: `IRS_USD_CURVE.csv`
- included source row patterns
- excluded source row patterns
- SOFR futures helper rules
- SOFR OIS swap helper rules
- active-future fixing requirement
- output quote type: zero rate
- output quote unit: decimal

## DataNode Output

The curve must be published through `DiscountCurvesNode`:

```text
index:   time_index, curve_identifier
columns: curve, key_nodes
```

The emitted row must use:

```text
curve_identifier = VALMER_USD_SOFR_OVERNIGHT
```

`curve` stores the final bootstrapped zero-rate payload:

```json
{
  "1": 0.0371,
  "30": 0.0372,
  "365": 0.0402
}
```

The numbers above are illustrative. Actual stored zero rates must come from
the QuantLib bootstrapped curve.

`key_nodes` stores the construction inputs.

Example futures key node:

```json
{
  "maturity_date": "2026-08-01",
  "asset_identifier": "Future.USD.CME.CME SR1 EOM.JUL.26",
  "instrument_type": "sofr_future",
  "helper_type": "sofr_future_rate_helper",
  "quote": 96.355,
  "quote_type": "futures_price",
  "quote_unit": "price",
  "quote_side": "mid",
  "quote_source": "IRS_USD_CURVE.csv",
  "implied_rate": 0.03645,
  "implied_rate_unit": "decimal",
  "contract_code": "SR1",
  "reference_month": "JUL",
  "reference_year": 2026,
  "reference_frequency": "Monthly",
  "earliest_date": "2026-07-01",
  "pillar_date": "2026-08-01"
}
```

Example SOFR OIS swap key node:

```json
{
  "maturity_date": "2036-07-02",
  "asset_identifier": "Swap.10Y.USD.SOFR.1D/1Y.SOFR",
  "instrument_type": "overnight_indexed_swap",
  "helper_type": "ois_rate_helper",
  "quote": 0.0401375,
  "quote_type": "par_swap_rate",
  "quote_unit": "decimal",
  "quote_side": "mid",
  "quote_source": "IRS_USD_CURVE.csv",
  "source_quote": 4.01375,
  "source_quote_unit": "percent",
  "tenor": "10Y",
  "floating_index": "USD_SOFR_OVERNIGHT",
  "fixed_payment_frequency": "Annual",
  "day_counter": "Actual360",
  "earliest_date": "2026-07-02",
  "pillar_date": "2036-07-02"
}
```

The Valmer USD SOFR curve DataNode attaches a source-specific key-node validator
before the core `DiscountCurvesNode` compresses provenance. The validator
enforces SR1/SR3 SOFR futures, USD SOFR OIS helpers, `mid` quote side, decimal
par-swap quotes, futures-price quotes, and exclusion of Fed Funds OIS and
Fed Funds/SOFR basis rows.

The implementation should not populate row-level `metadata_json` unless there
is a specific operational diagnostic that cannot belong in `key_nodes` or
`CurveBuildingDetails`.

## Excluded Rows

Fed Funds rows are intentionally excluded from the first SOFR curve:

```text
Swap.<tenor>.USD.FEDFUNDS.1D/1Y.FEDFUNDS1
```

Those rows should be handled by a separate Fed Funds curve ADR if needed.

Fed Funds/SOFR basis rows are also excluded:

```text
Swap.USD.<tenor>.FEDFUNDS.1D/SOFR.1D.SOFR
```

Those rows require a separate basis-curve decision that depends on both a Fed
Funds curve and a SOFR curve. They must not be folded into the pure SOFR
bootstrap.

## Implementation Tasks

- [x] Add USD SOFR constants and static seed definitions:
      `USD_SOFR_OVERNIGHT` and `VALMER_USD_SOFR_OVERNIGHT`.
- [x] Add `Index` and `IndexConventionDetails` seed data for
      `USD_SOFR_OVERNIGHT`.
- [x] Add `Curve`, `CurveBuildingDetails`, and market-data-set curve binding
      definitions for `VALMER_USD_SOFR_OVERNIGHT`.
- [x] Add an `IRS_USD_CURVE.csv` reader that preserves the two-column source
      shape.
- [x] Add source-family classification for SOFR futures, SOFR OIS swaps, Fed
      Funds OIS swaps, Fed Funds/SOFR basis swaps, and unsupported rows.
- [x] Resolve the valuation date through the Valmer benchmark-date contract.
- [x] Gate production updates on the Valmer source date before downloading the
      CSV.
- [x] Parse `SR1` and `SR3` futures contract month/year tokens.
- [x] Build `ql.SofrFutureRateHelper` instances for usable SOFR futures.
- [x] Detect already-started futures and include them only when required SOFR
      fixings are hydrated.
- [x] Parse `Swap.<tenor>.USD.SOFR.1D/1Y.SOFR` rows and normalize source
      percent quotes to decimal OIS rates.
- [x] Build `ql.OISRateHelper` instances against `ql.Sofr()`.
- [x] Bootstrap with `ql.PiecewiseLogLinearDiscount`.
- [x] Export zero-rate curve points keyed by positive days to maturity.
- [x] Emit futures and OIS `CurveKeyNode`-compatible provenance.
- [x] Attach a Valmer USD SOFR key-node semantic validator before core
      compression.
- [x] Keep Fed Funds and basis rows out of `key_nodes`.
- [x] Add a CLI/service update path only after the builder and seed definitions
      are covered by tests.
- [x] Update pricing docs after implementation is complete.

## Validation Plan

Unit tests must cover:

- date resolver follows the Valmer English homepage AJAX flow and selects the
  `Indices_Benchmarks` date record
- parser reads `IRS_USD_CURVE.csv` as two columns
- parser classifies SOFR futures, SOFR OIS swaps, Fed Funds OIS swaps, Fed
  Funds/SOFR basis swaps, and unsupported rows
- parser rejects a source file with no usable SOFR futures or SOFR OIS swaps
- futures parser preserves `SR1` versus `SR3`, month token, year token, and
  monthly versus quarterly frequency
- SOFR OIS parser preserves swap tenor tokens
- SOFR OIS quotes are normalized from percent to decimal
- futures key nodes preserve source price and implied decimal rate
- active futures without required historical SOFR fixings are excluded or fail
  explicitly according to the implemented policy
- QuantLib helper construction succeeds for the observed usable futures and
  SOFR swap tenor set
- bootstrapped output has strictly positive days-to-maturity keys
- `key_nodes` contain no Fed Funds OIS rows and no Fed Funds/SOFR basis rows
- emitted frame is keyed by `time_index` and `curve_identifier`
- emitted `curve_identifier` is `VALMER_USD_SOFR_OVERNIGHT`
- `CurveBuildingDetails` records futures/OIS helper bootstrap semantics
- market-data-set projection curve selection resolves `USD_SOFR_OVERNIGHT` to
  `VALMER_USD_SOFR_OVERNIGHT`

Operational validation, when implementation is ready, must run the normal curve
DataNode update with an explicit namespace or debug mode first, then verify:

- one `DiscountCurvesStorage` row exists for the Valmer source date
- `curve_identifier` is `VALMER_USD_SOFR_OVERNIGHT`
- key-node counts match the included SOFR futures plus included SOFR OIS swaps
- any excluded active futures are visible in diagnostics
- Fed Funds and Fed Funds/SOFR basis rows are not present in `key_nodes`
- pricing resolution for `USD_SOFR_OVERNIGHT` selects the SOFR curve for the
  intended projection role and quote side

## Consequences

This design adds a USD curve path without changing the existing TIIE or MXN
government curve identities. It keeps the same project invariant: source rows
become auditable market helpers, while the stored curve remains the canonical
`DiscountCurvesStorage` zero-rate payload consumed by pricing.

The implementation must treat the source file as incomplete without a source
date. It must also treat already-started futures as incomplete without
historical SOFR fixings.

Fed Funds and basis support remain deliberate follow-up decisions, not hidden
side effects of building the SOFR curve.
