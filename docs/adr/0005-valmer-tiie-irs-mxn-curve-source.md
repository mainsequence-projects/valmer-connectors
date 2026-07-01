# ADR 0005: Replace Deprecated TIIE Zero-Rate CSV With Valmer IRS MXN Curve Bootstrap

## Status

Implemented.

## Date

2026-07-01

## Success Criteria

This ADR defines the implemented TIIE source refactor. The implementation must:

- stop treating the deprecated Valmer TIIE CSV as the active source
- source TIIE curve quotes from
  `https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_MXN_CURVE.csv`
- source the valuation date from the Valmer homepage benchmark date contract
- build `VALMER_TIIE_OVERNIGHT` from QuantLib overnight-indexed swap helpers
- add a real TIIE overnight front-end anchor when a confirmed TIIE ON source is
  available
- keep TIIE reference indexes as selectors, not curve identities
- keep curve observations in `DiscountCurvesStorage`
- publish `curve` and `key_nodes` without row-level `metadata_json`
- leave FX and USD SOFR cross-currency rows out of the domestic TIIE OIS curve
  until a separate cross-currency curve decision exists
- never use `FX.USD.MXN.ON` as a TIIE curve anchor

## Replaced Gap

The replaced implementation treated the Valmer TIIE source as a direct
zero-rate curve file:

```text
deprecated direct zero-rate CSV
    -> id, curve_name, asof_yyMMdd, idx, zero_rate
    -> direct zero-rate nodes
    -> VALMER_TIIE_OVERNIGHT
```

That path does not create QuantLib market instruments. It reads source rows as
already-constructed curve nodes and stores those nodes as the final compressed
curve payload.

The replacement source is not that shape. `IRS_MXN_CURVE.csv` is a two-column
market quote file. It contains FX rows, cross-currency swap rows, and domestic
MXN FTIIE overnight swap rows. Parsing it as the old direct zero-rate file would
produce an invalid curve.

## Source Data

The active source URL is:

```text
https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_MXN_CURVE.csv
```

The observed file is headerless and has two columns:

| Column | Meaning |
| --- | --- |
| `instrument_identifier` | Dot-delimited Valmer benchmark instrument key |
| `quote` | Numeric source quote |

Observed source families on 2026-07-01:

| Row Pattern | Example | Quote Meaning For This ADR | TIIE Curve Use |
| --- | --- | --- | --- |
| `FX.USD.MXN...` | `FX.USD.MXN,17.486900000000` | FX spot or forward points | Exclude |
| `Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR` | `Swap.104W.MXN.FTIIE.1D/USD.SOFR.1D.SOFR,0.15000000` | Cross-currency swap or basis quote | Exclude |
| `Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO` | `Swap.28D.MXN.FTIIE.1D/28D.BANXICO,6.52875000` | Domestic FTIIE overnight OIS par rate, quoted in percent | Include |

The domestic TIIE OIS curve must include only rows matching:

```text
Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO
```

The current observed domestic tenors are:

```text
28D, 8W, 12W, 24W, 36W, 52W, 104W, 156W, 208W, 260W,
364W, 520W, 182M, 1040W, 364M
```

Preserve the source tenor token exactly when creating the QuantLib `Period`.
Do not coerce `182M` or `364M` into rounded year labels unless Valmer publishes
a different contract.

The CSV also contains `FX.USD.MXN.ON` and `FX.USD.MXN.TN` rows. Those are FX
overnight/tom-next forward-point rows, not TIIE overnight rates. They must not
be used as front-end anchors for `VALMER_TIIE_OVERNIGHT`.

## Valuation Date

`IRS_MXN_CURVE.csv` has no valuation-date column. The curve builder must not
infer the date from runtime clock time, file download time, or the first source
tenor.

The Valmer English homepage is the benchmark-date source:

```text
https://www.valmer.com.mx/en/
```

The curve builder reads that page directly and parses the rendered same-day
benchmark table caption:

```text
#tablaMismoDia span.lbFechaIndice
```

On 2026-07-01 the observed table used:

```html
<caption>Fecha <span class="lbFechaIndice">30/06/2026</span></caption>
```

On each update run, the production builder reads this homepage before it reads
`IRS_MXN_CURVE.csv`. If the Valmer benchmark date is not greater than the
latest persisted `DiscountCurvesStorage.time_index` for
`VALMER_TIIE_OVERNIGHT`, the builder returns an empty curve frame and does not
download the CSV. The local `data/IRS_MXN_CURVE.csv` file is only a temporary
analysis/test fixture and is not part of the production update path.

Decision:

- parse the date text from `#tablaMismoDia span.lbFechaIndice` using
  `DD/MM/YYYY`
- emit the TIIE curve row with that valuation date
- localize the TIIE `time_index` to UTC at start of day, for example
  `2026-06-30T00:00:00Z`
- fail the update when the homepage is unavailable or the date cannot be
  parsed
- do not fall back to `datetime.utcnow()` or source download time

## Front-End Anchor

`Swap.28D.MXN.FTIIE.1D/28D.BANXICO` is not a one-day curve anchor. In that
identifier:

- `FTIIE.1D` describes the overnight floating index used by the swap
- `/28D` describes the fixed/payment tenor of the first OIS quote

Using the local temporary CSV with valuation date 2026-06-30, QuantLib creates
the first domestic helper with:

```text
identifier: Swap.28D.MXN.FTIIE.1D/28D.BANXICO
earliest date: 2026-07-01
maturity/pillar date: 2026-07-29
```

QuantLib still anchors the curve at the valuation date with discount factor
`1.0`, but the first market-quoted pillar is the 28-day OIS maturity. The
overnight-to-28-day front end is therefore implied by interpolation and the 28D
par swap quote unless a separate overnight market quote or fixing is added.

Decision:

- the 28D OIS quote is enough to make the curve mathematically bootstrappable
- it is not enough to fully pin the overnight front end of a production OIS
  curve
- the production curve should add a real TIIE overnight front anchor when the
  project has a confirmed source
- the anchor source should be the TIIE overnight fixing/reference-rate source
  already modeled as `TIIE_OVERNIGHT`, not any FX row from `IRS_MXN_CURVE.csv`
- if the implementation cannot resolve a confirmed ON anchor, it may still
  publish the OIS-only curve, but the missing anchor must be explicit in tests,
  key-node counts, and operational diagnostics

Potential QuantLib front-anchor choices are:

| Source Input | QuantLib Helper | Notes |
| --- | --- | --- |
| confirmed one-day TIIE overnight rate | `ql.DepositRateHelper` | Pins valuation date to next Mexico business day. |
| confirmed dated overnight OIS quote | `ql.OISRateHelper.forDates` | Use when the source gives explicit start and end dates. |

Do not infer the anchor from the domestic 28D swap. Do not use the
cross-currency or FX rows as the anchor.

## QuantLib Instruments

The domestic swap rows are overnight-indexed swap market quotes. The curve
builder must create QuantLib OIS helpers, not direct zero-rate nodes.

Create one QuantLib overnight index for the source:

| Field | Value |
| --- | --- |
| QuantLib class | `ql.OvernightIndex` |
| Name | `FTIIE` |
| Currency | `ql.MXNCurrency()` |
| Calendar | `ql.Mexico()` |
| Settlement days | `1` |
| Day counter | `ql.Actual360()` |
| Forwarding handle | relinkable or empty handle during bootstrap |

For every included domestic row, create one helper:

| Field | Value |
| --- | --- |
| QuantLib class | `ql.OISRateHelper` |
| Tenor | parsed from `Swap.<tenor>...` |
| Rate | source quote divided by `100` |
| Index | the FTIIE overnight index above |
| Payment convention | `ql.ModifiedFollowing` |
| Payment frequency | `ql.EveryFourthWeek` |
| Fixed payment frequency | `ql.EveryFourthWeek` |
| Averaging | compound overnight averaging |
| Pillar | `ql.Pillar.LastRelevantDate` |

Then bootstrap the curve with `ql.PiecewiseLogLinearDiscount` using the OIS
helper vector, plus the optional real ON front-end helper when available, and
`ql.Actual360()`. Export the constructed curve as zero rates keyed by days to
maturity, matching the existing `DiscountCurvesStorage.curve` contract.

A local QuantLib 1.42.1 sanity check constructed all 15 observed domestic
helpers and bootstrapped a piecewise log-linear discount curve from the
2026-06-30 valuation date. The produced QuantLib curve had pillars from the
valuation date through the final `364M` helper maturity.

Do not use `ql.SwapRateHelper` for these rows. `SwapRateHelper` is the correct
family for a fixed-vs-Ibor swap source. The observed domestic identifiers state
`FTIIE.1D/28D.BANXICO`, so the floating leg is the overnight FTIIE leg and the
helper family is OIS.

Do not use QuantLib cross-currency helpers for the TIIE OIS curve. The
`MXN.FTIIE.1D/USD.SOFR.1D.SOFR` rows require USD SOFR, FX, and cross-currency
curve architecture that is outside this ADR.

## Curve Identity And Bindings

Keep the current pricing identity split:

```text
TIIE_OVERNIGHT, TIIE_28, TIIE_91, TIIE_182
    -> real Index rows and selector indexes

VALMER_TIIE_OVERNIGHT
    -> Curve row and DiscountCurvesStorage curve_identifier
```

The refactor must not create a new `IRS_MXN_CURVE` index or a separate
`VALMER_TIIE_28` curve. The source file changes the construction input, not the
index model.

Existing market-data-set curve bindings remain conceptually correct:

| Role | Selector | Curve |
| --- | --- | --- |
| `projection` | `TIIE_OVERNIGHT` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `TIIE_28` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `TIIE_91` | `VALMER_TIIE_OVERNIGHT` |
| `projection` | `TIIE_182` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `TIIE_OVERNIGHT` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `TIIE_28` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `TIIE_91` | `VALMER_TIIE_OVERNIGHT` |
| `z_spread_base` | `TIIE_182` | `VALMER_TIIE_OVERNIGHT` |

If the product later needs a separate discount role, add a
`PricingMarketDataSetCurveBinding` role decision. Do not model that as another
index.

## Curve Building Details

The `VALMER_TIIE_OVERNIGHT` build details must change from direct
`zero_rate_curve` semantics to OIS helper bootstrap semantics.

Target shape:

| Field | Target |
| --- | --- |
| `builder_type` | `ois_swap_helper_bootstrap` |
| `quote_convention` | `key_node_quote` |
| `rate_unit` | `key_node_unit` |
| `day_counter_code` | `Actual360` |
| `calendar_code` | `Mexico` |
| `interpolation_method` | `log_linear_discount` |
| `compounding` | `compounded_annual` |
| `bootstrap_method` | `quantlib_piecewise_log_linear_discount` |

The builder payload should document the source grammar, output type, and helper
rules. It should not move per-observation facts into row `metadata_json`.

## DataNode Output

The curve remains a `DiscountCurvesNode` output:

```text
index:   time_index, curve_identifier
columns: curve, key_nodes
```

The emitted row must use:

```text
curve_identifier = VALMER_TIIE_OVERNIGHT
```

`curve` stores the final bootstrapped zero-rate payload:

```json
{
  "28": 0.0652875,
  "56": 0.0654
}
```

The numbers above are illustrative. Actual stored zero rates must come from the
QuantLib bootstrapped curve, not from copying the par swap quotes directly.

`key_nodes` stores the input swap quotes used to construct that observation.
Example shape:

```json
[
  {
    "maturity_date": "2026-07-29",
    "asset_identifier": "Swap.28D.MXN.FTIIE.1D/28D.BANXICO",
    "instrument_type": "overnight_indexed_swap",
    "helper_type": "ois_rate_helper",
    "quote": 0.0652875,
    "quote_type": "par_swap_rate",
    "quote_unit": "decimal",
    "quote_side": "mid",
    "quote_source": "IRS_MXN_CURVE.csv",
    "source_quote": 6.52875,
    "source_quote_unit": "percent",
    "tenor": "28D",
    "floating_index": "TIIE_OVERNIGHT",
    "fixed_payment_frequency": "EveryFourthWeek",
    "day_counter": "Actual360"
  }
]
```

The Valmer TIIE curve DataNode attaches a source-specific key-node validator
before the core `DiscountCurvesNode` compresses provenance. The validator
enforces the domestic `MXN.FTIIE.1D/28D.BANXICO` OIS family, decimal par-swap
quotes, `mid` quote side, FTIIE overnight floating index, and Actual/360
construction metadata.

When a real TIIE ON anchor is available, include it as the first key node:

```json
{
  "maturity_date": "2026-07-01",
  "asset_identifier": "TIIE_OVERNIGHT",
  "instrument_type": "overnight_deposit",
  "helper_type": "deposit_rate_helper",
  "quote": 0.0663,
  "quote_type": "overnight_rate",
  "quote_unit": "decimal",
  "quote_side": "mid",
  "quote_source": "confirmed_tiie_overnight_source",
  "tenor": "1D",
  "day_counter": "Actual360"
}
```

The implementation must not populate row-level `metadata_json` for this curve
source. Source provenance belongs in `key_nodes` and stable construction rules
belong in `CurveBuildingDetails`.

## Implementation Tasks

- [x] Replace the TIIE source URL and source-file metadata with
      `IRS_MXN_CURVE.csv`.
- [x] Add a valuation-date resolver for the Valmer homepage benchmark date
      contract.
- [x] Gate production updates on Valmer source date being newer than the latest
      stored TIIE curve observation before downloading the CSV.
- [x] Parse `IRS_MXN_CURVE.csv` as a two-column file with source-family
      classification.
- [x] Include only `Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO` rows for
      `VALMER_TIIE_OVERNIGHT`.
- [x] Convert included domestic quotes from percent to decimal.
- [x] Do not use `FX.USD.MXN.ON` as a TIIE ON anchor; no confirmed ON anchor is
      available in this source.
- [x] Build the FTIIE overnight QuantLib index and one `ql.OISRateHelper` per
      domestic swap row.
- [x] Bootstrap with `ql.PiecewiseLogLinearDiscount` and export zero-rate curve
      points keyed by days to maturity.
- [x] Emit `CurveKeyNode`-compatible swap key nodes.
- [x] Attach a Valmer TIIE key-node semantic validator before core compression.
- [x] Update `CurveBuildingDetails` for `VALMER_TIIE_OVERNIGHT` to OIS helper
      bootstrap semantics.
- [x] Keep existing TIIE market-data-set curve bindings and add no new TIIE
      curve or index identities.
- [x] Update tests and active pricing docs after implementation is complete.

## Validation Plan

Unit tests must cover:

- date resolver parses the Valmer English homepage and selects
  `#tablaMismoDia span.lbFechaIndice`
- parser classifies FX, cross-currency, and domestic OIS rows
- parser rejects missing domestic OIS rows
- tenor parser preserves `D`, `W`, and `M` source units
- domestic quotes are normalized from percent to decimal
- QuantLib helper construction succeeds for the observed domestic tenor set
- the first OIS helper from the temporary CSV starts on 2026-07-01 and pillars
  on 2026-07-29 for valuation date 2026-06-30
- the implementation never classifies `FX.USD.MXN.ON` as a TIIE anchor
- bootstrapped output has strictly positive days-to-maturity keys
- `key_nodes` contain par swap quote provenance
- emitted frame has no row-level `metadata_json`
- existing TIIE selector bindings still resolve to `VALMER_TIIE_OVERNIGHT`

Operational validation must run the normal TIIE curve DataNode update with an
explicit namespace or debug mode first, then verify:

- one `DiscountCurvesStorage` row exists for the Valmer source date
- `curve_identifier` is `VALMER_TIIE_OVERNIGHT`
- `key_nodes` count equals the included domestic OIS helper count
  plus one only when a confirmed TIIE ON anchor is included
- FX and cross-currency rows are not present in `key_nodes`
- pricing resolution for `TIIE_28` and `TIIE_182` selects the same
  `VALMER_TIIE_OVERNIGHT` curve

## Consequences

This refactor changes TIIE from a direct curve ingestion path to a true
instrument-helper bootstrap path. The source data becomes auditable at the
market-instrument level, and the final stored `curve` remains compatible with
`DiscountCurvesStorage` consumers.

The new source date is independent from the CSV. The implementation must fail
closed when the date cannot be resolved because the CSV alone is not a complete
market snapshot.

Cross-currency rows are intentionally left unused. Supporting those rows
requires a separate ADR covering USD SOFR, FX spot/forward handling, and
QuantLib cross-currency basis helpers.
