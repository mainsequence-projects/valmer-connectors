# ADR 0004: Bootstrap Mexican Government Bond Curves From Valmer Vector Storage

## Status

Implemented; static curve relationship model superseded on 2026-06-30.

The curve publication path remains active, but the static pricing rows in this
ADR were updated after the `msm_pricing` curve model moved curve selection out
of `Curve.index_uid` and into `PricingMarketDataSetCurveBinding`. The current
implementation must not create `MXN_GOVERNMENT_BOND` as an `Index`.

## Date

2026-06-08

## Success Criteria

This ADR records the implemented Valmer-driven Mexican government bond curve
bootstrap. The path is separate from asset registration, vector price
publication, bond pricing hydration, and the Valmer TIIE CSV curve path.

The implementation that follows this ADR must:

- extract the Valmer vector valuation date from each source row/file using the
  same `fecha` semantics as vector price storage
- select only the supported Mexican government MXN bootstrap instruments
- build the first curve from CETES and M Bonos source rows
- use QuantLib for instrument helpers and bootstrapping
- persist the output through `msm_pricing.data_nodes.DiscountCurvesNode`
- encode the official CETES and M Bonos valuation conventions before
  implementing helpers
- keep curve identity on the core `msm_pricing.api.Curve` row, keep build
  mechanics on `CurveBuildingDetails`, and keep index-selector-to-curve
  resolution on `PricingMarketDataSetCurveBinding`
- fail explicitly when required source fields or conventions are missing
- leave `AssetTable` unchanged

## Context

The project already imports Valmer vector files through
`ImportValmer.prepare_source_data()` and publishes daily vector prices through
`ValmerVectorPricesStorage`.

Vector price publication currently derives:

- `valuation_date` from Valmer source column `fecha`
- `time_index` as end of valuation day:

```python
time_index = valuation_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
```

That same source-date rule should be used for the Mexican government bond curve
bootstrap. The curve builder should not infer valuation dates from runtime
clock time or artifact ingestion time.

The project also already has a Valmer TIIE curve path, but that path is a
different source and shape:

- source: Valmer public MexDer CSV
- instrument set: none; source is already a curve file
- output curve: `VALMER_TIIE_OVERNIGHT`

The Mexican government bond curve is different:

- source: persisted `ValmerVectorPricesStorage` rows joined to
  `ValmerAssetDetailsTable`
- instrument set: tradable government bond rows from persisted vector snapshots
- output curve: bootstrapped from observed instrument prices

Do not extend the TIIE CSV parser to handle this. This is a second curve source
builder that consumes persisted vector rows.

## Decision

Add a Valmer Mexican government bond bootstrap path that:

1. reads persisted Valmer vector rows and static asset details
2. groups rows by vector valuation date
3. selects supported Mexican government MXN instruments
4. builds QuantLib helpers for CETES and M Bonos
5. bootstraps a discount curve with strict interpolation policy
6. emits a `DiscountCurvesNode` frame keyed by `time_index` and
   `curve_identifier`

The first supported instrument families are:

| Instrument Family | Valmer `tipovalor` | Valmer `emisora` | Instrument Meaning |
| --- | --- | --- | --- |
| CETES | `BI` | `CETES` | Mexican zero-coupon government bills |
| M Bonos | `M` | `BONOS` | Mexican fixed-rate government bonds |

Rows must also satisfy:

- `monedaemision == "MPS"` because Valmer uses `MPS` for MXN-denominated rows
- `fecha` is present and parseable
- maturity date `fechavcto` is present and parseable
- price fields needed by the selected QuantLib helper are present

If `sector` is present, the selector requires `sector == "GUBERNAMENTAL"`.
If persisted Valmer details omit `sector`, the selector proceeds only for the
explicit `(tipovalor, emisora)` allow-list above.

## Source Row Contract

The bootstrap builder consumes persisted vector snapshots, not raw vector
files. The curve source is `ValmerVectorPricesStorage` joined to
`ValmerAssetDetailsTable` on
`ValmerAssetDetailsTable.valmer_unique_identifier =
ValmerVectorPricesStorage.asset_identifier`.

Storage-to-builder mapping:

| Builder field | Source | Required for | Meaning |
| --- | --- | --- | --- |
| `time_index` | `ValmerVectorPricesStorage.time_index` | all rows | Snapshot timestamp and curve update boundary |
| `unique_identifier` | `ValmerVectorPricesStorage.asset_identifier` | all rows | Valmer asset identifier for diagnostics and duplicate checks |
| `fecha` | `ValmerVectorPricesStorage.valuation_date` | all rows | Valuation date; falls back to `time_index` when missing |
| `preciolimpio` | `ValmerVectorPricesStorage.clean_price` | M Bonos | Clean price quote |
| `preciosucio` | `ValmerVectorPricesStorage.dirty_price` | all rows | CETES quote and M Bonos dirty-price validation |
| `interesesacumulados` | `ValmerVectorPricesStorage.accrued_interest` | M Bonos | Accrued interest for clean + accrued = dirty validation |
| `diastransccpn` | `ValmerVectorPricesStorage.days_since_coupon` | M Bonos when present | Actual/360 accrued-interest validation input |
| `valornominalactualizado` | `ValmerVectorPricesStorage.adjusted_face_value` | not consumed by helpers | Selected for source parity and future use |
| `yield_rate` | `ValmerVectorPricesStorage.yield_rate` | all rows when available | Valmer `TASA DE RENDIMIENTO`, normalized from percent to decimal in key nodes |
| `tipovalor` | `ValmerAssetDetailsTable.security_type` | all rows | Valmer security type |
| `emisora` | `ValmerAssetDetailsTable.issuer` | all rows | Valmer issuer/family code |
| `serie` | `ValmerAssetDetailsTable.series` | all rows | Instrument series |
| `sector` | `ValmerAssetDetailsTable.sector` | all rows when present | Must be `GUBERNAMENTAL` when populated |
| `fechaemision` | `ValmerAssetDetailsTable.issue_date` | M Bonos | Issue date for schedule generation |
| `fechavcto` | `ValmerAssetDetailsTable.maturity_date` | all rows | Maturity date |
| `valornominal` | `ValmerAssetDetailsTable.face_value` | optional | CETES default to `10`; M Bonos default to `100` |
| `monedaemision` | `ValmerAssetDetailsTable.issue_currency` | all rows | Must be `MPS` |
| `freccpn` | `ValmerAssetDetailsTable.coupon_frequency` | M Bonos | Must parse to `182` days |
| `tasacupon` | `ValmerAssetDetailsTable.coupon_rate` | M Bonos | Coupon rate |

CETES rows require `tipovalor = BI`, `emisora = CETES`, and
`monedaemision = MPS`. They build zero-coupon bond helpers using `preciosucio`
as the market quote. `valornominal` is optional and defaults to `10`.

M Bonos rows require `tipovalor = M`, `emisora = BONOS`, and
`monedaemision = MPS`. They build fixed-rate bond helpers using
`preciolimpio`, `preciosucio`, `interesesacumulados`, `fechaemision`,
`fechavcto`, `freccpn`, and `tasacupon`. `freccpn` must resolve to 182 days.
`valornominal` is optional and defaults to `100`. `diastransccpn` is used for
accrual validation when present.

`reglacupon` / `coupon_rule` is not part of the current curve-source frame and
is not consumed by the implemented helper builder. If a later M Bonos schedule
requires coupon-rule-specific behavior, add `coupon_rule` to the storage
loader, consume it in schedule construction, and update tests with the same
change.

The builder preserves `unique_identifier` for diagnostics and helper
attribution, but curve storage must not be asset-indexed.

## Issuer And Valuation Standards

The implementation follows the official SHCP/Banco de Mexico
technical notes for CETES and BONOS rather than generic bond defaults.

Source documents:

- [Technical Description of Mexican Federal Treasury Certificates (CETES)](https://www.hacienda.gob.mx/English/public_credit_new/domestic_debt/Documents/Goverment/tn_cetes.pdf)
- [Technical Description of Bonos de Desarrollo del Gobierno Federal con tasa de interes fija (BONOS)](https://hacienda.gob.mx/English/public_credit_new/domestic_debt/Documents/Goverment/tn_bonos.pdf)
- [Banco de Mexico government securities market chapter](https://www.banxico.org.mx/elib/mercado-valores-gub-en/OEBPS/Text/iien.html)
- [Clearstream Mexico settlement process](https://www.clearstream.com/clearstream-en/res-library/market-coverage/settlement-process-mexico-1281864)
- [QuantLib-Python dates and calendars](https://quantlib-python-docs.readthedocs.io/en/latest/dates.html)

### Common MXN Government Convention

Use these conventions for the `VALMER_MXN_GOVERNMENT_BOND`
`CurveBuildingDetails` row and the government curve builder:

| Convention Field | Value | Rationale |
| --- | --- | --- |
| `currency_code` | `MXN` | Instruments are selected from Valmer `monedaemision == "MPS"`, the Valmer MXN issue-currency code. |
| `calendar_code` | `Mexico` | Current `msm_pricing` calendar JSON codec accepts `Mexico` for the QuantLib Mexico/BMV calendar. |
| `day_counter_code` | `Actual360` | CETES and M Bonos valuation formulas use actual elapsed days over a 360-day year. |
| `business_day_convention` | source schedule first; otherwise configured Mexico business-day adjustment | SHCP/Banxico says dates are replaced by a banking business day when holidays intervene, but the source text does not fully specify a universal roll convention for every secondary-market valuation. |
| `coupon_period_days` | `182` for M Bonos | M Bonos pay interest every 182 days or the banking business day that substitutes that date. |
| `settlement_days` | `0` for Valmer valuation helpers unless explicitly overridden | Valmer vector prices are as-of valuation observations. Secondary fixed-income settlement in Mexico can be T+0, T+1, T+2, or another agreed cycle, so the bootstrap must not hard-code T+2. |
| `date_generation_rule` | `Backward` for generated fallback schedules | M Bonos series identify maturity; coupon schedules should be generated backward from maturity when source coupon dates are unavailable. |
| `end_of_month` | `False` | The official descriptions use fixed day terms and 182-day periods, not end-of-month scheduling. |

The implementation generates a backward 182-day schedule using the Mexico
calendar and the configured business-day convention. If generated dates cannot
reproduce Valmer accrued interest within tolerance, it fails instead of silently
publishing a curve.

### CETES Standard

CETES are Mexican Federal Treasury Certificates:

- zero-coupon securities
- face value: MXN 10
- usually issued at 28, 91, close to 182, and close to 364 day maturities
- maturity is normally a Thursday or a substitute business day
- no accrued interest before maturity
- market convention supports price, discount-rate, or rate-of-return quoting;
  the technical note's rate-of-return price formula uses actual days over 360

Bootstrap policy:

- build CETES as zero-coupon helpers
- use the Valmer price field explicitly selected by the builder, normalized to
  the helper's face-value convention
- do not create accrued-interest validation for CETES
- if deriving a yield for diagnostics, use the CETES actual-days-over-360
  formula and the exact days from valuation date to maturity

### M Bonos Standard

M Bonos are fixed-rate Federal Government Development Bonds:

- face value: MXN 100
- original terms are multiples of 182 days
- fixed coupon rate determined at issuance
- interest is paid every 182 days or on the banking business day that
  substitutes that date
- coupon interest uses actual elapsed days over a 360-day year
- market convention supports price or yield-to-maturity quoting
- clean price excludes accrued interest; settlement/payment amount adds accrued
  interest for the current coupon period

Bootstrap policy:

- build M Bonos as fixed-rate bond helpers
- use `preciolimpio` as the clean-price quote for `FixedRateBondHelper`
- use `preciosucio` and `interesesacumulados` as validation fields
- validate that clean price plus accrued interest agrees with dirty price after
  applying the configured face-value normalization and tolerance
- compute accrued interest using the M Bonos actual-days-over-360 convention,
  not Actual/Actual or 30/360
- use coupon dates from source when available; otherwise generate a 182-day
  backward schedule from maturity and validate against Valmer accrued interest

## Target Static Pricing Rows

The curve must be represented by core pricing MetaTables, not by constants or
assets.

Recommended static identities:

| Row Type | Identifier | Notes |
| --- | --- | --- |
| `Curve` | `VALMER_MXN_GOVERNMENT_BOND` | Valmer-sourced MXN government bond discount curve |
| `CurveBuildingDetails` | keyed by `VALMER_MXN_GOVERNMENT_BOND.uid` | QuantLib calendar, day count, settlement, compounding, and interpolation build policy |
| `PricingMarketDataSetCurveBinding` | `projection`, `discount`, and `z_spread_base` for CETE index selectors | Runtime CETE policy resolution to the government curve |

The `CurveBuildingDetails` row must encode at least:

```text
builder_type = bond_helper_bootstrap
quote_convention = key_node_quote
rate_unit = key_node_unit
currency_code = MXN
calendar_code = Mexico
day_counter_code = Actual360
settlement_days = 0
coupon_period_days = 182
date_generation_rule = Backward
end_of_month = false
builder_payload.key_node_schema = CurveKeyNode
```

`settlement_days = 0` is specific to this Valmer valuation bootstrap. It must
not be reused as a blanket trading settlement convention for every Mexican
fixed-income workflow.

The `Curve` row uses:

- `curve_type = "discount"`
- `source = "valmer"`
- `interpolation_method = "log_linear_discount"`
- `compounding = "compounded_annual"`
- `quote_side = "mid"`
- metadata only for stable source descriptors, not per-file observations

The `key_nodes` stored with each curve row use the recommended `CurveKeyNode`
provenance shape plus Valmer-specific extensions.
The `quote` field is always the construction input passed into the helper, while
`quote_type` and `quote_unit` define its meaning. Valmer yield is stored
separately as optional decimal `yield` provenance:

```json
{
  "maturity_date": "2026-06-25",
  "asset_identifier": "BI_CETES_260625",
  "instrument_type": "zero_coupon_bond",
  "helper_type": "zero_coupon_bond_helper",
  "quote": 9.87342,
  "quote_type": "clean_price",
  "quote_unit": "price_per_10",
  "quote_side": "mid",
  "quote_source": "preciosucio",
  "source_quote_type": "dirty_price",
  "yield": 0.105,
  "yield_type": "yield_to_maturity",
  "yield_unit": "decimal"
}
```

The Valmer curve TimeIndexTableUpdater attaches a source-specific key-node validator before
the core `DiscountCurvesNode` compresses provenance. The validator enforces the
CETES zero-coupon and M Bonos fixed-rate helper families, quote units, `mid`
quote side, and Valmer yield provenance.

Do not register CETES, M Bonos, or this curve as `Index` rows individually.
The instruments remain `Asset` / bond rows. CETE indexes are real benchmark
selectors; `PricingMarketDataSetCurveBinding` maps those selectors to
`VALMER_MXN_GOVERNMENT_BOND` for z-spread valuation.

## Target TimeIndexTableUpdater Output

Publish the bootstrapped curve through `DiscountCurvesNode`.

The builder output must be a DataFrame indexed or indexable by:

```text
time_index
curve_identifier
```

Required columns:

```text
curve
key_nodes
```

The `curve` value should be the uncompressed curve dictionary expected by
`DiscountCurvesNode`. The node and core codec own compression.
The `key_nodes` value should list the construction instruments and quote
provenance used to build that curve observation.

Example logical output:

```text
time_index                 curve_identifier              curve
2024-09-05T23:59:59Z       VALMER_MXN_GOVERNMENT_BOND    {28: 0.104, 182: 0.102, ...}
```

## Workflow

```text
ImportValmer vector TimeIndexTableUpdater
    |
    v
ValmerVectorPricesStorage
    + ValmerAssetDetailsTable
    |
    v
ValmerMxnGovernmentBondDiscountCurvesNode
    OFFSET_START = 2024-08-30T23:59:59Z
    |
    v
query persisted snapshots from the updater boundary
    |
    v
select bootstrap rows
    |
    +-- CETES:  tipovalor=BI, emisora=CETES, monedaemision=MPS
    |
    +-- M Bonos: tipovalor=M, emisora=BONOS, monedaemision=MPS
    |
    v
build QuantLib helpers
    |
    +-- CETES zero-coupon bond helpers
    |
    +-- M Bonos fixed-rate bond helpers
    |
    v
bootstrap QuantLib discount curve
    |
    v
sample/export strict discount curve points
    |
    v
DiscountCurvesNode
```

## QuantLib Construction Plan

### CETES

CETES rows are zero-coupon instruments.

For each selected CETES row:

- parse valuation date from `fecha`
- parse maturity date from `fechavcto`
- normalize the market quote to the helper's face-value convention; CETES
  official face value is MXN 10, while curve points should be comparable to
  standard rate/discount output
- use `preciosucio` or the explicitly selected quote field as the market quote,
  but document the selected field in the code
- construct a QuantLib zero-coupon bond or equivalent bond helper
- reject rows with non-positive price, missing maturity, or maturity on or
  before valuation date
- validate diagnostic yield or discount rate with actual days over 360 when the
  implementation derives rates from price

The implementation must document the chosen price convention. If QuantLib helper
construction expects clean price, the code must convert or select the matching
Valmer quote explicitly instead of relying on implicit assumptions.

### M Bonos

M Bonos rows are fixed-rate coupon bonds.

For each selected M Bono row:

- parse valuation date from `fecha`
- parse issue date from `fechaemision` when needed
- parse maturity date from `fechavcto`
- parse coupon from `tasacupon`
- parse coupon frequency from `freccpn`
- apply the Mexican government bond calendar, settlement, day count, and
  business-day convention from `IndexConventionDetails`
- construct a QuantLib `FixedRateBond` and bond helper
- use `preciolimpio`, `preciosucio`, and `interesesacumulados` consistently
  with the selected helper quote convention
- use `Actual360` for coupon accrual and accrued-interest validation
- use 182-day coupon periods, not generic calendar six-month coupon periods,
  unless a source schedule explicitly proves the dates

Recommended initial convention:

- use clean price for `FixedRateBondHelper` if QuantLib expects clean prices
- retain dirty price and accrued interest for validation and diagnostics
- fail if clean price plus accrued interest materially disagrees with dirty
  price after applying the chosen convention

## Strictness Rules

The bootstrap must fail fast for bad inputs instead of silently dropping large
parts of the curve.

Required validation:

- all rows for one curve build have the same valuation date
- no unsupported currency enters the helper set
- no duplicate instrument helper is built for the same `unique_identifier`
- maturity dates are strictly after valuation date
- coupon-bearing instruments have coupon and frequency
- selected instruments produce at least one CETES helper and one M Bonos helper
  for the initial full curve build
- curve pillars are strictly increasing after QuantLib helper construction
- interpolation policy comes from `Curve.interpolation_method`
- no extrapolation is enabled unless the core pricing convention explicitly
  allows it
- CETES helpers use zero-coupon semantics and do not accrue interest
- M Bonos helpers use clean price, fixed coupons, 182-day periods, and
  `Actual360` accrual
- generated M Bonos schedules must reproduce Valmer accrued interest within
  tolerance before the curve is published

Exact duplicate maturity pillars are deduplicated before QuantLib bootstrap by
preferring CETES over M Bonos for that maturity. The resulting helper set still
must have strictly increasing pillars. This keeps the curve publishable for real
Valmer snapshots where a CETES row and an M Bono row can share the same
maturity.

If a source snapshot has only CETES or only M Bonos, the builder fails with a clear
"insufficient bootstrap instruments" error. There is no partial-curve
publication mode.

## Relationship To Asset Registration

This ADR does not change the canonical asset registration rule.

```text
AssetTable.unique_identifier = tipovalor_emisora_serie
AssetTable.asset_type        = bond
```

Government/MXN classification belongs in source selection and static Valmer
asset details. It must not widen `AssetTable`.

```text
AssetTable.uid
    |
    | 1:1
    v
ValmerAssetDetailsTable.asset_uid
    |
    +-- sector
    +-- issue_currency
    +-- security_type
    +-- issuer
```

The curve bootstrap operates directly from normalized vector rows so historical
files can be rebuilt deterministically. It does not require
`ValmerAssetDetailsTable`.

## Implemented Code

Implementation locations:

- `src/valmer_connectors/instruments/mexican_government_bond_curve.py`
  for row selection, source validation, QuantLib helper creation, and curve
  point extraction
- `src/valmer_connectors/instruments/curve_bootstrap.py`
  for `VALMER_MXN_GOVERNMENT_BOND` `Curve`, `CurveBuildingDetails`, and
  explicit `mid` `PricingMarketDataSetCurveBinding` upserts
- `src/valmer_connectors/services/curve_update.py`
  for a service function that runs the project-specific
  `DiscountCurvesNode` subclass with `OFFSET_START = 2024-08-30T23:59:59Z`
  and reads persisted vector storage snapshots
- `src/valmer_connectors/cli/main.py`
  for the CLI command:

```bash
valmer-connectors curves update-mxn-government
```

`build_tiie_irs_mxn_valmer(...)` remains focused on the Valmer IRS MXN TIIE OIS
source.

Valmer does not seed `discount:currency:MXN:mid`. Government discounting is
selected through explicit CETE index-role bindings or an explicit valuation
request, not through an MXN currency fallback.

## Implementation Tasks

- [x] Add the static curve identifier
  `VALMER_MXN_GOVERNMENT_BOND`.
- [x] Extend `curve_bootstrap.py` to upsert the government `Curve`,
  `CurveBuildingDetails`, and explicit `mid`
  `PricingMarketDataSetCurveBinding` rows.
- [x] Encode the `VALMER_MXN_GOVERNMENT_BOND` build details with `MXN`,
  `Mexico`, `Actual360`, `settlement_days = 0`, `coupon_period_days = 182`,
  `date_generation_rule = Backward`, and `end_of_month = false`.
- [x] Add a vector-date extraction helper that mirrors vector price `fecha` to
  end-of-day `time_index` behavior.
- [x] Add `select_mxn_government_bootstrap_instruments(...)` with the
  allow-list:
  `(tipovalor, emisora) in {("BI", "CETES"), ("M", "BONOS")}` and
  `monedaemision == "MPS"`.
- [x] Add validation that `sector == "GUBERNAMENTAL"` when the source column is
  present.
- [x] Add CETES QuantLib zero-coupon helper construction.
- [x] Add M Bonos QuantLib fixed-rate helper construction.
- [x] Add M Bonos accrued-interest validation using clean price, dirty price,
  and `Actual360` coupon accrual.
- [x] Add face-value normalization tests for CETES MXN 10 and M Bonos MXN 100.
- [x] Add strict input validation for missing price, coupon, maturity, duplicate
  instrument, and mixed valuation-date cases.
- [x] Add a `build_mxn_government_curve_frame(...)` builder that returns
  uncompressed curve dictionaries keyed by `time_index` and
  `curve_identifier`.
- [x] Publish through `DiscountCurvesNode`; do not add a custom compressed
  storage table.
- [x] Add a service and CLI command for running the new curve update.
- [x] Build the curve from `ValmerVectorPricesStorage` joined to
      `ValmerAssetDetailsTable`, not from a separate raw artifact read.
- [x] Set the MXN government curve TimeIndexTableUpdater `OFFSET_START` to the first
  available persisted vector snapshot, August 30, 2024 at 23:59:59 UTC.
- [x] Add unit tests for row selection using the local sample shape.
- [x] Add unit tests for CETES helper validation and M Bonos helper validation.
- [x] Add a frame-shape test proving the builder returns
  `time_index`, `curve_identifier`, and `curve`.
- [x] Add a bootstrap test proving the new `Index`, convention, and `Curve`
  definitions are included.
- [x] Update `docs/pricing.md` after implementation with the operational command
  and expected output table.

## Non-Goals

This ADR does not:

- change Valmer asset registration
- change `ValmerVectorPricesStorage`
- price individual bonds
- add Banxico sources
- add real fixing publication
- include non-MXN government bonds
- include all government MXN families immediately
- include BONDES, BPAs, BREMS, or `MC` / `MP` / `MS` Bonos in the first
  bootstrap implementation
- move Valmer source parsing into core `ms-markets`
- hand-author table creation migrations

## Future Extensions

After the first CETES plus `M_BONOS` implementation works, evaluate extending
the bootstrap universe to the remaining government MXN families observed in
Valmer source rows:

- `MC_BONOS`
- `MP_BONOS`
- `MS_BONOS`
- `LD_BONDESD`
- `LF_BONDESF`
- `LG_BONDESG`
- `IM_BPAG28`
- `IQ_BPAG91`
- `IS_BPA182`
- `XR_BREMS`
- `XR_BREMSR`

Each family needs its own explicit QuantLib helper and convention validation.
Do not add them through a broad `sector == GUBERNAMENTAL` catch-all.

## Verification Plan

Do not mark the implementation complete until these checks pass:

- local sample selection returns CETES and `M_BONOS` rows only for the first
  curve universe
- selected rows have `monedaemision == "MPS"`
- selected rows use one valuation `time_index` per emitted curve row
- `Curve.get_by_unique_identifier("VALMER_MXN_GOVERNMENT_BOND")` returns the
  expected curve row
- `CurveBuildingDetails` for `VALMER_MXN_GOVERNMENT_BOND` stores `Mexico`,
  `Actual360`, 182-day coupon periods, and Valmer-specific `settlement_days = 0`
- `PricingMarketDataSetCurveBinding` resolves CETE benchmark selectors to
  `VALMER_MXN_GOVERNMENT_BOND` with `role_key` values `projection`,
  `discount`, and `z_spread_base`, and `quote_side="mid"`
- no `PricingMarketDataSetCurveBinding` row exists for
  `discount:currency:MXN:mid`
- no `Index` row is created for `MXN_GOVERNMENT_BOND`
- the builder emits a frame accepted by `DiscountCurvesNode`
- persisted curve rows decode through the core curve codec
- no code path publishes the government bond curve as asset-indexed data
- no code path adds government bond curve fields to `AssetTable`
- CLI run logs selected instrument counts, skipped rows, curve pillars, and the
  published `time_index`
