# ADR 0008: USD/MXN Cross-Currency Discount Curve

## Status

Implemented with Valmer source adaptation in this repository and generic
cross-currency helper reconstruction in `msm_pricing`.

## Date

2026-07-02

## Success Criteria

This ADR defines the implementation contract for the USD/MXN F-TIIE/SOFR
cross-currency curve. The implementation must:

- keep Valmer source parsing, source row selection, quote scaling, tenor
  normalization, and source provenance in this repository
- consume the FX and cross-currency rows from Valmer `IRS_MXN_CURVE.csv`
- reuse the same Valmer benchmark valuation-date source used by the TIIE and
  USD SOFR builders
- solve and publish a USD-collateralized MXN discount curve as
  `VALMER_MXN_USD_COLLATERAL_DISCOUNT`
- use ms-markets `rate_helpers@v1` reconstruction for QuantLib
  `FxSwapRateHelper` and `ConstNotionalCrossCurrencyBasisSwapRateHelper`
- use constant-notional CCS helpers by default, not MtM resettable helpers
- keep USD/MXN quote, FX-point, basis-side, basis-sign, collateral, payment,
  and odd-tenor conventions fixed as stated in this ADR
- publish final zero-rate observations through the existing
  `DiscountCurvesNode` storage path
- persist FX/CCS helper provenance as generic ms-markets `rate_helpers@v1`
  cross-currency context and helper key nodes plus Valmer-specific extension
  fields
- validate FX forwards, CCS helper repricing, curve publication, and runtime
  curve resolution before claiming the curve is production-ready

## Context

`IRS_MXN_CURVE.csv` is already used for the domestic Valmer TIIE OIS curve.
That builder intentionally excludes the source rows that are not domestic MXN
OIS quotes:

- `FX.USD.MXN...`
- `Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR`

Those excluded rows are the source for this ADR. The parser already recognizes
the relevant source families in
`src/valmer_connectors/instruments/rates_curves.py`:

| Family | Current classification | Cross-currency use |
| --- | --- | --- |
| `FX.USD.MXN...` | `fx` | Spot and FX swap point helpers |
| `Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO` | `domestic_ois` | Dependency through `VALMER_TIIE_OVERNIGHT`, not direct xccy key nodes |
| `Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR` | `cross_currency` | CCS basis helpers |

QuantLib itself is not the blocker. The installed QuantLib version is
`1.42.1` and exposes:

- `FxSwapRateHelper`
- `ConstNotionalCrossCurrencyBasisSwapRateHelper`
- `MtMCrossCurrencyBasisSwapRateHelper`

The `msm_pricing` abstraction now owns generic cross-currency helper specs,
key-node parsing, runtime dependency resolution, and curve reconstruction. This
repository remains responsible for adapting Valmer rows into those generic
contracts.

The current fixture-level expectation for the MXN source is:

| Classification | Count |
| --- | --- |
| `fx` | 10 |
| `domestic_ois` | 15 |
| `cross_currency` | 9 |

Therefore, this repository adapts the source data and delegates the generic
reconstruction:

```text
Valmer IRS_MXN_CURVE.csv
    |
    +-- FX spot and FX swap points
    +-- USD/MXN F-TIIE/SOFR CCS basis quotes
    |
    v
msm_pricing rate_helpers@v1 reconstruction
    |
    v
zero-rate curve observations + local provenance key_nodes
    |
    v
msm_pricing DiscountCurvesNode storage
```

## Decision

Build `VALMER_MXN_USD_COLLATERAL_DISCOUNT` from Valmer-owned source rows and
delegate generic helper reconstruction to `msm_pricing.pricing_engine.curves`.

The curve is a normal stored discount curve after publication. Runtime pricing
must be able to read it as final zero-rate curve data even though the original
market helpers were FX swaps and cross-currency basis swaps.

This means:

- `Curve.curve_type = "discount"`
- `Curve.currency_code = "MXN"`
- `Curve.quote_side = "mid"`
- `Curve.source = "valmer"`
- `Curve.interpolation_method = "log_linear_discount"`
- `Curve.compounding = "compounded_annual"`
- `CurveBuildingDetails.builder_type = "rate_helper_curve"`
- `CurveBuildingDetails.quote_convention = "helper_quote"`
- `CurveBuildingDetails.rate_unit = "helper_unit"`
- `CurveBuildingDetails.bootstrap_method = "piecewise_log_linear_discount"`
- `CurveBuildingDetails.builder_payload["helper_schema"] =
  "rate_helpers@v1"`

The source helper provenance uses generic helper tokens such as
`fx_swap_rate_helper` and
`const_notional_cross_currency_basis_swap_rate_helper`. Valmer-specific fields
such as `source_quote`, `source_quote_unit`, `basis_side`, `basis_sign`, and
`notional_style` remain local extensions on top of the generic helper schema.

## Locked Market Standards

These standards are fixed for this project implementation:

| Item | Standard |
| --- | --- |
| FX pair | `USD/MXN`, not `MXN/USD` |
| FX quote meaning | MXN per 1 USD |
| FX point scale | raw point / `10,000` |
| CCS basis side | USD SOFR leg |
| CCS basis sign | positive quote means `SOFR + spread` |
| CCS notional style | constant-notional, not MtM resettable, unless broker data explicitly says otherwise |
| QuantLib FX base currency | USD |
| QuantLib FX quote currency | MXN |
| Collateral curve for MXN discounting | USD SOFR |
| MXN F-TIIE payment frequency | 28D / `EveryFourthWeek` |
| MXN F-TIIE fixing offset | 0D, not 1D |
| Weird tenors `182M`, `364M` | treat as 15Y and 30Y curve pillars, not literal 182-month and 364-month periods |

The valuation date source is also fixed: use the same Valmer English homepage
plus `Indices_Benchmarks` AJAX date flow already used by the TIIE and USD SOFR
curve builders. Do not infer the valuation date from wall-clock time, download
time, or a source tenor.

## Curve Dependencies

The cross-currency curve is not the first curve in the graph. It depends on
already available projection/collateral curves:

| Dependency | Use |
| --- | --- |
| `VALMER_USD_SOFR_OVERNIGHT` | USD SOFR projection curve and USD collateral curve handle |
| `VALMER_TIIE_OVERNIGHT` | MXN F-TIIE projection curve dependency |
| `FX.USD.MXN` spot | USD/MXN spot, MXN per 1 USD |
| `FX.USD.MXN.<tenor>` | FX swap points |
| `Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR` | CCS basis quotes |

The solved output is:

```text
VALMER_MXN_USD_COLLATERAL_DISCOUNT
    -> MXN discount curve under USD SOFR collateral
```

Do not add a generic MXN currency-level discount binding for this curve. A
binding like `discount:currency:MXN:mid` is a hidden mono-curve default: it
would cause unrelated MXN instruments to pick the USD-collateralized curve
solely because their currency is MXN.

The USD-collateralized MXN discount curve is still a canonical published
Valmer curve. Runtime selection must be explicit, either by direct curve
identifier in the valuation request or by a future policy selector that names
the collateralization policy, for example `MXN_USD_COLLATERAL`. Currency alone
is not enough information to choose a discount curve.

The existing index-scoped projection bindings remain unchanged. TIIE selectors
continue to resolve to `VALMER_TIIE_OVERNIGHT`; USD SOFR continues to resolve
to `VALMER_USD_SOFR_OVERNIGHT`.

## Source Quote Normalization

### FX Spot And Points

`FX.USD.MXN` is the spot quote and means:

```text
spot = MXN per 1 USD
```

Forward point rows use raw Valmer points:

```text
forward_points = raw_points / 10000
market_forward = spot + forward_points
```

`FX.USD.MXN.ON` and `FX.USD.MXN.TN` are FX swap point rows. They must not be
treated as TIIE anchors. If ON/TN are included, prefer dated FX swap helpers
because ON/TN are not simple month/year curve tenors.

### CCS Basis Quotes

CCS quote rows are:

```text
Swap.<tenor>.MXN.FTIIE.1D/USD.SOFR.1D.SOFR
```

Normalize the basis quote as a decimal rate:

```text
basis = source_quote / 100
```

The basis is on the USD SOFR leg. A positive quote means:

```text
USD leg coupon = SOFR + basis
```

Do not flip the sign for the MXN leg. Do not move the basis to the MXN leg.

### Tenors

Standard `D`, `W`, `M`, and `Y` tokens should retain their source meaning
except for the two Valmer odd-tenor conventions in this source:

| Source tenor | Curve pillar |
| --- | --- |
| `182M` | `15Y` |
| `364M` | `30Y` |

This exception belongs to the cross-currency builder. Do not reinterpret the
existing domestic TIIE OIS curve unless that curve is explicitly refactored.

## QuantLib Helper Mapping

Valmer key nodes are converted by
`msm_pricing.pricing_engine.curves.reconstruct_curve_result_from_key_nodes(...)`
using `helper_schema = "rate_helpers@v1"` and a
`StaticRateHelperRuntimeResolver` that supplies the USD SOFR collateral curve,
USD SOFR index, and MXN F-TIIE index.

### FX Swap Helpers

The generic ms-markets helper spec builds:

```text
ql.FxSwapRateHelper
```

Target fields:

| QuantLib field | Value |
| --- | --- |
| `fwdPoint` | raw FX point / `10000` |
| `spotFx` | `FX.USD.MXN` spot |
| `tenor` | normalized tenor; ON/TN use dated helpers |
| `fixingDays` | 2 |
| `calendar` | joint Mexico and United States settlement calendar |
| `convention` | `ModifiedFollowing` |
| `endOfMonth` | `False` |
| `isFxBaseCurrencyCollateralCurrency` | `True` |
| `collateralCurve` | USD SOFR curve handle |

The FX forward repricing check is:

```text
F_model = spot * DF_USD_SOFR / DF_MXN_USD_COLLATERAL
F_market = spot + raw_points / 10000
```

`F_model - F_market` must be within the configured curve tolerance for each
included FX helper.

### Constant-Notional CCS Helpers

The generic ms-markets helper spec builds:

```text
ql.ConstNotionalCrossCurrencyBasisSwapRateHelper
```

Do not use `ql.MtMCrossCurrencyBasisSwapRateHelper` unless the source is
changed to an explicitly MtM-resettable quote set.

Target fields:

| QuantLib field | Value |
| --- | --- |
| `basis` | source quote / `100` |
| `tenor` | normalized tenor |
| `fixingDays` | 0 |
| `calendar` | joint Mexico and United States settlement calendar |
| `convention` | `ModifiedFollowing` |
| `endOfMonth` | `False` |
| `baseCurrencyIndex` | USD SOFR index |
| `quoteCurrencyIndex` | MXN F-TIIE index |
| `collateralCurve` | USD SOFR curve handle |
| `isFxBaseCurrencyCollateralCurrency` | `True` |
| `isBasisOnFxBaseCurrencyLeg` | `True` |
| `paymentFrequency` | `EveryFourthWeek` for the MXN F-TIIE payment convention |
| `paymentLag` | `0` |

The MXN F-TIIE side must use the 28D payment convention and 0D fixing offset.
This is separate from the domestic OIS helper's overnight construction.

## DataNode Output

The implementation should publish through the same storage class used by the
other Valmer curves:

```text
DiscountCurvesNode(
    CurveConfig(curve_unique_identifier="VALMER_MXN_USD_COLLATERAL_DISCOUNT")
)
```

The output frame must contain one row per valuation date:

| Column | Requirement |
| --- | --- |
| `time_index` | Valmer `Indices_Benchmarks` date at UTC start of day |
| `curve_identifier` | `VALMER_MXN_USD_COLLATERAL_DISCOUNT` |
| `curve` | bootstrapped zero-rate nodes, not source par/basis quotes |
| `key_nodes` | `rate_helpers@v1` context/helper nodes plus Valmer source provenance for spot, FX swap, and CCS source helpers |
| `metadata_json` | leave unset unless row-level metadata is required by storage |

`key_nodes` must not include domestic TIIE OIS rows as direct xccy helpers.
Those rows enter through the dependent `VALMER_TIIE_OVERNIGHT` curve. The xccy
key nodes should include source identifiers, normalized tenors, source quotes,
normalized helper quotes, helper type tokens, quote side, source file, and
QuantLib helper flags.

## Implementation Tasks

1. Add local constants and curve definition for
   `VALMER_MXN_USD_COLLATERAL_DISCOUNT`.
2. Seed `CurveBuildingDetails` as a `rate_helper_curve`, with
   `builder_payload["helper_schema"] = "rate_helpers@v1"`.
3. Do not seed `discount:currency:MXN:mid`. If a valuation workflow needs the
   USD-collateralized MXN discount curve, wire it through an explicit direct
   curve request or a future collateral-policy selector.
4. Extend the Valmer MXN IRS parser into explicit quote models for FX spot, FX
   swap points, and CCS basis rows.
5. Normalize FX points, CCS basis quotes, and odd tenors exactly as specified
   in this ADR.
6. Build Valmer FX spot, FX swap, and CCS key nodes, then call ms-markets
   `reconstruct_curve_result_from_key_nodes(...)`.
7. Export the solved curve as zero-rate observations through
   `DiscountCurvesNode`.
8. Add a local key-node validator for the new FX/CCS provenance shape.
9. Add a CLI command such as `valmer-connectors curves update-usd-mxn-xccy`
   after the builder and tests are in place.
10. Keep the implementation modular enough that Valmer source parsing remains
    independent from generic ms-markets reconstruction.

## Validation Plan

The implementation must prove these checks:

| Check | Expected evidence |
| --- | --- |
| Parser classification | Source fixture produces the expected FX, domestic OIS, and cross-currency row counts |
| FX quote normalization | `FX.USD.MXN` is spot; point rows divide raw points by `10000` |
| CCS quote normalization | basis rows divide source quote by `100` |
| Basis side/sign | positive CCS quote is applied as `SOFR + spread` on the USD leg |
| Tenor normalization | `182M -> 15Y`, `364M -> 30Y` for xccy pillars |
| Helper family | constant-notional CCS helpers are used; MtM helpers are absent |
| Helper flags | `isFxBaseCurrencyCollateralCurrency=True` and `isBasisOnFxBaseCurrencyLeg=True` |
| FX repricing | model forwards match `spot + points / 10000` within tolerance |
| CCS repricing | QuantLib helper quote errors are within tolerance |
| Curve output | one `DiscountCurvesStorage` row is emitted for the Valmer benchmark date |
| Key-node provenance | key nodes parse as ms-markets `rate_helpers@v1` cross-currency helper/context nodes and retain Valmer source extensions |
| Runtime binding policy | no `discount/currency/MXN/mid` row exists; runtime use is explicit by curve identifier or a future collateral-policy selector |

The implementation is not complete if it only constructs QuantLib helpers in
memory. It is complete when the curve is published, no generic MXN discount
binding is seeded, and pricing code can consume the stored curve through an
explicit discount-curve policy.

## Upstream Promotion Result

The previous upstream promotion criteria are satisfied by ms-markets. Generic
ownership now lives upstream for:

- `FxSwapRateHelper` key-node schema and helper spec
- `ConstNotionalCrossCurrencyBasisSwapRateHelper` key-node schema and helper
  spec
- explicit base/quote currency fields for FX helpers
- collateral-currency flags
- basis-side and basis-sign fields
- fixed payment-frequency/fixing-offset fields for the MXN F-TIIE leg
- safe reconstruction from stored FX/CCS helper key nodes

This curve's `CurveBuildingDetails` therefore uses generic `rate_helper_curve`
semantics. Valmer still owns source family detection, source quote scaling,
tenor normalization, source provenance, and USD/MXN-specific validation.

## Consequences

This keeps the source-specific USD/MXN Valmer decisions inside
`valmer_connectors` while avoiding a local copy of generic QuantLib helper
construction. Runtime pricing still consumes a normal stored discount curve,
and scenario/reconstruction paths can use the same ms-markets helper machinery
as other helper-based curves.
