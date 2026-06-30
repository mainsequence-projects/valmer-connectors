# Pricing

This page documents pricing hydration and curve/index runtime behavior. It is
separate from source import, DataNode publication, and static asset details.

## Runtime Bootstrap

The single project bootstrap entry point is:

```text
valmer_connectors.instruments.bootstrap.bootstrap_runtime()
```

It attaches already-migrated runtime tables and seeds static pricing reference
rows. It does not create schemas at runtime. It is also the composition point
for downstream libraries that need additional shared `ms-markets` runtime
models.

```text
bootstrap_runtime()
    |
    v
msm.start_engine(...) or a wrapper over the same shared runtime
    |
    +-- AssetType
    +-- Asset
    +-- IndexType
    +-- Index
    +-- ValmerAssetDetailsTable
    +-- ValmerVectorPricesStorage
    +-- optional extra_markets_models
    |
    v
msm_pricing.bootstrap.attach_pricing_schemas(models=[...])
    |
    v
seed static pricing rows
```

Callers that extend Valmer with portfolio or project-local market tables must
compose those tables into the first Valmer bootstrap call:

```python
from valmer_connectors.instruments.bootstrap import bootstrap_runtime

bootstrap_runtime(
    extra_markets_models=[
        # portfolio, account, signal, or project-local SQLAlchemy models
    ],
)
```

Do not call `msm.start_engine(...)` or `msm_portfolios.start_engine(...)` later
with a different model list in the same process. The shared `ms-markets`
runtime only accepts one startup configuration per process.

Pricing model selectors need precise handling:

- pricing string selectors such as `"Curve"` or
  `"AssetCurrentPricingDetails"` belong to `msm_pricing` resolution and should
  not be passed through `msm_portfolios.start_engine(...)`
- concrete pricing SQLAlchemy classes are valid `MarketsBase` models for the
  shared markets runtime if they must be included there intentionally
- pricing runtime behavior still requires
  `msm_pricing.bootstrap.attach_pricing_schemas(models=[...])`

## Static Pricing Rows

`src/valmer_connectors/instruments/curve_bootstrap.py` attaches the explicit
pricing runtime models needed by this project, then seeds the Mexican
reference-index, convention, and Valmer curve rows required by core
`ms-markets` / `msm_pricing`.

The project uses `attach_pricing_schemas(models=[...])`, not the legacy
`create_pricing_schemas(...)` entry point. Valmer owns the pricing
market-data-set rows and curve-selection bindings it needs, so source storage
bindings and curve bindings are explicit bootstrap work instead of side effects
of schema attachment.

Reference indexes:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`

Pricing convention details:

- stored in `IndexConventionDetails`
- keyed by `index_uid`
- source: `mexico`

`MXN_GOVERNMENT_BOND` is not an `Index`. It is the curve-family identifier in
`VALMER_MXN_GOVERNMENT_BOND`, selected at runtime through explicit
market-data-set curve bindings.

Curve identities:

| Curve | Type | Source | Purpose |
| --- | --- | --- | --- |
| `VALMER_TIIE_28` | `projection` | Valmer MexDer TIIE CSV | TIIE 28 projection curve |
| `VALMER_MXN_GOVERNMENT_BOND` | `discount` | Valmer Vector Analitico | CETES + M Bonos MXN government discount and z-spread base curve |

Both curves use:

- `interpolation_method = "log_linear_discount"`
- `compounding = "compounded_annual"`
- `source = "valmer"`
- `quote_side = "mid"`

Relationship:

```text
+-----------------------------+
| IndexTable                  |
|-----------------------------|
| uid                         |
| unique_identifier           |
| index_type = interest_rate  |
+-----------------------------+
              |
              | index_uid
              v
+-----------------------------+
| IndexConventionDetails      |
+-----------------------------+

+-----------------------------+       +-----------------------------+
| Curve                       |       | CurveBuildingDetails        |
|-----------------------------|       |-----------------------------|
| uid                         |<----->| curve_uid                   |
| unique_identifier           |       | calendar_code = Mexico      |
| curve_type                  |       | compounding/interpolation   |
+-----------------------------+       +-----------------------------+
              ^
              |
              | curve_uid
+-----------------------------+
| PricingMarketDataSetCurveBinding |
|-----------------------------------|
| set_key = default                 |
| role_key                          |
| selector = index:<uid>:mid        |
+-----------------------------+

+-----------------------------+
| PricingMarketDataSetBinding |
|-----------------------------|
| default + discount_curves   |
| default + interest_rate_index_fixings |
+-----------------------------+
```

Valmer seeds the default market-data set and binds it to the canonical
`DiscountCurvesStorage` and `IndexFixingsStorage` tables. Curve selection is
quote-side explicit. Runtime calls must request `mid`; there is no implicit
fallback from an omitted quote side.

Seeded curve-selection bindings:

| Role | Selector | Curve |
| --- | --- | --- |
| `projection` | `index:<TIIE_28.uid>:mid` | `VALMER_TIIE_28` |
| `projection` | `index:<TIIE_91.uid>:mid` | `VALMER_TIIE_28` |
| `projection` | `index:<TIIE_182.uid>:mid` | `VALMER_TIIE_28` |
| `z_spread_base` | `index:<TIIE_28.uid>:mid` | `VALMER_TIIE_28` |
| `z_spread_base` | `index:<TIIE_91.uid>:mid` | `VALMER_TIIE_28` |
| `z_spread_base` | `index:<TIIE_182.uid>:mid` | `VALMER_TIIE_28` |
| `z_spread_base` | `index:<CETE_28.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `z_spread_base` | `index:<CETE_91.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |
| `z_spread_base` | `index:<CETE_182.uid>:mid` | `VALMER_MXN_GOVERNMENT_BOND` |

`CurveBuildingDetails.calendar_code` is `Mexico`, which is the token accepted
by the current `msm_pricing` calendar JSON codec for the Mexico/BMV QuantLib
calendar.

## Bond Pricing Hydration

Bond pricing hydration runs during `ImportValmer.prepare_for_update()`, before
the DataNode run.

```text
prepare_for_update()
    |
    v
_prepare_latest_inputs(...)
    |
    +-- latest Valmer row per unique_identifier
    +-- target-bond subset
    |
    v
_sync_asset_registry_and_pricing(...)
    |
    +-- resolve/upsert AssetTable rows for target bonds
    +-- upsert ValmerAssetDetailsTable rows for registered assets
    +-- decide which target bonds need pricing refresh
    +-- build_qll_bond_from_row(...)
    +-- add_many_pricing_details(...)
```

Pricing hydration writes through:

```text
valmer_connectors.data_nodes.nodes._persist_valmer_pricing_details_batch(...)
    |
    v
msm_pricing.api.add_many_pricing_details(...)
```

It writes timestamped pricing-detail rows and lets `msm_pricing` reconcile the
current pricing projection. With an explicit `pricing_details_date`, the
timestamped row is always upserted, while the current row is updated only when
there is no current row or the incoming date is strictly newer than the current
date. Equal or older dates do not replace current rows.

The Valmer wrapper does not suppress `msm_pricing` write errors. It also treats
an incomplete timestamped result as a hard failure: if
`add_many_pricing_details(...)` returns fewer pricing-detail rows than the
submitted batch, the vector update raises with the submitted Valmer UIDs instead
of silently accepting a partial hydration.

The asset registration universe and current pricing-detail universe are the
same: both use the target-bond subset selected by
`ImportValmer._get_target_bonds(...)`. The vector publication is also scoped to
those asset identifiers so `ValmerVectorPricesStorage.asset_identifier` always
has a matching `AssetTable.unique_identifier`.

The broader source universe is not registered. The Valmer vector contains
multiple instrument types, and this project does not yet own a full Valmer
asset-type classifier.

## Target Bond Selection

Not every Valmer vector row receives pricing details.

Target bond selection is implemented in:

- `ImportValmer._get_target_bonds(...)`

The source vector table is broader than the supported pricing surface. A row
outside the supported pricing surface is ignored by the default vector update.

## Instrument Construction

The instrument adapter is documented in `instruments.md`.

Pricing calls:

```text
build_qll_bond_from_row(...)
```

That adapter converts one normalized Valmer source row into an `msm_pricing`
instrument.

## TIIE Curve Publication

The active TIIE curve publication path is:

```text
valmer-connectors curves update-tiie-zero
    |
    v
bootstrap_runtime()
    |
    v
configure_valmer_discount_curves_cadence()
    |
    v
DiscountCurvesNode(
    CurveConfig(curve_unique_identifier="VALMER_TIIE_28")
)
    |
    v
set_curve_builder(build_tiie_valmer)
    |
    v
run(force_update=True)
```

The curve writes to the canonical `msm_pricing.data_nodes.DiscountCurvesNode`
storage. The old standalone Valmer TIIE curve DataNode is not an active
publication path.

## MXN Government Bond Curve Publication

The Valmer Mexican government bond curve consumes the persisted market snapshot
owned by the vector DataNode. It does not re-read a separate raw Vector
Analitico file during curve publication.

The curve source is:

- `ValmerVectorPricesStorage` for daily price observations
- `ValmerAssetDetailsTable` for the Valmer static fields needed to build
  QuantLib helpers

```text
valmer-connectors curves update-mxn-government
    |
    v
bootstrap_runtime()
    |
    v
configure_valmer_discount_curves_cadence()
    |
    v
ValmerMxnGovernmentBondDiscountCurvesNode
    OFFSET_START = 2026-06-01T00:00:00Z
    |
    v
builder reads ValmerVectorPricesStorage
    joined to ValmerAssetDetailsTable
    |
    v
select_mxn_government_bootstrap_instruments(...)
    |
    +-- CETES:  tipovalor=BI, emisora=CETES, monedaemision=MPS
    |
    +-- M Bonos: tipovalor=M, emisora=BONOS, monedaemision=MPS
    |
    v
build_mxn_government_curve_frame(...)
    |
    v
ValmerMxnGovernmentBondDiscountCurvesNode(
    CurveConfig(curve_unique_identifier="VALMER_MXN_GOVERNMENT_BOND")
)
    |
    v
run(force_update=True)
```

On first publication the DataNode offset boundary starts at June 1, 2026. The
builder queries vector-storage snapshots at or after that timestamp and emits
one curve row per available snapshot. On later runs it queries rows after the
last stored curve observation for `VALMER_MXN_GOVERNMENT_BOND`.

### MXN Government Bootstrap Instrument Contract

The government curve bootstrap uses tradable Valmer asset rows as curve
instruments. It does not use `Index` rows as curve pillars. CETE benchmark
indexes such as `CETE_28` and `CETE_182` only select the published
`VALMER_MXN_GOVERNMENT_BOND` curve for z-spread resolution through
`PricingMarketDataSetCurveBinding`.

Storage-to-builder mapping:

| Builder field | Source | Required for | Notes |
| --- | --- | --- | --- |
| `time_index` | `ValmerVectorPricesStorage.time_index` | all rows | Snapshot timestamp and curve update boundary |
| `unique_identifier` | `ValmerVectorPricesStorage.asset_identifier` | all rows | Valmer asset identifier used for diagnostics and duplicate checks |
| `fecha` | `ValmerVectorPricesStorage.valuation_date` | all rows | Falls back to `time_index` if the valuation date is missing |
| `preciolimpio` | `ValmerVectorPricesStorage.clean_price` | M Bonos | Clean price used by `FixedRateBondHelper` |
| `preciosucio` | `ValmerVectorPricesStorage.dirty_price` | all rows | CETES quote; M Bonos dirty-price consistency check |
| `interesesacumulados` | `ValmerVectorPricesStorage.accrued_interest` | M Bonos | Required for clean + accrued = dirty validation |
| `diastransccpn` | `ValmerVectorPricesStorage.days_since_coupon` | M Bonos when present | Used to validate Actual/360 accrued interest when available |
| `valornominalactualizado` | `ValmerVectorPricesStorage.adjusted_face_value` | not consumed by helpers | Selected for source parity and future use |
| `tipovalor` | `ValmerAssetDetailsTable.security_type` | all rows | Bootstrap allow-list accepts `BI` and `M` |
| `emisora` | `ValmerAssetDetailsTable.issuer` | all rows | Bootstrap allow-list accepts `CETES` and `BONOS` |
| `serie` | `ValmerAssetDetailsTable.series` | all rows | Used in fallback instrument identity |
| `sector` | `ValmerAssetDetailsTable.sector` | all rows when present | Must be `GUBERNAMENTAL` when populated |
| `fechaemision` | `ValmerAssetDetailsTable.issue_date` | M Bonos | Start date for the generated fixed-rate schedule |
| `fechavcto` | `ValmerAssetDetailsTable.maturity_date` | all rows | Pillar maturity; must be after valuation date |
| `valornominal` | `ValmerAssetDetailsTable.face_value` | optional | CETES defaults to `10`; M Bonos default to `100` |
| `monedaemision` | `ValmerAssetDetailsTable.issue_currency` | all rows | Must be `MPS` |
| `freccpn` | `ValmerAssetDetailsTable.coupon_frequency` | M Bonos | Must parse to `182` days |
| `tasacupon` | `ValmerAssetDetailsTable.coupon_rate` | M Bonos | Coupon rate, normalized from percent to decimal when needed |

CETES rows are selected with
`tipovalor = BI`, `emisora = CETES`, and `monedaemision = MPS`. They are built
as QuantLib zero-coupon bond helpers. `preciosucio` is the market quote, and
`valornominal` defaults to `10` when Valmer details do not provide it.

M Bonos rows are selected with
`tipovalor = M`, `emisora = BONOS`, and `monedaemision = MPS`. They are built as
QuantLib fixed-rate bond helpers with a Mexico calendar, `Actual360`, a
backward 182-day schedule, clean-price quotes, and `BondPrice.Clean`.
`valornominal` defaults to `100` when Valmer details do not provide it.

`reglacupon` / `coupon_rule` is not currently loaded into the curve-source
frame and is not consumed by the helper builder. If coupon-rule-specific M Bono
scheduling becomes required, the change must add the detail-table field to the
loader, use it in schedule construction, and update the validation tests in the
same patch.

Each emitted curve date must have at least one CETES helper and one M Bonos
helper. Duplicate instrument identifiers fail. Duplicate maturities are reduced
to one pillar, preferring CETES over M Bonos for the same maturity.

The connector builder follows the current `msm_pricing.DiscountCurvesNode`
contract:

```text
time_index
curve_identifier
curve
```

The node is configured with `CurveConfig(curve_unique_identifier=...)`, but the
builder frame uses `curve_identifier`. Do not emit legacy
`curve_unique_identifier` in the builder output.

The emitted `curve` value is an uncompressed dictionary of zero-rate points
keyed by days to maturity. The core `DiscountCurvesNode` storage and curve codec
own compression and persistence.

Example logical output:

```text
time_index                 curve_identifier              curve
2024-08-30T23:59:59Z       VALMER_MXN_GOVERNMENT_BOND    {6: 0.0562, 13: 0.0871, ...}
```

Representative vector snapshots select CETES rows and M Bonos rows for the
supported bootstrap universe. Exact counts vary by valuation date and by the
assets present in `ValmerVectorPricesStorage`.

## What Pricing Does Not Own

Pricing does not own:

- Valmer source file loading
- raw vector table schema
- static Valmer asset descriptors
- AssetTable identity shape
- dashboard rendering

Those are separate project boundaries.
