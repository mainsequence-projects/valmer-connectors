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
`create_pricing_schemas(...)` entry point. This avoids default
`PricingMarketDataSet` / `PricingMarketDataBinding` seeding during vector
updates. If this project later overrides default curve or fixing DataNodes, it
must do that explicitly in a separate pricing market-data configuration flow.

Reference indexes:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`
- `MXN_GOVERNMENT_BOND`

Pricing convention details:

- stored in `IndexConventionDetails`
- keyed by `index_uid`
- source: `mexico`

Curve identities:

| Curve | Benchmark Index | Source | Purpose |
| --- | --- | --- | --- |
| `VALMER_TIIE_28` | `TIIE_28` | Valmer MexDer TIIE CSV | TIIE 28 discount curve |
| `VALMER_MXN_GOVERNMENT_BOND` | `MXN_GOVERNMENT_BOND` | Valmer Vector Analitico | CETES + M Bonos MXN government discount curve |

Both curves use:

- `curve_type = "discount"`
- `interpolation_method = "log_linear_discount"`
- `compounding = "compounded_annual"`
- `source = "valmer"`

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
              |
              | index_uid
              v
+-----------------------------+
| Curve                       |
|-----------------------------|
| unique_identifier           |
| VALMER_TIIE_28              |
| VALMER_MXN_GOVERNMENT_BOND  |
+-----------------------------+
```

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

The Valmer Mexican government bond curve uses Vector Analitico rows directly.
It does not run asset registration, asset-detail upserts, vector price storage,
or current bond-pricing hydration.

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
ImportValmer.prepare_source_data()
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
DiscountCurvesNode(
    CurveConfig(curve_unique_identifier="VALMER_MXN_GOVERNMENT_BOND")
)
    |
    v
run(force_update=True)
```

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

The local sample file currently selects 37 CETES rows and 17 M Bonos rows for
the first supported bootstrap universe. Exact counts vary by source file.

## What Pricing Does Not Own

Pricing does not own:

- Valmer source file loading
- raw vector table schema
- static Valmer asset descriptors
- AssetTable identity shape
- dashboard rendering

Those are separate project boundaries.
