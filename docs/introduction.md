# Introduction

`valmer-connectors` extends `mainsequence`, `ms-markets`, and `msm_pricing`
with Valmer market data for Mexican fixed income.

## What The Project Does

- Imports Valmer vector source files from either a Main Sequence Artifact bucket
  or a local debug folder.
- Registers or reuses Valmer bond assets keyed as `tipovalor_emisora_serie`.
- Stores static Valmer asset descriptors in `ValmerAssetDetailsTable`.
- Publishes time-varying Valmer vector observations as
  `vector_de_precios_valmer`.
- Hydrates current pricing details for the supported Mexican bond universe
  through `msm_pricing.api.add_many_pricing_details(...)`, which bulk upserts
  timestamped pricing-detail rows and reconciles current rows by strict source
  date.
- Seeds Mexican TIIE/CETE and USD SOFR index identities, pricing conventions,
  Valmer curve identities, curve build details, and explicit `mid`
  market-data-set curve bindings for `VALMER_TIIE_OVERNIGHT`,
  `VALMER_USD_SOFR_OVERNIGHT`, and `VALMER_MXN_GOVERNMENT_BOND`.
- Publishes complete Valmer MXN and USD curve-input quote snapshots as canonical
  daily `Index` observations in `IndexValuesTS.1d`.
- Publishes the Valmer TIIE and USD SOFR curves through named dependency-backed
  `DiscountCurvesNode` implementations that consume those stored observations.
- Publishes the USD/MXN collateral curve from stored XCCY observations and
  same-date stored TIIE/SOFR curve dependencies.
- Publishes the Valmer MXN government curve through the same
  `DiscountCurvesNode` path from CETES and M Bonos Vector Analitico rows.
- Publishes FRED Treasury yields, the Federal Funds target upper limit, and the
  Banco de Mexico policy target through canonical `IndexValuesTS.1d` storage.
- Ships a multipage Streamlit dashboard for source coverage, pricing hydration,
  and curve-health monitoring.

## Main Runtime Flow

```text
valmer-connectors vector update
    |
    v
bootstrap_runtime()
    |
    v
ImportValmer.prepare_for_update()
    |
    +-- import source rows
    +-- filter rows from the last vector observation per asset_identifier
    +-- sync AssetTable rows
    +-- sync ValmerAssetDetailsTable rows
    +-- hydrate supported bond pricing details
    |
    v
ImportValmer.run(force_update=True)
    |
    v
ValmerVectorPricesStorage
```

## Current Entry Points

- `valmer-connectors vector update`
- `valmer-connectors quotes update-irs-mxn`
- `valmer-connectors quotes update-irs-usd`
- `valmer-connectors curves update-tiie-irs-mxn`
- `valmer-connectors curves update-usd-sofr`
- `valmer-connectors curves update-usd-mxn-xccy`
- `valmer-connectors curves update-mxn-government`
- `valmer-connectors fixings update-banxico`
- `valmer-connectors reference-rates update-fred`
- `valmer-connectors reference-rates update-banxico-policy`
- `valmer-connectors runtime validate`
- `src/valmer_connectors/data_nodes/nodes.py`
- `src/valmer_connectors/instruments/bootstrap.py`
- `dashboards/valmer_monitor/app.py`

The `scripts/*.py` files remain compatibility wrappers around package services.

## What The Project Does Not Create

This repository does not currently create:

- Main Sequence portfolios
- asset translation tables

Those gaps remain explicit project boundaries.
