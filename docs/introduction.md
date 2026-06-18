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
- Seeds Mexican TIIE/CETE and MXN government benchmark identities, pricing
  conventions, and the `VALMER_TIIE_28` and
  `VALMER_MXN_GOVERNMENT_BOND` curve identities.
- Publishes the Valmer TIIE curve through the canonical
  `msm_pricing.data_nodes.DiscountCurvesNode` path.
- Publishes the Valmer MXN government curve through the same
  `DiscountCurvesNode` path from CETES and M Bonos Vector Analitico rows.
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
- `valmer-connectors curves update-tiie-zero`
- `valmer-connectors curves update-mxn-government`
- `valmer-connectors runtime validate`
- `src/valmer_connectors/data_nodes/nodes.py`
- `src/valmer_connectors/instruments/bootstrap.py`
- `dashboards/valmer_monitor/app.py`

The `scripts/*.py` files remain compatibility wrappers around package services.

## What The Project Does Not Create

This repository does not currently create:

- Main Sequence portfolios
- asset translation tables
- fixing-rate ETL builders owned by this repo

Those gaps remain explicit project boundaries.
