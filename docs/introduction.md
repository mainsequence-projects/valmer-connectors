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
  through `msm_pricing.api.instruments.persist_current_pricing_details(...)`.
- Seeds Mexican TIIE/CETE index identities, pricing conventions, and the
  `VALMER_TIIE_28` curve identity.
- Publishes the Valmer TIIE curve through the canonical
  `msm_pricing.data_nodes.DiscountCurvesNode` path.
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
