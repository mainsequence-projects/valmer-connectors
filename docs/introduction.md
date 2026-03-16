# Introduction

`valmer-connectors` extends `mainsequence` with Valmer market data for Mexican
fixed income.

## What The Project Does

- Reads historical Valmer vector files from a MainSequence artifact bucket and
  publishes them as `vector_de_precios_valmer`.
- Builds or reuses MainSequence `Asset` objects keyed as
  `tipovalor_emisora_serie`.
- Attaches pricing details for the supported Mexican bond universe through
  `asset.add_instrument_pricing_details_from_ms_instrument(...)`.
- Registers `ZERO_CURVE__VALMER_TIIE_28` into the discount-curve ETL registry.
- Registers pricing-runtime `IndexSpec` wiring through
  `src/instruments/bootstrap.py`.
- Ships a multipage Streamlit dashboard for source coverage, pricing hydration,
  and curve-health monitoring.

## Main Entry Points

- `scripts/update_vector_valmer.py`
- `scripts/update_tiie_zero_curve.py`
- `scripts/validate_runtime.py`
- `src/data_nodes/nodes.py`
- `src/instruments/bootstrap.py`
- `dashboards/valmer_monitor/app.py`

## What The Project Does Not Create

This repository does not currently create:

- MainSequence portfolios
- asset translation tables
- fixing-rate ETL builders owned by this repo

Those gaps remain explicit in `astro/tasks.md`.
