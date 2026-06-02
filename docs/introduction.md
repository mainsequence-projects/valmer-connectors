# Introduction

`valmer-connectors` extends `mainsequence` with Valmer market data for Mexican
fixed income.

## What The Project Does

- Reads historical Valmer vector files from a MainSequence artifact bucket and
  publishes them as `vector_de_precios_valmer`.
- Builds or reuses MainSequence `Asset` objects keyed as
  `tipovalor_emisora_serie`.
- Attaches pricing details for the supported Mexican bond universe through
  `msm_pricing.api.instruments.persist_current_pricing_details(...)`.
- Registers Mexican TIIE/CETE `Index` and `IndexConventionDetails` rows plus
  the Valmer `VALMER_TIIE_28` `Curve` row through
  `src/valmer_connectors/instruments/bootstrap.py`.
- Publishes the Valmer TIIE curve through
  `msm_pricing.data_nodes.DiscountCurvesNode`.
- Ships a multipage Streamlit dashboard for source coverage, pricing hydration,
  and curve-health monitoring.

## Main Entry Points

- `scripts/update_vector_valmer.py`
- `scripts/update_tiie_zero_curve.py`
- `scripts/validate_runtime.py`
- `src/valmer_connectors/data_nodes/nodes.py`
- `src/valmer_connectors/instruments/bootstrap.py`
- `dashboards/valmer_monitor/app.py`

## What The Project Does Not Create

This repository does not currently create:

- MainSequence portfolios
- asset translation tables
- fixing-rate ETL builders owned by this repo

Those gaps remain explicit in `astro/tasks.md`.
