# valmer-connectors

`valmer-connectors` extends `mainsequence` with Valmer market data for Mexican
fixed income. The repository ingests Valmer vector artifacts from MainSequence
artifact storage, registers or reuses Valmer bond assets, attaches pricing
details through `msm_pricing`, and publishes a Valmer TIIE 28 curve from the
public Valmer MexDer benchmark CSV.

## What The Project Does

- Reads historical Valmer vector files from a MainSequence artifact bucket and
  publishes them as `vector_de_precios_valmer`.
- Builds or reuses MainSequence `Asset` objects keyed as
  `tipovalor_emisora_serie`.
- Attaches `msm_pricing` pricing details for the supported Mexican bond
  universe.
- Registers Mexican TIIE/CETE index identities, pricing conventions, and the
  `VALMER_TIIE_28` curve identity through `src/valmer_connectors/instruments/bootstrap.py`.
- Publishes the Valmer TIIE curve through the canonical
  `msm_pricing.data_nodes.DiscountCurvesNode` path.
- Includes a project-specific multipage Streamlit dashboard under
  `dashboards/valmer_monitor/`.

## How Vector Rows Become Assets

The pricing-hydration path is centered on
`src/valmer_connectors/instruments/vector_to_asset.py` and is triggered from
`ImportValmer.get_asset_list()` in `src/valmer_connectors/data_nodes/nodes.py`.

At a high level, the flow is:

1. Valmer source files are normalized and a stable asset key is built as
   `tipovalor_emisora_serie`.
2. The latest row per asset is filtered down to the supported bond universe.
3. `get_instrument_conventions(...)` chooses the market conventions and
   `build_qll_bond_from_row(...)` converts the Valmer row into a
   `msm_pricing.instruments` bond object.
4. Missing assets are registered and supported rows receive current pricing
   details through `msm_pricing.api.instruments.persist_current_pricing_details(...)`.

The important consequence is that not every row in the source vector becomes a
priced asset. The source table is broader than the supported instrument-mapping
surface.

## How To Extend The Mapping

To extend the current vector-to-asset path, change the smallest layer that owns
the behavior:

- Add or correct a vendor benchmark mapping in `src/valmer_connectors/settings.py` via
  `SUBYACENTE_TO_INDEX_MAP`.
- Expand the set of rows that should receive pricing details in
  `ImportValmer._get_target_bonds(...)`.
- Add a new currency or market convention in
  `get_instrument_conventions(...)`.
- Add a new instrument-construction rule in `build_qll_bond_from_row(...)`.
- Validate the change with `run_price_check(...)`,
  `build_position_from_sheet(...)`, and `python scripts/validate_runtime.py`.

The detailed extension guide lives in `docs/instruments.md`.

## Quickstart

### Requirements

- Python 3.11 or newer
- A working MainSequence environment
- Access to the Valmer artifact bucket and to the Valmer benchmark CSV endpoint

### Install

```bash
pip install -e .
# or
uv pip install -e .
```

### Run The Valmer Vector Import

```bash
python scripts/update_vector_valmer.py
```

### Run The TIIE Zero Curve Build

```bash
python scripts/update_tiie_zero_curve.py
```

### Validate The Pricing Runtime

```bash
python scripts/validate_runtime.py
```

## Documentation

Authoritative project documentation lives under `docs/` and is organized
for MkDocs through `mkdocs.yml`.

- `docs/index.md`: documentation entry point and navigation
- `docs/introduction.md`: project overview; this page intentionally mirrors the
  README
- `docs/deployment.md`: deployment sequence, verification commands, and backend follow-up
- `docs/data-nodes.md`: Valmer DataNode definitions and stored fields
- `docs/markets.md`: MainSequence assets and market-side pricing reference data
- `docs/instruments.md`: vector-to-asset flow, `msm_pricing` bootstrap, and
  extension points
- `docs/dashboards.md`: dashboards currently shipped by the project
- `astro/tasks.md`: current open tasks only
- `astro/journal.md`: historical implementation and failure log

## Current Scope

This repository currently does not create:

- MainSequence portfolios
- asset translation tables
- fixing-rate ETL builders owned by this repo

The repository now includes:

- `scheduled_jobs.yaml` for repo-managed ETL scheduling
- `scripts/validate_runtime.py` for runtime validation
- a project-specific dashboard overview plus source, pricing, and curve pages

For deployment verification and current backend follow-up, see
`docs/deployment.md`.

## Key Entry Points

- `scripts/update_vector_valmer.py`: artifact-backed Valmer vector refresh
- `scripts/update_tiie_zero_curve.py`: TIIE 28 discount-curve refresh
- `scripts/validate_runtime.py`: runtime smoke test for pricing MetaTable rows
- `src/valmer_connectors/data_nodes/nodes.py`: Valmer source nodes
- `src/valmer_connectors/instruments/vector_to_asset.py`: instrument mapping and pricing-detail construction
- `src/valmer_connectors/instruments/bootstrap.py`: current ms-markets pricing bootstrap
- `src/valmer_connectors/instruments/rates_curves.py`: Valmer TIIE curve builder
- `dashboards/valmer_monitor/app.py`: dashboard overview entry point
- `scheduled_jobs.yaml`: canonical job schedule definitions for the two ETL runners
