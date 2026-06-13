# valmer-connectors

`valmer-connectors` extends `mainsequence`, `ms-markets`, and `msm_pricing`
with Valmer market data for Mexican fixed income. The repository imports Valmer
vector source rows, registers or reuses Valmer bond assets, stores static Valmer
asset descriptors, publishes source vector observations, hydrates supported bond
pricing details, publishes a Valmer TIIE 28 curve from the public Valmer
MexDer benchmark CSV, and publishes a Valmer MXN government discount curve from
Vector Analitico CETES and M Bonos rows.

## What The Project Does

- Reads historical Valmer vector files from a Main Sequence Artifact bucket or
  a local `DEBUG_ARTIFACT_PATH` file or folder.
- Builds or reuses MainSequence `Asset` objects keyed as
  `tipovalor_emisora_serie`.
- Stores static Valmer asset descriptors in `ValmerAssetDetailsTable`.
- Publishes time-varying Valmer vector observations as
  `vector_de_precios_valmer`.
- Attaches `msm_pricing` pricing details for the supported Mexican bond
  universe.
- Registers Mexican TIIE/CETE index identities, pricing conventions, and the
  `VALMER_TIIE_28` and `VALMER_MXN_GOVERNMENT_BOND` curve identities through
  `bootstrap_runtime()` and
  `src/valmer_connectors/instruments/curve_bootstrap.py`.
- Publishes the Valmer TIIE curve through the canonical
  `msm_pricing.data_nodes.DiscountCurvesNode` path.
- Publishes the Valmer MXN government curve through the same
  `DiscountCurvesNode` path from CETES and M Bonos Vector Analitico rows.
- Includes a project-specific multipage Streamlit dashboard under
  `dashboards/valmer_monitor/`.

## Workflow Boundaries

The vector update flow is explicit:

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

`ImportValmer.get_asset_list()` is only the prepared asset-scope handoff for
the asset-indexed DataNode lifecycle. It does not own registration or pricing
hydration.

The vector update registers and publishes only rows that pass the supported
target-bond instrument-mapping filter. The broader Valmer vector universe is not
registered as `AssetTable` rows because the source file contains multiple
instrument types and this project does not yet own a full Valmer asset-type
classifier.

The public batch API for source rows is
`valmer_connectors.assets.register_valmer_assets_from_rows(...)`. It normalizes
Valmer rows, classifies supported asset types, writes `AssetTable`,
`ValmerAssetDetailsTable`, `AssetSnapshot`, and optionally persists pricing
details through the batch `msm_pricing` machinery. See `docs/markets.md` for
validation rules and extension-library boundaries.

## How To Extend The Mapping

To extend the current vector-to-pricing path, change the smallest layer that
owns the behavior:

- Add or correct a vendor benchmark mapping in `src/valmer_connectors/settings.py` via
  `SUBYACENTE_TO_INDEX_MAP`.
- Expand the set of rows that should receive pricing details in
  `ImportValmer._get_target_bonds(...)`.
- Add a new currency or market convention in
  `get_instrument_conventions(...)`.
- Add a new instrument-construction rule in `build_qll_bond_from_row(...)`.
- Validate the change with `run_price_check(...)`,
  `build_position_from_sheet(...)`, and `valmer-connectors runtime validate`.

Detailed guides:

- `docs/source-import.md`: bucket versus local file import
- `docs/data-nodes.md`: Valmer vector DataNode publication
- `docs/markets.md`: AssetTable, ValmerAssetDetailsTable, and
  extension-library asset registration boundaries
- `docs/pricing.md`: pricing hydration and curve publication
- `docs/instruments.md`: Valmer row-to-instrument mapping

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

### Migration CLI

Run the core ms-markets provider first, then the Valmer project provider:

```bash
mainsequence migrations current --provider msm.migrations:migration
mainsequence migrations upgrade --provider msm.migrations:migration head

mainsequence migrations current --provider migrations:migration
mainsequence migrations upgrade --provider migrations:migration head
```

The Valmer provider uses the top-level `migrations:migration` package exposed
from `src/migrations`.

Do not run `mainsequence migrations revision` during normal setup. Use
`revision` only after changing the Valmer SQLAlchemy table contract and
expecting an in-place Alembic DDL delta. Project table DDL and MetaTable
catalog registration are applied by `mainsequence migrations upgrade` through
the Valmer migration provider. Do not hand-author DDL for built-in ms-markets
tables in this project.

### Project CLI Surface

The package CLI surface is defined in `docs/adr/cli/0004-valmer-connectors-cli-design.md`.
It is installed through the `pyproject.toml` console script:

```text
valmer-connectors = "valmer_connectors.cli.main:main"
```

Current commands:

```bash
valmer-connectors version
valmer-connectors migrations commands
valmer-connectors runtime validate
valmer-connectors copy-valmer-skills --path .
valmer-connectors vector update
valmer-connectors curves update-tiie-zero
valmer-connectors curves update-mxn-government
```

The current `scripts/*.py` files are compatibility wrappers around package
services.

### Agent Skills

Downstream projects that depend on `valmer-connectors` can import the bundled
Valmer-specific Codex skills into their local `.agents/skills/` tree:

```bash
valmer-connectors copy-valmer-skills --path /path/to/host-project
```

The command writes only:

```text
<host-project>/.agents/skills/valmer-connectors
```

It refuses to run against the `valmer-connectors` source checkout so the
library cannot accidentally overwrite its own skill bundle.

### Current Operations

```bash
valmer-connectors runtime validate
valmer-connectors vector update
valmer-connectors curves update-tiie-zero
valmer-connectors curves update-mxn-government
```

## Compatibility Scripts

- `scripts/update_vector_valmer.py`: compatibility wrapper for the Valmer vector
  refresh.
- `scripts/update_tiie_zero_curve.py`: compatibility wrapper for the TIIE 28
  discount-curve refresh.
- `scripts/validate_runtime.py`: compatibility wrapper for runtime validation.

## Documentation

Authoritative project documentation lives under `docs/` and is organized
for MkDocs through `mkdocs.yml`.

- `docs/index.md`: documentation entry point and navigation
- `docs/introduction.md`: project overview and runtime flow
- `docs/source-import.md`: source selection, bucket import, and local debug import
- `docs/data-nodes.md`: Valmer vector DataNode publication boundary
- `docs/markets.md`: AssetTable, ValmerAssetDetailsTable, and
  extension-library asset registration boundaries
- `docs/pricing.md`: pricing hydration, reference indexes, and curve publication
- `docs/instruments.md`: row-to-instrument mapping rules
- `docs/metatable-query-optimization.md`: thin MetaTable projection reads and
  bulk pricing-details persistence behavior
- `docs/agent-skills.md`: importing bundled Valmer Codex skills into host
  projects
- `docs/deployment.md`: deployment sequence, verification commands, and backend follow-up
- `docs/dashboards.md`: dashboards currently shipped by the project
- `docs/SUMMARY.md`: documentation map required by the project instructions
- `.agents/tasks.md`: current open tasks, when that file exists in the checkout
- `.agents/journal.md`: historical implementation and failure log, when that
  file exists in the checkout

## Current Scope

This repository currently does not create:

- MainSequence portfolios
- asset translation tables
- fixing-rate ETL builders owned by this repo

The repository now includes:

- `scheduled_jobs.yaml` for repo-managed ETL scheduling
- `valmer-connectors runtime validate` for runtime validation
- a project-specific dashboard overview plus source, pricing, and curve pages

For deployment verification and current backend follow-up, see
`docs/deployment.md`.
