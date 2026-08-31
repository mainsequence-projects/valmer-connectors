# Python 3.13 And Main Sequence 6 Upgrade

## Status

The local dependency, application, and MetaTable-schema cutover was completed
on 2026-08-21. Full canary execution and platform scheduling remain open.

Verified local state:

- `.venv` uses CPython `3.13.11`;
- `mainsequence==6.0.46`, `ms-markets==0.0.110`, and
  `valmer-connectors==0.1.24` are installed;
- the project declares `mainsequence` without a version pin, keeps
  `ms-markets==0.0.110`, and the lock contains no developer-machine source
  paths;
- `.python-version`, Ruff, VS Code, JetBrains, and the Dockerfile target Python
  3.13;
- `uv.lock` resolves the Python 3.13 package graph, `uv lock --check`
  passes, and `requirements.txt` was regenerated;
- QuantLib, NumPy, pandas, SciPy, PyArrow, `psycopg2`, Streamlit,
  the project CLI, and the native package graph import successfully;
- Ruff passes for `src`, `tests`, and `scripts`;
- MkDocs strict build and `uv build` pass;
- the full suite passes with 248 tests and 39 subtests;
- canonical Index values have no `unit` column, current Index payloads validate,
  and the clean project migration head `0001` targets
  `IndexFormulaDefinitionTable`.

The current upstream SDK is `6.0.46`. ms-markets `0.0.110` declares an
unbounded `mainsequence` dependency, so the project no longer needs a uv
override for stale transitive metadata. The project keeps `mainsequence`
unbounded in `pyproject.toml`, while the lock and exported requirements capture
the tested SDK version for reproducible environments.

## Success Condition

The upgrade is complete only when:

- the repository has one strict CPython 3.13 contract and no 3.11 fallback;
- Main Sequence 6 and the selected ms-markets version resolve from immutable,
  deployment-available sources;
- all Index identities use the Main Sequence 6/ms-markets custom-or-formula
  contract;
- canonical Index observations contain no `unit` field;
- TIIE, SOFR, Fed Funds, FX, and cross-currency quote semantics remain explicit
  without adding a compatibility storage column;
- the project migration provider imports only current ms-markets models;
- the complete test suite, Ruff, package build, strict documentation build, and
  CLI/import smoke tests pass from a clean Python 3.13 environment;
- the authenticated Python 3.13 image builds, migrations reach the expected
  heads, and one ordered quote-to-curve canary run succeeds.

No Python-version migration should modify market data. The ms-markets Index
schema migration is separate and must be applied only after its hard
preconditions pass.

## Gap Analysis

| Area | Current state | Required change |
| --- | --- | --- |
| Python | Local environment and repository constraint are 3.13 | Keep the strict `<3.14` upper bound until 3.14 is certified |
| Main Sequence | Registry SDK `6.0.46` is installed from an unbounded direct dependency; ms-markets also leaves the SDK unbounded | Keep the lock and full validation suite green |
| Index identity | Current payloads are explicit custom/formula contracts with provider metadata | Complete live upsert validation after platform login |
| Index values | Producers and reads use the unit-free canonical schema; the clean project baseline is applied | Verify source repopulation and exact-date reads |
| Curve quote reads | Quote semantics are required in `metadata_json` | Verify exact-date live reads and key-node reconciliation |
| Migration metadata | Provider references `IndexFormulaDefinitionTable`; current-only revision `0001` is applied | Keep fresh-schema migration and runtime attachment checks green |
| Tests | 248 tests and 39 subtests pass | Keep this gate green in the release image |
| Managed skills | Main Sequence managed skills match SDK `6.0.46`; ms-markets is `0.0.110` | Refresh only when installed versions change |
| Deployment image | Dockerfile names the Python 3.13 platform image | Authenticate to GHCR and prove pull/build; the anonymous registry check was denied |
| Platform | Local authenticated migrations and source updates execute | Verify images, jobs, scheduled runs, and logs on the target branch |

## Implementation Phases

### 1. Finish The Main Sequence 6 Index Contract

Owning skill: `mainsequence-markets-index-workflow`.

Update all `Index.upsert(...)` payload producers:

- `canonical_index_values.py`;
- `curve_quote_indices.py`;
- `curve_bootstrap.py`.

Every FRED, Banxico, Valmer quote, TIIE, CETE, and SOFR identity published by
application code is a `custom` Index. Set:

- `calculation_method="custom"`;
- `value_format="percent"` for rates and basis spreads whose stored values are
  normalized decimals;
- `value_format="decimal"` for futures prices and FX values;
- a bounded `value_suffix` only when it improves presentation;
- provider, source family, quote type, canonical quote unit, source identifier,
  and quote side in Index metadata.

Remove the obsolete top-level `provider` payload field. Do not create formula
definitions for source-published values.

Gate: focused payload tests prove every Index upsert validates against
`IndexUpsert` and preserves the intended stored numeric value.

### 2. Make Canonical Observations Unit-Free

Owning skills: `mainsequence-markets-index-workflow` and
`mainsequence-data-nodes`.

Change `canonical_index_value_row(...)` and `empty_index_values_frame()` so
their columns exactly match:

```text
time_index
index_identifier
value
definition_uid
observation_status
source_as_of
metadata_json
```

Remove the `unit` argument from FRED, Banxico, and Valmer quote producers.
Custom observations must keep `definition_uid=None`. Preserve raw vendor quote
and vendor unit in observation provenance as `source_quote` and
`source_quote_unit`.

For curve construction:

- remove `unit` from `_QUOTE_COLUMNS` and from the SQL projection;
- validate `source_family`, `quote_type`, source unit, and canonical unit from
  metadata and the registered family contract;
- materialize an explicit in-memory `quote_unit` for curve builders;
- keep `quote_unit` and `source_quote_unit` in curve `key_nodes` audit
  provenance.

The derived `quote_unit` is not a storage compatibility column.

Gate: exact-date quote reads execute against the real
`IndexValuesTS.1d` table shape, and every key-node quote reconciles to the
persisted value and its validated semantic contract.

### 3. Replace Removed Migration Models

Owning skills: `mainsequence-markets-index-workflow` and
`mainsequence-metatable-migrations`.

In `src/migrations/__init__.py` and migration tests:

- replace `IndexCalculationDefinitionTable` with
  `IndexFormulaDefinitionTable`;
- keep `AssetTable`, `IndexTable`, and `IndexFormulaDefinitionTable` as
  reference metadata, not project-owned migration models;
- include `IndexFormulaInputTable` only if a Valmer-owned table gains a direct
  foreign key to it;
- add no old-module shim, compatibility alias, decoder, or automatic
  translation.

The project uses a clean current-schema baseline after ms-markets `0015`. It
does not translate removed calculation-definition tables or provide a
compatibility path.

Gate: both migration providers import, report the expected heads, and the
Valmer provider owns only Valmer tables.

### 4. Rebuild And Validate From A Clean Lock

Owning skill: `mainsequence-project`.

After the code refactor:

```bash
uv lock --check --python 3.13
uv sync --locked --python 3.13
uv export --locked --no-dev --no-hashes \
  --format requirements.txt --output-file requirements.txt
PYTHONPATH=src uv run --python 3.13 --with pytest pytest -q
.venv/bin/ruff check src tests scripts
uv build
```

Also verify:

- package and migration-provider imports;
- `valmer-connectors version` and `valmer-connectors --help`;
- QuantLib, `psycopg2`, NumPy, pandas, SciPy, and PyArrow imports;
- wheel metadata contains `Requires-Python: <3.14,>=3.13`,
  `Requires-Dist: mainsequence`, and
  `Requires-Dist: ms-markets==0.0.110`;
- MkDocs builds in strict mode.

Gate: every command exits successfully. Ignoring a test file is not an
acceptable completion result.

### 5. Make Dependency Sources Release-Safe

Owning skill: `mainsequence-orchestration-and-releases`.

The local path sources were removed. Before an image build:

- resolve the current `mainsequence` release and `ms-markets==0.0.110` from the configured
  package index;
- regenerate and compare the lock and exported requirements in the image
  context;
- prove a fresh checkout can run `uv sync --locked --python 3.13` without
  developer-machine paths.

Gate: lock and requirements contain no developer-machine absolute or relative
checkout dependency required by the image builder.

### 6. Authenticated Platform Rollout

Owning skills: `mainsequence-orchestration-and-releases` and
`mainsequence-metatable-migrations`.

After `mainsequence login`:

1. refresh the token;
2. verify `AGENTS.md` and managed skill provenance record SDK `6.0.46`, ms-markets
   `0.0.104`, and the platform skill manifest;
3. authenticate to the private image registry and verify the Python 3.13 base;
4. build a project image and inspect its Python and package versions;
5. inspect and apply ms-markets migrations, then Valmer migrations;
6. run quote producers, TIIE/SOFR curves, XCCY, and the government curve in
   dependency order;
7. run each producer a second time to prove stable updater identity and no
   duplicate keys;
8. configure the backend-owned CodeRepositoryBranch workflow and jobs against the
   verified image;
9. inspect job configurations, runs, logs, and published rows.

Gate: one complete canary cycle and its immediate no-op rerun succeed before
the Python 3.13 image becomes the scheduled production image.

## Rollback

The interpreter cutover does not require a data rollback. If local
compatibility work fails, restore the previous dependency contract and lock in
a dedicated rollback change and recreate `.venv`; do not add Python 3.11
branches or compatibility modules.

The recreated data source is already at ms-markets `0015` and Valmer `0001`.
Rollback requires a verified database backup/restore; do not add an old-schema
downgrade or compatibility model.

Do not move scheduled jobs to the Python 3.13 image until the canary gate
passes. The previous image remains the operational rollback target.
