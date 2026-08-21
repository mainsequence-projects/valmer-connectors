# Python 3.13 And Main Sequence 5 Upgrade Implementation Task

## Status

Local environment cutover was completed on 2026-07-25. The application
compatibility refactor and platform rollout remain open.

Verified local state:

- `.venv` uses CPython `3.13.11`;
- `mainsequence==5.0.1`, `ms-markets==0.0.99`, and
  `valmer-connectors==0.1.23` are installed;
- `pyproject.toml` requires `>=3.13,<3.14` and
  `mainsequence>=5.0.0,<6`;
- `.python-version`, Ruff, VS Code, JetBrains, and the Dockerfile target Python
  3.13;
- `uv.lock` resolves 97 packages for Python `==3.13.*`, `uv lock --check`
  passes, and `requirements.txt` was regenerated;
- QuantLib, NumPy, pandas, SciPy, PyArrow, `pymssql`, `psycopg2`, Streamlit,
  the project CLI, and the native package graph import successfully;
- Ruff passes for `src`, `tests`, and `scripts`;
- MkDocs strict build and `uv build` pass;
- the built wheel reports `Requires-Python: <3.14,>=3.13` and
  `Requires-Dist: mainsequence<6,>=5.0.0`.

The linked local Main Sequence checkout is at tag `v5.0.0.1` and reports
package version `5.0.1`. Main Sequence 5.0.1 is now available from PyPI, but the
development lock still uses the local checkout. The checkout contains
uncommitted work and is not a portable release source.

The full suite is not yet green:

- collection fails because `msm.models.index_calculations` was removed;
- with that migration test excluded, 236 tests and 36 subtests pass and four
  tests fail because canonical `IndexValuesTS` no longer has a `unit` column;
- the same removed column is still projected by
  `queries/curve_quote_indices.py`, so this is a runtime gap, not only stale
  tests.

## Success Condition

The upgrade is complete only when:

- the repository has one strict CPython 3.13 contract and no 3.11 fallback;
- Main Sequence 5 and the selected ms-markets version resolve from immutable,
  deployment-available sources;
- all Index identities use the Main Sequence 5/ms-markets custom-or-formula
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
| Main Sequence | 5.0.1 is installed from a local checkout although 5.0.1 is now on PyPI | Remove the local override and prove registry-only resolution |
| Index identity | Valmer payloads still use the old fields and top-level `provider` | Set `calculation_method`, `value_format`, optional `value_suffix`, and move provider provenance into metadata |
| Index values | Producer helpers still construct `unit`; storage silently omits it | Remove `unit` from the canonical row contract and every producer |
| Curve quote reads | SQL still projects nonexistent `unit` | Read canonical columns only and derive a typed in-memory `quote_unit` from validated quote semantics |
| Migration metadata | Imports removed `IndexCalculationDefinitionTable` | Reference `IndexFormulaDefinitionTable` directly; add no alias or decoder |
| Tests | One collection error and four unit-contract failures | Rewrite tests against the hard new contract and add runtime query coverage |
| Managed skills | SDK is 5.0.1 but managed Main Sequence skills remain pinned to 4.4.32 | Refresh scaffold and dual-source skills together after login |
| Deployment image | Dockerfile names the Python 3.13 platform image | Authenticate to GHCR and prove pull/build; the anonymous registry check was denied |
| Platform | CLI token is invalid | Login, refresh the token, then verify migrations, images, jobs, runs, and logs |

## Implementation Phases

### 1. Finish The Main Sequence 5 Index Contract

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

The ms-markets `0015` migration is a one-way replacement. Before applying it,
prove the old calculation-definition tables contain no definitions requiring
manual reconstruction. If they do, stop and create exact formula definitions
with source MetaTable UIDs; do not infer them.

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
- QuantLib, `psycopg2`, `pymssql`, NumPy, pandas, SciPy, and PyArrow imports;
- wheel metadata contains `Requires-Python: <3.14,>=3.13` and
  `Requires-Dist: mainsequence<6,>=5.0.0`;
- MkDocs builds in strict mode.

Gate: every command exits successfully. Ignoring a test file is not an
acceptable completion result.

### 5. Make Dependency Sources Release-Safe

Owning skill: `mainsequence-orchestration-and-releases`.

The current local-path sources are valid for linked development only. Before an
image build:

- remove the Main Sequence path override and resolve the published 5.0.1
  package from the configured package index;
- replace the sibling ms-markets path with a published or immutable source
  available to the image builder;
- regenerate the lock and exported requirements in that release context;
- prove a fresh checkout can run `uv sync --locked --python 3.13` without
  `/Users/jose/...` paths.

Gate: lock and requirements contain no developer-machine absolute or relative
checkout dependency required by the image builder.

### 6. Authenticated Platform Rollout

Owning skills: `mainsequence-orchestration-and-releases` and
`mainsequence-metatable-migrations`.

After `mainsequence login`:

1. refresh the token;
2. refresh `AGENTS.md` and managed Main Sequence skills together so the pin
   records SDK 5.0.1 and the platform skill manifest;
3. authenticate to the private image registry and verify the Python 3.13 base;
4. build a project image and inspect its Python and package versions;
5. inspect and apply ms-markets migrations, then Valmer migrations;
6. run quote producers, TIIE/SOFR curves, XCCY, and the government curve in
   dependency order;
7. run each producer a second time to prove stable updater identity and no
   duplicate keys;
8. synchronize `scheduled_jobs.yaml` against the verified image;
9. inspect job configurations, runs, logs, and published rows.

Gate: one complete canary cycle and its immediate no-op rerun succeed before
the Python 3.13 image becomes the scheduled production image.

## Rollback

The interpreter cutover does not require a data rollback. If local
compatibility work fails, restore the previous dependency contract and lock in
a dedicated rollback change and recreate `.venv`; do not add Python 3.11
branches or compatibility modules.

Do not apply ms-markets migration `0015` until the hard preflight passes. Once
it is applied, rollback requires a verified database backup/restore because the
migration intentionally has no legacy downgrade.

Do not move scheduled jobs to the Python 3.13 image until the canary gate
passes. The previous image remains the operational rollback target.
