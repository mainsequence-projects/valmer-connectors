# Project Status

Last local verification: 2026-10-03
Last complete backend lifecycle verification: 2026-08-21

## Dependency And Skill Pins (Verified Locally 2026-10-03)

- Installed: `mainsequence` 9.0.5, `mainsequence-metatable` 0.1.8, ms-markets
  2.0.1. Declared floors: `mainsequence>=9.0.4,<10` (required by MetaTables
  0.1.8) and `mainsequence-metatable>=0.1.8,<0.2`.
- `.agents/skills/metatables/` is pinned to MetaTables 0.1.8. The code follows
  its provider-placement rules (owned module, explicit `script_location` and
  `version_location_prefix`, prefixed version table, wheel lists only owned
  top-level entries) and sets no Timescale policies in contracts or revisions.
- SDK skills are pinned to 9.0.5 and ms-markets skills to 2.0.1, matching the
  installed packages. Platform skills (`.agents/skills/mainsequence_platform/`)
  are not installed.
- The managed `AGENTS.md` block still differs from the SDK 9.0.5 template: it
  says "canonical CodeRepository sync" and lacks the `mainsequence-inference`
  route. Refreshing it requires an explicit `update AGENTS.md` request.
- Since SDK 9.0.5, `code-repository sync` only refreshes dependency files;
  publishing is `git commit` and `git push`. Every workflow declares
  `tag_regex: null`, so every push deploys the API and the Jobs.

## Wheel Packaging (Verified Locally 2026-10-02)

- `valmer-connectors` 0.1.47 installs exactly one top-level import package,
  `valmer_connectors`. The Valmer migration provider is
  `valmer_connectors.migrations:migration`; Banxico and FRED live under
  `valmer_connectors.banxico` and `valmer_connectors.fred`; the repo-level
  `agents/` scaffold is not installed.
- Earlier wheels (through 0.1.46) installed top-level `migrations`, `banxico`,
  `fred`, and `agents`. Next to ms-markets, the Valmer `migrations` files
  overwrote the ms-markets ones, and `import msm.migrations` failed with
  `ImportError: MarketsAlembicVersion` in the marketsanalytics venv.
- A clean venv with the 0.1.47 wheel and ms-markets imports both
  `msm.migrations` and `valmer_connectors.migrations`. Alembic resolves
  `env.py`, the template, and the single head `0001` from the new package. The
  version table is still `valmer_connectors__alembic_version`.
- Full suite: 295 tests and 39 subtests pass. Ruff passes.
- Setuptools copies a stale `build/lib` into new wheels; remove `build/` before
  building a release wheel.
- Not yet verified live: `metatables migrations current` against the backend
  with the new provider reference, and the first Banxico/FRED updater runs
  under their new update hashes.

## SDK 8 Migration Status

- Local runtime: CPython `3.13.11`, Main Sequence `8.0.4`, ms-markets
  `1.0.2`.
- The dependency lock, synchronized virtual environment, exported
  `requirements.txt`, SDK imports, removed legacy SDK module check, Ruff, wheel
  and source builds, and `git diff --check` all pass.
- The project-owned updater surface uses the SDK 7+ time-index-table names and
  the current `_required_output_table` hook expected by the latest ms-markets
  source.
- The published `ms-markets==1.0.2` metadata declares `mainsequence>=8.0.4`.
  The normally resolved environment imports the Valmer updater modules, and
  the suite excluding `tests/test_valmer_migrations.py` passes: 254 tests and
  36 subtests.
- Managed Main Sequence skills and `AGENTS.md` were refreshed on 2026-08-30 and
  are pinned to SDK `8.0.4` plus the authenticated platform manifest. The local
  CLI authentication and exact Git-derived CodeRepositoryBranch context now
  resolve successfully.
- No backend updater-lifecycle success is claimed under SDK 8.

## Command Center Control Plane

- `valmer-connectors` now contains the request-authenticated FastAPI backend,
  operational resource contracts, fail-closed approved-Job actions, and the
  backend-managed Job workflow.
- The separate `ValmerConnectorsMonitor` repository contains the Vite/React UI
  built on Command Center SDK 0.1.15 navigation, layout, resource, action,
  iframe transport, and theme contracts.
- Focused backend validation passes with 11 tests. The static site passes its
  SDK theme audit, five Vitest tests, TypeScript build, and production Vite build.
- The Vite repository now has a live-backend-validated API 2.1.0 static-site
  workflow configured for every synchronized commit and three retained
  revisions. It remains local and has not created a release.
- The backend VS Code compound `Control Plane: API + Vite (local review)`
  starts the development-only API capability wrapper and the sibling Vite SDK
  host. Browser verification passed for the iframe handshake, authenticated
  viewer projection, API CORS boundary, overview, nine-row data-product page,
  and dark/light theme propagation. The production apps remain unchanged.
- Platform inspection currently returns zero indexed backend FastAPI resources
  and zero releases in either repository. A valid backend FastAPI workflow
  cannot be authored until the first reviewed backend sync indexes the resource
  and returns its real public UID, so no deployment is claimed.

## Backend Runtime and Schema Last Verified on 2026-08-21

- MetaTable data source: `3bde59a2-af55-439f-9584-a954b165324c`.
- ms-markets migration provider is current at revision `0015`, with 55 active
  application tables and no failed definitions.
- The Valmer migration provider is current at clean baseline revision `0001`.
  It owns exactly:
  - `valmer_connectors__valmerassetdetails`;
  - `valmer_connectors__vector_de_precios_valmer`;
  - `ms_markets__index_values__t_1d`.
- No project-specific reference-rate storage, runtime compatibility decoder,
  translator, or migration export utility exists.
- `valmer-connectors runtime validate` resolves the four current curve
  definitions and eight current Index/convention definitions.

## Completed Updates (2026-08-21 Snapshot)

All current producers and curves were executed after schema recreation:

- FRED reference rates: 6,814 rows;
- Banxico policy target: 1,819 rows;
- Banxico supported fixings: 26,430 rows;
- Valmer MXN quote snapshot: 34 Index observations;
- Valmer USD quote snapshot: 47 Index observations;
- dependency-backed TIIE curve: one row;
- dependency-backed SOFR curve: one row;
- dependency-backed USD/MXN XCCY curve: one row;
- vector history: 248 source dates from 2024-08-30 through 2025-08-27;
- government curve: 248 rows rebuilt from persisted vector observations.

The government source loader now permits the required governed-query volume,
uses SDK pagination, and rejects truncated results. This fixed the earlier
1,000-row read ceiling before the final government rebuild.

## Persisted-Data Audit (2026-08-21 Snapshot)

`scripts/verify_current_pipeline.py` passed against the local platform data:

- `IndexValuesTS.1d`: 8,633 reference-rate observations and 81 Valmer quote
  observations across 81 quote identities;
- `IndexFixingsStorage`: 26,430 observations across seven indices;
- government vector: 13,083 rows across 248 dates, including 9,029 CETES rows
  across 87 identities and 4,054 M Bonos rows;
- curve storage: 248 government rows plus one TIIE, one SOFR, and one XCCY row;
- key nodes: 15 TIIE, 23 SOFR, 17 XCCY, and 12,761 government;
- all 55 quote-backed source references are typed `index` and resolve to the
  exact-date persisted Index value;
- all 12,761 government source references are typed `asset` and resolve to the
  exact-date persisted vector observation.

The 2026-08-30 verification found that the vector source has since advanced to
16,099 rows across 489 dates through 2026-08-20, while government-curve storage
still covers 248 dates through 2025-08-27. The repeatable verifier now derives
coverage from the governed source dates instead of freezing the 2026-08-21 row
counts. It reports 241 missing government-curve dates until the government
curve refresh catches up.

## Strict Failure Behavior

Vector registration no longer silently omits a pricing target when its current
instrument cannot be built. Missing instrument payloads are included in the
fatal refresh set and the exception reports the source schedule context.

The vendor row `F_BINVEX_24484` contains a schedule that the current pricing
adapter cannot reconcile (76 remaining 28-day coupons with a 2024-11-28
maturity). It is not part of the CETES/M Bonos government curve universe. A
strict broad vector replay now stops on this row until its source schedule is
corrected; no identifier-specific bypass was added.

## Repository Validation Recorded on 2026-08-21

- Focused strict-pricing and government-query tests pass.
- The repeatable persisted-data verification passes.
- Full suite: 246 tests and 39 subtests pass.
- Ruff, `uv lock --check`, and `git diff --check` pass.
- MkDocs is not installed in the project environment, so no documentation-build
  success is claimed.
