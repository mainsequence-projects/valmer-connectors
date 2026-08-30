# Project Status

Last local verification: 2026-08-30
Last complete backend lifecycle verification: 2026-08-21

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
- Full collection is now blocked only because the project-owned `migrations`
  package expected by `tests/test_valmer_migrations.py` is absent; Python resolves
  an unrelated installed package named `migrations` instead.
- Managed Main Sequence skills and `AGENTS.md` were refreshed on 2026-08-30 and
  are pinned to SDK `8.0.4` plus the authenticated platform manifest. The local
  CLI authentication and exact Git-derived CodeRepositoryBranch context now
  resolve successfully.
- No SDK 8 commit, tag, push, ResourceRelease, or backend updater-lifecycle
  success is claimed; the backend worktree still contains extensive unreviewed
  changes and canonical sync would stage all of them.

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
