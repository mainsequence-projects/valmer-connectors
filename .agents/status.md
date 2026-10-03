# Project Status

Last local verification: 2026-10-03
Last complete backend lifecycle verification: 2026-08-21

## Dependency And Skill Pins (Verified Locally 2026-10-03)

- Locked and installed: `mainsequence` 9.0.5, `mainsequence-metatable` 0.1.14,
  ms-markets 2.1.1. Declared floors: `mainsequence>=9.0.5,<10`,
  `mainsequence-metatable>=0.1.14,<0.2` and `ms-markets>=2.1.1,<3`.
- ms-markets 2.1 renamed its `cli` and `command_center` packages to `msm_cli`
  and `msm_command_center` and ships its provider as `msm_migrations`. Nothing
  in this repository imports the old names.
- Skills are pinned to SDK 9.0.5, MetaTables 0.1.14 and ms-markets 2.1.1.
  Platform skills (`.agents/skills/mainsequence_platform/`) are not installed.
  The managed `AGENTS.md` block matches the SDK 9.0.5 template.
- Since SDK 9.0.5, `code-repository sync` only refreshes dependency files;
  publishing is `git commit` and `git push`, and the workflow files decide what
  deploys. Every workflow resource declares `tag_regex: null`.

## Deployment Migration Job (2026-10-03, Not Yet Pushed)

- `.mainsequence/workflows/valmer-control-plane-api.yaml` is workflow API
  2.3.0. The API is `kind: fastapi` with
  `source_path: api/valmer_control_plane/main.py` under the unchanged key
  `valmer-control-plane-api`. The `migrate-valmer` Job runs
  `scripts/migrate_valmer.py`, which calls `metatables.upgrade_application` for
  `valmer_connectors.migrations:migration` only. Graph: `image`
  (prepare_image API) -> `migrate` (run_job, image_from image) -> `deploy_api`
  (deploy API, image_from image, needs migrate).
- `.mainsequence/workflows/valmer-control-plane-jobs.yaml` is unchanged:
  workflow API 2.1.0, sixteen Jobs, outside the graph.
- The file passes tdag-django's own `validate_execution` and 2.3.0 spec field
  sets, run offline. It was not validated through the branch
  `validate-workflow` endpoint: the backend at `127.0.0.1:8000` lists no
  CodeRepositoryBranch for this repository.
- The control plane lists the migration Job but its preflight refuses to launch
  it by hand.
- Local proof against an isolated MetaTables 0.1.14 SQLite runtime:
  - `metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`
    applied revision `0001` (`current` reports `0001 (head)`), then finalization
    failed: `400 POST /meta-tables/finalize-managed/:
    {"code": "cascading_foreign_key_unsupported"}`. The cause is
    `fk__valmer_connectors__valmerassetdetails__asset_uid_cc247d13d3`
    (`ON DELETE CASCADE` to `ms_markets__asset.uid`) in revision `0001`.
    MetaTables refuses any active table that has, or is referenced by, a
    foreign key whose delete or update rule is not `RESTRICT` or `NO ACTION`;
    its PostgreSQL access layer has the same rule.
  - With that foreign key rebuilt as `RESTRICT` in the scratch database only,
    the asset-details and version tables became active; the two time-index
    tables then failed with `foreign_key_target_columns_not_unique`, because
    the referenced ms-markets tables did not exist locally.
  - The ms-markets 2.1.1 provider could not create them: its reservation fails
    with `422 POST /time-index-meta-tables/: secondary index name must be a
    valid PostgreSQL identifier` (`ms_markets__portfolioeventledgerts` derives
    the 70-character name
    `portfolio_identifier_event_identifier_event_revision_record_identifier`),
    and its revision `0018` declares 29 `ON DELETE CASCADE` foreign keys.
  - Expect the hosted `migrate-valmer` Job to fail the same way, which blocks
    the API deployment, until a new Valmer revision replaces the cascading
    foreign key and ms-markets fixes its schema.

## Wheel Packaging (Verified Locally 2026-10-03)

- `valmer-connectors` 0.1.47 installs exactly one top-level import package,
  `valmer_connectors`. The Valmer migration provider is
  `valmer_connectors.migrations:migration`; Banxico and FRED live under
  `valmer_connectors.banxico` and `valmer_connectors.fred`; the repo-level
  `agents/` scaffold is not installed.
- Earlier wheels (through 0.1.46) installed top-level `migrations`, `banxico`,
  `fred`, and `agents`. Next to ms-markets, the Valmer `migrations` files
  overwrote the ms-markets ones, and `import msm.migrations` failed with
  `ImportError: MarketsAlembicVersion` in the marketsanalytics venv.
- A clean venv with the 0.1.47 wheel and ms-markets 2.1.1 imports both
  `msm_migrations` and `valmer_connectors.migrations` and has no top-level
  `migrations`, `banxico`, `fred`, `agents`, `cli` or `command_center`. Alembic
  resolves the single head `0001` from the installed package. The version table
  is still `valmer_connectors__alembic_version`.
- Full suite: 303 tests and 39 subtests pass. Ruff passes.
- Setuptools copies a stale `build/lib` into new wheels; remove `build/` before
  building a release wheel.
- Not yet verified live: the first `migrate-valmer` Job run, and the first
  Banxico/FRED updater runs under their new update hashes.

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
  workflow configured for every pushed commit and three retained
  revisions. It remains local and has not created a release.
- The backend VS Code compound `Control Plane: API + Vite (local review)`
  starts the development-only API capability wrapper and the sibling Vite SDK
  host. Browser verification passed for the iframe handshake, authenticated
  viewer projection, API CORS boundary, overview, nine-row data-product page,
  and dark/light theme propagation. The production apps remain unchanged.
- Platform inspection returned zero indexed backend FastAPI resources and zero
  releases in either repository at the last check, so no deployment is claimed.
  The backend API workflow is now workflow API 2.3.0 and declares the release by
  `source_path`, so it carries no resource UID.

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
