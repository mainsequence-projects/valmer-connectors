# Project Status

Last local verification: 2026-10-04
Last complete backend lifecycle verification: 2026-08-21

## Dependency And Skill Pins (Verified Locally 2026-10-04)

- Locked and installed: `mainsequence` 9.0.6, `mainsequence-metatable` 0.1.20,
  ms-markets 2.2.5. Declared floors: `mainsequence>=9.0.6,<10`,
  `mainsequence-metatable>=0.1.20,<0.2` and `ms-markets>=2.2.5,<3`. On them
  the full suite passes (307 tests, 39 subtests), Ruff passes, and a wheel
  built from the committed tree contains only `valmer_connectors` and declares
  these floors.
- SDK 9.0.6 never replays a POST, PUT, PATCH or DELETE and bounds read-only
  retries by one timeout budget; PyJWT and cryptography are base dependencies.
- MetaTables 0.1.19 runs one local API per local storage directory: `serve
  --local` publishes `server.json` there, a second `serve --local` on the same
  directory reports the running API and exits, and `--local` and
  `configure_local_client()` work from any checkout without a restart after
  commits. The Local/Hosted switch is gone. 0.1.20 retries timed-out
  runtime-access requests.
- Hosted prerequisite, checked in the MetaTables source and offline on
  2026-10-04: the 0.1.20 client validates `/runtime-context/` with
  `RuntimeContext` (`extra="forbid"`), and a MetaTables API on 0.1.18 or older
  still sends `runtime_switch_available`, `runtime_switch_error` and
  `hosted_environment_target`. A 0.1.18-shaped payload fails with three
  `extra_forbidden` errors
  ([MetaTables issue #33](https://github.com/mainsequence-projects/MetaTables/issues/33)).
  `upgrade_application` and `TimeIndexTableUpdater.run()` read that context
  first, so a push needs the Environment's MetaTables API on 0.1.19 or later
  (`docs/deployment.md`, "Publishing"). Not checked against a hosted API.
- MetaTables registers cascading foreign keys since 0.1.17, and 0.1.18 makes
  write on a table depend only on that table's own grants (0.1.17 also required
  write on every table a cascade modifies). The asset-details key to
  `ms_markets__asset.uid` keeps the `ON DELETE CASCADE` of revision `0001`,
  the only Valmer revision.
- ms-markets 2.1 renamed its `cli` and `command_center` packages to `msm_cli`
  and `msm_command_center` and ships its provider as `msm_migrations`; 2.2.0
  removed `msm_command_center`, the asset-monitor builders, pricing
  `response_mappings` and the `apps.v1` re-export modules. Nothing in this
  repository uses any of them. The ms-markets models the Valmer provider
  references are unchanged since 2.1.1. 2.2.3 to 2.2.5 change only the
  bulk-delete preflight and the serialization of the bulk-action and
  tabular-frame contracts, which this repository does not use.
- Skills are pinned to SDK 9.0.6, MetaTables 0.1.20 and ms-markets 2.2.5.
  Platform skills (`.agents/skills/mainsequence_platform/`) are not installed.
  The managed `AGENTS.md` block matches the SDK 9.0.6 template.
- Since SDK 9.0.5, `code-repository sync` only refreshes dependency files;
  publishing is `git commit` and `git push`, and the single workflow file
  decides what deploys. Every workflow resource declares `tag_regex: null`.

## Deployment Migration Job (2026-10-03, Not Yet Pushed)

- `.mainsequence/workflows/valmer-control-plane-api.yaml` is workflow API
  2.3.0. The API is `kind: fastapi` with
  `source_path: api/valmer_control_plane/main.py` under the unchanged key
  `valmer-control-plane-api`. The `migrate-valmer` Job runs
  `scripts/migrate_valmer.py`, which calls `metatables.upgrade_application` for
  `valmer_connectors.migrations:migration` only. Graph: `image`
  (prepare_image API) -> `migrate` (run_job, image_from image) -> `deploy_api`
  (deploy API, image_from image, needs migrate).
- It is the only workflow file. The sixteen pipeline Jobs moved into it from
  `.mainsequence/workflows/valmer-control-plane-jobs.yaml` (workflow API
  2.1.0, now deleted) with keys and specs unchanged, and no execution step
  references them. Moved declarations are new backend targets: the push
  creates sixteen new Jobs, and the old ones, still bound to the removed file,
  are deleted by a backend running ADR-077 (tdag-django `4a2833f12`) or by
  hand. The list is in `docs/deployment.md`, "Cleanup After The Push".
- The 18-resource file passes, offline at tdag-django `122a395fe`, the
  backend's own `validate_execution`, the 2.3.0 root, resource and spec field
  sets, `_validate_task_schedule` and the Job compute limits. It was not
  validated through the branch `validate-workflow` endpoint: on 2026-10-03 the
  backend at `127.0.0.1:8000` sent an empty reply after 136 seconds, and
  earlier it listed no CodeRepositoryBranch for this repository.
- 2026-10-04: the file is still valid offline against tdag-django
  `5a6c9280b`. The branch `validate-workflow` route was not tried: the backend
  at `127.0.0.1:8000` was hung.
- The local proof below ran on MetaTables 0.1.18. It has not been repeated on
  0.1.20: on 2026-10-04 the backend at `127.0.0.1:8000` was hung and no SDK
  session was saved, so `metatables serve --local` could not start. The task
  is in `.agents/tasks.md`.
- The control plane lists the migration Job but its preflight refuses to launch
  it by hand.
- Local proof on 2026-10-03 against a fresh, isolated MetaTables 0.1.18 SQLite
  runtime (`metatables --local runtime initialize`: `ready`, system revision
  `0010_cascade_targets`, DataSource `22451dae-c994-5937-a6f4-fc378592e955`),
  at commit `75f1342`; later commits change only documentation, tests and the
  workflow file:
  - The runtime held the ms-markets 2.2.2 schema first (revision `0018`, 62
    active catalog tables), standing in for the ms-markets deployment.
  - `metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`:
    Alembic ran `-> 0001, current_schema_baseline`, then `ok: True`,
    `finalized_count: 4`, `active_count: 4`, `reserved_count: 0`,
    `failed_count: 0` (`valmer_connectors__alembic_version`,
    `valmer_connectors__valmerassetdetails`,
    `valmer_connectors__vector_de_precios_valmer`,
    `ms_markets__index_values__t_1d`). `current` printed `0001 (head)`, and all
    66 catalog tables were `active`.
  - `PRAGMA foreign_key_list` and the catalog show
    `fk__valmer_connectors__valmerassetdetails__asset_uid_cc247d13d3` as
    `ON DELETE CASCADE`; the other three keys on Valmer-owned tables stay
    `RESTRICT`. `MetaTable.get_impact(action="delete_rows")` on
    `ms_markets__asset` returned `allowed: True` with no blockers, a
    `cascade_delete` edge from the details table and a `restrict` edge from the
    vector table.
  - `scripts/migrate_valmer.py` after `metatables.configure_local_client()`
    printed `valmer_connectors.migrations:migration: 0001 (already current) on
    22451dae-c994-5937-a6f4-fc378592e955` and exited 0. After `downgrade base`
    (three tables `deleted`, the version table kept), it printed
    `0001 (migrated)`, exited 0, and the four tables were `active` again.
  - Alembic autogenerate comparison of the provider metadata with that schema:
    no differences.

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
- A clean venv with the 0.1.47 wheel, mainsequence-metatable 0.1.18 and
  ms-markets 2.2.2 imports both `msm_migrations` and
  `valmer_connectors.migrations` and has no top-level `migrations`, `banxico`,
  `fred`, `agents`, `cli`, `command_center` or `msm_command_center`. Alembic
  resolves the single head `0001` from the installed package. The version table
  is still `valmer_connectors__alembic_version`.
- Full suite: 305 tests and 39 subtests pass. Ruff passes.
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
  single backend-managed workflow that declares the API and every Job.
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
