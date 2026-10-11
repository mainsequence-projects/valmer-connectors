# Project Status

Last local verification: 2026-10-11
Last complete backend lifecycle verification: 2026-08-21

## Dependency And Skill Pins (Verified Locally 2026-10-11)

- Locked and installed: `mainsequence` 9.0.22, `mainsequence-metatable` 0.1.34,
  ms-markets 2.2.10, the newest stable releases on PyPI that day. Declared:
  `mainsequence>=9.0.22,<10`, `mainsequence-metatable>=0.1.34,<0.2` and
  exactly `ms-markets==2.2.10`, the release the Markets deployment runs;
  `requires-python` stays `>=3.13,<3.14`. MetaTables 0.1.34 adds fastembed
  (onnxruntime, tokenizers, huggingface-hub) and `mainsequence[mcp]` to the
  lock. On them the full suite passes (339 tests, 1 skipped, 39 subtests) and
  Ruff passes.
- Identity check against the previous lock (9.0.6, 0.1.20, 2.2.5), offline,
  2026-10-11: the provider (package, namespace, version location, head `0001`,
  version table `valmer_connectors__alembic_version`), the six provider
  metadata tables (columns, types, keys, indexes), the owned tables' MetaTable
  identifiers and contract hashes, the updater `update_hash` of eight Jobs'
  nodes, the curve and vector config contracts, and the backend serialization
  of a CETES zero-coupon and an M Bono fixed-rate bond are identical. So is
  `metatables/updaters/configuration.py`, which computes every update hash.
- SDK 9.0.22 requires the caller's `team_uids` and `is_organization_admin` in
  each hosted caller assertion; 9.0.6 accepted only its older exact claim set
  and so rejects what the backend signs now. The API tests sign both claims,
  refuse an assertion without them, and keep an Organization admin outside
  `VALMER_CONTROL_PLANE_OPERATOR_UIDS` a viewer.
- MetaTables 0.1.34's client reads `/runtime-context/` with
  `extra="ignore"` (0.1.20 used `extra="forbid"`), so MetaTables issue #33
  no longer applies. A push needs the Environment's MetaTables API on 0.1.34;
  the hosted Production API reported system revision `0012_table_creator`,
  nothing pending, on 2026-10-11.
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
  references (`AssetTable`, `IndexTable`, `IndexFormulaDefinitionTable`) are
  unchanged through 2.2.10, whose provider head is `0020`. ms-markets talks
  only to the MetaTables API (`MetaTable.execute_operation`), so no workload
  needs access to the MainSequenceMarkets repository's branch.
- Skills are pinned to SDK 9.0.22, MetaTables 0.1.34 and ms-markets 2.2.10.
  Platform skills (`.agents/skills/mainsequence_platform/`) are not installed.
  The managed `AGENTS.md` block matches the SDK 9.0.22 template.
- Since SDK 9.0.5, `code-repository sync` only refreshes dependency files;
  publishing is `git commit` and `git push`, and the single workflow file
  decides what deploys. Every workflow resource declares `tag_regex: null`.

## Deployment Workflow (Not Yet Pushed)

- `.mainsequence/workflows/valmer-control-plane-api.yaml` is workflow API
  2.3.0. The API is `kind: fastapi` with
  `source_path: api/valmer_control_plane/main.py` under the unchanged key
  `valmer-control-plane-api`. The `migrate-valmer` Job runs
  `scripts/migrate_valmer.py`, which calls `metatables.upgrade_application` for
  `valmer_connectors.migrations:migration` only. Graph: `image`
  (prepare_image API) -> `migrate` (run_job, image_from image) -> `deploy_api`
  (deploy API, image_from image, needs migrate), plus one `prepare_image`
  step with `needs: [migrate]` for each of the sixteen pipeline Jobs, all of
  which the API launches.
- Every workload declares `access.branches: [{repository: MetaTables, level:
  view}]`; the API also declares `access.resources` at `edit` on the sixteen
  Jobs. Hosted, the control plane therefore sees only those Jobs and no longer
  lists the migration Job; its preflight still refuses that execution path.
- It is the only workflow file. The sixteen pipeline Jobs moved into it from
  `.mainsequence/workflows/valmer-control-plane-jobs.yaml` (workflow API
  2.1.0, now deleted) with keys and specs unchanged. Moved declarations are
  new backend targets: the push creates sixteen new Jobs, plus
  `migrate-valmer`, and deletes the targets bound to the removed file. On
  2026-10-11 the Production branch had none of those old Jobs, so nothing is
  deleted there.
- 2026-10-11: the committed file is valid through the branch
  `validate-workflow` route (`valid=True`, `document_api_version` 2.3.0), and
  `msapi.py keys` lists 17 Jobs created, the API kept and the 16 old-file Jobs
  deleted, the same as before this upgrade's changes.
- Backend behavior read at tdag-django `f16c42249` (2026-10-11): a Job's
  `prepare_image` step does not hold its new image back.
  `WorkflowExecutionService._prepare` starts each Job's
  `JobImageDeploymentService.prepare` run when it prepares the connected
  group, and that run activates the image and re-enables the schedule once the
  shared commit image is verified, without the `workflow_deployment_allowed`
  gate that release and static-site rollouts use. Every target of a group must
  admit the push, so a Job taken off every-push promotion would make the API's
  group skip later pushes unless its step is removed too.
- Local proof on 2026-10-11 against a fresh, isolated MetaTables 0.1.34 SQLite
  runtime (its own `METATABLES_LOCAL_STORAGE_DIR`, `serve --local --port
  63500` with a scratch configuration from `init --local --configuration`,
  saved SDK session on the development backend `127.0.0.1:8000`; the
  laptop's shared local API on `18473` and its store were not touched;
  `metatables --local runtime initialize`: `ready`, system revision
  `0012_table_creator`, DataSource `1bc7dde9-65d7-5dab-b32a-c0ffe6a08eb2`),
  with the provider code of commit `e161566`, mainsequence 9.0.22 and
  ms-markets 2.2.10:
  - `msm_migrations:migration` was applied first as a local stand-in for the
    ms-markets deployment: `ok: True`, `finalized_count: 62`,
    `failed_count: 0`, `current` `0020 (head) (mergepoint)`.
  - `metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`:
    Alembic ran `-> 0001, current_schema_baseline`, then `ok: True`,
    `finalized_count: 4`, `active_count: 4`, `reserved_count: 0`,
    `failed_count: 0`: `valmer_connectors__alembic_version`,
    `valmer_connectors__valmerassetdetails` and
    `valmer_connectors__vector_de_precios_valmer` in namespace
    `valmer_connectors`, `ms_markets__index_values__t_1d` in namespace
    `mainsequence.markets`, all with provider key
    `valmer_connectors:valmer_connectors`. `current` printed `0001 (head)`, and
    all 66 catalog tables were `active`.
  - `PRAGMA foreign_key_list` shows the asset-details key to
    `ms_markets__asset.uid` as `ON DELETE CASCADE`; the vector key to
    `ms_markets__asset.unique_identifier` and the two Index-values keys stay
    `RESTRICT`.
  - `scripts/migrate_valmer.py`, unchanged, after
    `metatables.configure_local_client()` printed
    `valmer_connectors.migrations:migration: 0001 (already current) on
    1bc7dde9-65d7-5dab-b32a-c0ffe6a08eb2` and exited 0. After `downgrade base`
    (`ok: True`, three tables deleted, the version table kept,
    `failed_count: 0`), it printed `0001 (migrated)` and exited 0; `current`
    printed `0001 (head)` and all 66 catalog tables were `active` again.
  - Alembic `compare_metadata` with the provider's include hooks found no
    difference between `VALMER_TARGET_METADATA` and the migrated SQLite
    schema; a negative control that added one model column reported exactly
    that `add_column`.
  - `MetaTable.get_impact(action="delete_rows")` on `ms_markets__asset`:
    allowed, no blockers, `valmer_connectors__valmerassetdetails` a
    `cascade_delete` edge and `valmer_connectors__vector_de_precios_valmer` a
    `restrict` edge.

## Production Platform State (Read Only, 2026-10-11)

- CodeRepositoryBranch `64338319-bd19-48db-bf14-945d8debb9be` (`main`,
  Environment Production `18595d12-f92a-4960-84cd-0c6798a1ad25`) has no
  workflow runs.
- FastAPI release `584c0bf3-2ce3-41fb-aadf-022d1fa66bf5`, workload User
  `97331098-9f56-4e6d-8366-cb081a58afc2`, backing Job
  `ad475c1d-e79b-4998-b89f-474538db3717`: active revision `41b312ef`, desired
  `33909ecf`. Five redeployments of `1842f93` (v0.1.45) from 2026-10-03 to
  2026-10-09 failed with `runtime_deployment_failed`. `ValmerConnectorsMonitor`
  pins this release UID, which the push keeps.
- No pipeline Job exists on the branch; the only Job is the release's backing
  Job.
- MetaTables (DataSource `ebd9bc7f-5f11-42fa-8961-19e9e66e2196`, TimescaleDB,
  system revision `0012_table_creator`): the namespaces are
  `alpaca-connectors`, `mainsequence-crm`, `mainsequence-sdk-tutorial` and
  `mainsequence.markets`. There is no `valmer_connectors` namespace, no Valmer
  table and no `ms_markets__index_values__t_1d`; `ms_markets__asset` is active
  under `msm:mainsequence.markets`. Writers of `mainsequence.markets` are the
  Teams `ms-markets-production` and `alpaca-connectors-production`.
- There is no Valmer Team.
- Production has none of the Secrets `FRED_API_KEY`, `BANXICO_TOKEN`,
  `VALMER_ONEDRIVE_TENANT_ID`, `VALMER_ONEDRIVE_CLIENT_ID` and
  `VALMER_ONEDRIVE_CLIENT_SECRET`, no Constant `VALMER_ONEDRIVE_DRIVE_ID`, no
  Bucket `Hitorical Valmer Vector Analytico`, and no registered MetaTable
  `dbo.vector_precios_gubernamental_pip_vista`.

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
  starts the development-only API wrapper and the sibling Vite SDK host.
  Browser verification passed for the iframe handshake, authenticated
  viewer projection, API CORS boundary, overview, nine-row data-product page,
  and dark/light theme propagation. That check predates the SDK request
  identity and used the since-removed static local capability.
- Request identity (2026-10-04): `create_app` installs SDK 9.0.6
  `install_request_identity` once per application and routes read the caller
  with `User.get_logged_user()`. PodDeploymentOrchestrator's own
  `require_request_identity` (`3057764`) raised "Application request identity
  is not installed" for the app at `c4925c6`, built by `create_app` and by
  importing `api/valmer_control_plane/main.py` (engine calls stubbed), with
  hosted and with local configuration; it passes for all four after the
  change. Against the development backend `127.0.0.1:8000`, the VS Code
  wrapper under uvicorn answered `/api/v1/control-plane/me` with the UID that
  the backend's `users/me` returns for a `mainsequence auth token` token
  (viewer), and 401 without a token, with only `X-User-UID`, or with the old
  static capability. Not verified: a hosted deployment, and whether the
  Environment's gateway forwards caller assertions.
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
