# Open Tasks

## Verify the namespaced packages against the backend

- Scope: after the first successful `migrate-valmer` Job run, read
  `metatables migrations current --provider
  valmer_connectors.migrations:migration` (read-only), then run the
  Banxico fixings, Banxico policy, and FRED updaters once. They now register
  under new update hashes because their class import paths changed; retire the
  stale updater records for the old `banxico.*` and `fred.*` import paths.
- Owning skills: `metatables-migrations` and `metatables-time-index-table-updates`.
- Expected output: the provider reports revision `0001` on
  `valmer_connectors__alembic_version`, and each updater continues incrementally
  from its existing output table without a backfill.
- Required evidence: the migration JobRun log, `current` output, updater
  exits, before/after governed row counts, and the updater record list.

## Publish the data API and move its consumers

- Scope: push `adr-0010-valmer-data-api` (ADR 0010). The push creates the
  `valmer-data-api` release and its workload User; add that User to the Valmer
  Team, then confirm the release is named `valmer_data_api` (consumers search
  for that name; `VALMER_DATA_API_RELEASE_NAME` overrides it). Then push the
  consumers' `valmer-data-api` branches: mexicofundcompetition first, then
  marketsanalytics. Their workflows declare `reader` on the `valmer-connectors`
  branch, so each push needs a person who manages the workload and holds
  `reader` on that branch. The Valmer Team grants mexicofundcompetition's three
  portfolio-building Jobs `data_viewer` on
  `valmer_connectors__vector_de_precios_valmer`; marketsanalytics' workload
  drops its roles on the Valmer details and vector tables.
- Owning skills: `mainsequence-command-center-fastapi`, `resource-release` and
  `mainsequence-access-control-and-sharing`.
- Expected output: both consumers deploy with no `valmer-connectors` package,
  and their Valmer screens read through the data API.
- Required evidence: the workflow run with succeeded `deploy_data_api`, the
  release name, `GET /api/v1/datasets/` and one bond search through the API as
  a consumer workload, a request without a credential refused, each consumer's
  `uv.lock` without `valmer-connectors`, and one Valmer screen per consumer
  matching the package-backed release for the same valuation date.

## Publish and verify the two-repository control plane

- Scope: the API 2.3.0 workflow, the only workflow file, validates through
  `POST /api/v1/code-repository-branches/{uid}/validate-workflow/` (2026-10-11).
  Before the push, confirm that the Environment's MetaTables API runs 0.1.35
  (`metatables runtime status`: system revision `0012_table_creator`, nothing
  pending), that its ms-markets deployment runs 2.2.10 and its migration Job
  has succeeded in the target DataSource (the Valmer foreign keys need its
  tables), that the `valmer_connectors` namespace exists and the Valmer Team
  holds Writer on it and on `mainsequence.markets`, and that the gateway
  forwards the platform-signed caller assertion (see "Publishing" and "Teams
  And Namespaces" in `docs/deployment.md`). The push needs a person the
  platform resolves (a linked GitHub account), because every workload
  declares view on the `MetaTables` repository's branch. While the candidate
  image builds, add the workload Users of the seventeen Jobs the push creates,
  `migrate-valmer` first, and of the API to the Team; otherwise
  `migrate-valmer` fails with `namespace_not_writable` and blocks the API.
  Then point `ValmerConnectorsMonitor` at the release by repository and key
  and push its static-site declaration. Navigation placement remains a
  separate explicit human grant.
- Owning skills: `mainsequence-command-center-fastapi`, `resource-release`,
  `static-site`, and `command-center`.
- Expected output: the embedded SDK-native control plane reads current Valmer
  state and an authorized operator can launch one approved Job.
- Required evidence: the validate-workflow response, the
  `metatables runtime status` output read before the push, the ms-markets
  migration JobRun, the Team's members and namespace grants, the workflow
  action's granted `access` entries, the workflow run with a succeeded
  `migrate` step (its log
  line
  `valmer_connectors.migrations:migration: 0001 (migrated) on <DataSource UID>`)
  before `deploy_api`, both ready active revisions, automatic deployment enabled
  with null tag policies and three retained revisions, repository-event
  DeploymentRuns, iframe/theme verification, `/api/v1/control-plane/me`
  through the gateway returning the caller's UID and a request without a
  release credential denied, viewer denial, operator preflight and launch,
  JobRun polling, and application logs.

## Give the pipeline Jobs their source credentials

- Scope: the source Jobs read the Secrets `FRED_API_KEY`, `BANXICO_TOKEN`,
  `VALMER_ONEDRIVE_TENANT_ID`, `VALMER_ONEDRIVE_CLIENT_ID` and
  `VALMER_ONEDRIVE_CLIENT_SECRET`, the Constant `VALMER_ONEDRIVE_DRIVE_ID`, the
  Bucket `Hitorical Valmer Vector Analytico` with the vector Artifacts, and the
  registered MetaTable `dbo.vector_precios_gubernamental_pip_vista`. On
  2026-10-11 Production had none of them; the deployment needs none of them.
  Where a source Job should run, create what it reads and grant its workload
  User by hand, as "Objects Outside MetaTables" in `docs/deployment.md` lists.
  The workflow never declares them. Until then those Jobs, and the scheduled
  standard pipeline, fail at their first source read.
- Owning skill: `mainsequence-access-control-and-sharing`.
- Expected output: every source Job and one scheduled standard-pipeline run
  succeed, and the control plane's preflight for `vector-metatable-refresh`
  passes its one-row probe.
- Required evidence: the grants on each object, the Artifact and MetaTable
  grants, and one succeeded JobRun per source Job.

## Renew the local review host's access token

- Scope: the `ValmerConnectorsMonitor` local review host
  (`src/local-review-host.tsx`) keeps returning the `VITE_LOCAL_FASTAPI_TOKEN`
  that its VS Code launch read from `mainsequence auth token` at startup.
  Development-backend access tokens last five minutes, so after that the local
  API answers 401 until the host restarts. Give the host a fresh token when the
  iframe asks for a credential, without serving tokens to anonymous callers.
- Owning skills: `static-site` and `command-center`.
- Expected output: a local review session keeps reading the overview after its
  first token expires.
- Required evidence: a local review session older than five minutes that still
  reads the overview, and the request that obtained its current token.

## Retire the Jobs of the removed jobs workflow

- Scope: after the push, make sure no Job is still bound to the removed
  `.mainsequence/workflows/valmer-control-plane-jobs.yaml`. A backend
  running ADR-077 deletes them in the push's workflow action; delete any that
  remain by hand (Main Sequence UI or `DELETE /api/v1/jobs/{uid}/`). Their
  JobRuns are deleted with them, so record any run worth keeping before the
  push. The keys and names are in `docs/deployment.md`, "Cleanup After The
  Push". On 2026-10-11 the Production branch had none of these Jobs, so its
  push should delete nothing.
- Owning skill: `mainsequence-orchestration-and-releases`.
- Expected output: one Job per pipeline Job name plus `Valmer migrations`, all
  declared by `valmer-control-plane-api.yaml`, and one standard-pipeline run
  per weekday.
- Required evidence: the workflow action's `deletions` list or
  `report_undeclared_workflow_targets` for the branch, and the branch Job list.

## Correct the invalid `F_BINVEX_24484` source schedule

- Scope: determine the authoritative issuance, maturity, coupon-frequency, and
  remaining-coupon fields for `F_BINVEX_24484`; correct the source or upstream
  mapping; then run the normal vector update without an identifier allowlist or
  dropped-row fallback.
- Owning skill: `valmer-connectors-registering-assets`.
- Expected output: the pricing instrument builds from internally consistent
  current fields and the row is either published normally or rejected by an
  explicit supported classification rule.
- Required evidence: source-field comparison, successful strict vector update,
  current pricing-details readback, and persisted vector observation readback.

## Prove immediate idempotent reruns

- Scope: execute FRED, Banxico policy, Banxico fixings, both quote producers,
  TIIE, SOFR, XCCY, vector history, and government curve a second time using
  their normal updater identities.
- Owning skills: `mainsequence-time-index-table-updates` and
  `mainsequence-markets-index-workflow`.
- Expected output: no duplicate time-series keys and no unexpected replacement
  of current observations.
- Required evidence: successful updater exits, before/after governed row counts,
  duplicate-key checks, and a passing `scripts/verify_current_pipeline.py` run.

## Schedule and inspect the pipeline jobs

- Scope: publish the current backend-managed project workflow for the ordered
  producers and curves, then inspect one scheduled run of every data job.
- Owning skill: `mainsequence-orchestration-and-releases`.
- Expected output: quote producers run before TIIE/SOFR, both upstream curves
  run before XCCY, and the government curve runs after vector publication.
- Required evidence: retrieved workflow configuration, successful run statuses
  and logs, and a passing persisted-data verification after the scheduled batch.
