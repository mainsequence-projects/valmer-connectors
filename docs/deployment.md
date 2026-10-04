# Deployment

## Publishing

A plain `git push` publishes this repository. `.mainsequence/workflows/` holds
one workflow file, `valmer-control-plane-api.yaml` (workflow API 2.3.0), and it
decides what deploys. It declares the control-plane FastAPI release, the
`migrate-valmer` Job and the sixteen pipeline Jobs. Every resource declares
`tag_regex: null`, so every push to the branch is eligible:

- the execution graph prepares the code repository image, runs the
  `migrate-valmer` Job from it, and only then deploys the API
  (`needs: [migrate]`);
- no step references the pipeline Jobs, so each one redeploys on every push by
  itself (see [Workflow Declarations](#workflow-declarations)).

Push only after the Environment's hosted MetaTables API (the `metatables-api`
release of the MetaTables repository) runs 0.1.19 or later, and after the
ms-markets deployment's migration Job has succeeded in the same DataSource (see
[Migrations](#migrations)). The `mainsequence-metatable` 0.1.20 client in this
image validates the API's `/runtime-context/` response with a model that
forbids unknown fields, and an API on 0.1.18 or older still sends
`runtime_switch_available`, `runtime_switch_error` and
`hosted_environment_target`, which 0.1.19 removed
([MetaTables issue #33](https://github.com/mainsequence-projects/MetaTables/issues/33)).
Against such an API, `migrate-valmer` fails before it migrates anything,
`deploy_api` never runs, and the push deploys no API. The pipeline Jobs still
take the new image, because the push applies them outside the graph, and their
updater runs fail the same way: `TimeIndexTableUpdater.run()` reads the runtime
context first.

To check the API, run `metatables runtime status` without `--local`, after
`mainsequence login`, from a checkout whose branch is registered in that
Environment. It is a read-only request that prints the response without
validating it; an API on 0.1.18 or older lists `runtime_switch_available`.

```bash
git add <reviewed paths>
git commit -m "<specific commit message>"
git push
```

After a dependency change, refresh the dependency files and commit them with
the change:

```bash
mainsequence code-repository sync --path .
```

Since SDK 9.0.5, `code-repository sync` only runs `uv lock`, `uv sync` and
`uv export --locked --no-dev --no-hashes` into `requirements.txt`. It does not
bump the version, tag, commit or push. A pushed commit is not deployment
evidence; inspect the workflow run and its DeploymentRuns.

## Skill And Scaffold Refresh

Run these only when the update is explicitly requested:

```bash
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository update-platform-skills --path .
metatables copy-metatables-skills --path .
msm copy-msm-skills --path .
mainsequence code-repository update AGENTS.md --path .
```

`update-agent-skills` copies the SDK-owned skills offline and replaces
`.agents/skills/mainsequence/`; `update-platform-skills` needs the signed-in
session and installs the platform skills under
`.agents/skills/mainsequence_platform/`.

## Migrations

Hosted environments are migrated only by the `migrate-valmer` Job of
`valmer-control-plane-api.yaml`. It runs `scripts/migrate_valmer.py` from the
candidate image, which calls
`metatables.upgrade_application("valmer_connectors.migrations:migration")` and
prints one line with the revision, whether it migrated, and the DataSource. A
database already at head is left unchanged. A failed Job blocks the API
deployment and the previous release keeps serving; fix the revision and push
again. Redeploying an older image does not roll the schema back.

The Job needs the Environment's hosted MetaTables API on 0.1.19 or later.
`upgrade_application` reads the API's runtime context before it migrates
anything, and the 0.1.20 client rejects the context of an API on 0.1.18 or
older (MetaTables issue #33). The JobRun then fails with
`3 validation errors for RuntimeContext`, `Extra inputs are not permitted` for
`runtime_switch_available`, `runtime_switch_error` and
`hosted_environment_target`; nothing was migrated and the API was not
deployed. Upgrade that API, then push a new commit: a failed workflow run does
not resume, and each commit gets its own run. Do not migrate the Environment by
hand.

The Job applies only the Valmer provider. The ms-markets schema that the Valmer
tables reference is migrated by the ms-markets deployment, and it must be in
the same DataSource first: the Valmer foreign keys need active
`ms_markets__asset`, `ms_markets__index` and `ms_markets__indexformuladefinition`
tables, so `migrate-valmer` fails, and blocks the API, until the ms-markets
deployment's migration Job has succeeded there.

The asset-details key to `ms_markets__asset.uid` is `ON DELETE CASCADE`, as
revision `0001` declares; MetaTables registers cascading foreign keys since
0.1.17.

Do not run `metatables migrations upgrade` or `downgrade` against a hosted
runtime, and do not migrate at application startup. The control plane lists the
migration Job but refuses to launch it by hand. Local development applies the
provider with
`metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`
and authors a revision against that local runtime, at head, with
`metatables --local migrations revision --provider valmer_connectors.migrations:migration --message "<change>"`:
autogenerate compares the models with the runtime that the command selects.

## Workflow Declarations

`.mainsequence/workflows/valmer-control-plane-api.yaml` is the only workflow
file. Besides the API and `migrate-valmer`, it declares the control-plane Jobs
and their platform-owned descriptions. The standard pipeline is the only
scheduled Job; leaf Jobs and source/rebuild variants are manual operations. The
platform catalog deliberately excludes launch configurations that depend on a
developer workstation or exist only to run UI, debugging, examples, or
migration administration.

The pipeline Jobs stay out of `execution.steps` on purpose. The backend
prepares the image of every Job that a step references when it admits the
workflow run, before any `run_job` step, so a step cannot make a schedule wait
for the migration. A Job that no step references keeps its own every-push
redeployment. A scheduled or manual run can therefore start on a new image
before `migrate-valmer` has finished.

The API is declared as `kind: fastapi` with
`source_path: api/valmer_control_plane/main.py`. Workflow API 2.3.0 files carry
no resource, release or Job UIDs. The backend binds each target through the
workflow file path and its resource key (the API also through its release
kind), so neither the file nor a key may be renamed. Validate an edited
workflow against the branch workflow template
(`POST /api/v1/code-repository-branches/{uid}/validate-workflow/`) before
committing it. The release enables every-commit automatic redeployment and
retains three immutable revisions. The backend owns exact image creation; do
not select a caller image for this workflow-owned release.

The Vite repository contains
`.mainsequence/workflows/valmer-control-plane-static-site.yaml`. It uses the
same every-commit promotion policy and retains three revisions. Push the
static repository only after the FastAPI release exists and the installed
Command Center SDK's exact stable release target has been configured. See
`docs/control-plane.md` for the two-repository release order.

## Cleanup After The Push: The Removed Jobs Workflow

Until this release the sixteen pipeline Jobs were declared in
`.mainsequence/workflows/valmer-control-plane-jobs.yaml` (workflow API 2.1.0).
They now live in `valmer-control-plane-api.yaml` with the same keys and the
same settings; only the file and its `api_version` changed. The backend
identifies a workflow target by CodeRepositoryBranch, workflow file path and
resource key, assigned once, and a declaration in another file is a new
target. The push therefore creates sixteen new Jobs with new UIDs and never
reuses the old ones, which stay bound to the removed file.

What happens to the old Jobs depends on the backend that processes the push:

- tdag-django with ADR-077 (`4a2833f12`, on `development` since 2026-10-03)
  deletes them in the same workflow action, after it has created the new ones.
  The action result lists each one under `deletions` as `deleted`, or as
  `deletion_blocked` with a reason. A blocked Job keeps its last image and its
  schedule, and the next push retries the deletion.
- An older backend keeps them. Their image stays at the last commit that
  declared them, and the old `standard-pipeline-refresh` keeps its schedule, so
  the standard pipeline runs twice every weekday (old and new image) until the
  old Job is deleted.

ADR-077 deletes the old Jobs even when `valmer-control-plane-api.yaml` is
invalid, because their file is gone, while an invalid file creates no new Jobs
and deploys nothing. Validate the file through the branch `validate-workflow`
route before the push.

After the push, list the branch's Jobs
(`mainsequence code-repository jobs list --path . --timeout 60`). Exactly one
Job must remain for each display name below, besides `Valmer migrations`.
Delete each older duplicate by hand, in the Main Sequence UI or with
`DELETE /api/v1/jobs/{uid}/`; the SDK CLI has no delete command. A backend
with ADR-077 also has a read-only report of the targets still bound to the
removed file:
`python manage.py report_undeclared_workflow_targets --code-repository-branch-uid <uid>`.

| Old workflow file | Key | Kind | Display name |
| --- | --- | --- | --- |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `vector-refresh` | job | Valmer Vector Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `vector-onedrive-refresh` | job | Valmer Vector Refresh — OneDrive Graph |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `vector-metatable-refresh` | job | Valmer Vector Refresh — MetaTable |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `irs-mxn-quotes-refresh` | job | Valmer IRS MXN Quotes Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `irs-usd-quotes-refresh` | job | Valmer IRS USD Quotes Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `banxico-fixings-refresh` | job | Banxico Fixings Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `banxico-tiie-fixings-refresh` | job | Banxico TIIE Fixings Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `fred-reference-rates-refresh` | job | FRED Reference Rates Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `banxico-policy-refresh` | job | Banxico Policy Target Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `tiie-curve-refresh` | job | Valmer TIIE Curve Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `sofr-curve-refresh` | job | Valmer SOFR Curve Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `xccy-curve-refresh` | job | Valmer USD/MXN XCCY Curve Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `xccy-curve-rebuild` | job | Valmer USD/MXN XCCY Curve Rebuild |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `government-curve-refresh` | job | Valmer MXN Government Curve Refresh |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `standard-pipeline-refresh` | job | Valmer Standard Pipeline Refresh (scheduled `0 13 * * 1-5`) |
| `.mainsequence/workflows/valmer-control-plane-jobs.yaml` | `pipeline-verification` | job | Valmer Pipeline Verification |

What refers to the old identities:

- Nothing in this repository or in `ValmerConnectorsMonitor` stores a Job UID.
  The control plane looks Jobs up on every request by execution path (the
  `JOB_LAUNCH_PROFILES` catalog, and the preflight that refuses
  `scripts/migrate_valmer.py`), so it follows the new Jobs unchanged. While both
  copies exist, its Jobs collection lists each moved Job twice with the same
  name and execution path, and the pipeline view binds each action to whichever
  copy the Jobs query returns last.
- Deleting a Job deletes its JobRuns, because the JobRun foreign key cascades.
  The old Jobs' run history goes with them, and a saved link to one of their
  JobRun UIDs stops resolving. With ADR-077 that happens at the push; record
  any run you still need before pushing.
- Settings changed on an old Job outside the workflow file do not carry over:
  custom environment variables, scheduled command arguments (for example from
  `mainsequence code-repository jobs update`) and the schedule timezone. The
  declarations set none of them, so the new standard pipeline runs at
  `0 13 * * 1-5` in UTC with no arguments.
- The FastAPI release keeps its identity, because its file and key did not
  change, so `ValmerConnectorsMonitor`'s `VITE_FASTAPI_RESOURCE_RELEASE_UID`
  still names the same release. `migrate-valmer` is new in this release and
  has been declared in that file from the start.

## Verification Commands

After the push and image creation, verify the deployed state with:

```bash
mainsequence code-repository current --debug
mainsequence code-repository jobs list --path . --timeout 60
metatables code-repository time-index-table-updates list --timeout 60
mainsequence code-repository resources list --path . --timeout 60
mainsequence code-repository images list --path . --timeout 60
metatables migrations current --provider valmer_connectors.migrations:migration
mainsequence markets portfolios list --timeout 60
valmer-connectors runtime validate
valmer-connectors reference-rates update-fred
valmer-connectors reference-rates update-banxico-policy
valmer-connectors quotes update-irs-mxn
valmer-connectors quotes update-irs-usd
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-usd-mxn-xccy
valmer-connectors curves update-mxn-government
python scripts/verify_current_pipeline.py
```

Use the dashboard after deployment to confirm:

- source node coverage
- pricing hydration
- curve publication health
- external reference-rate coverage and freshness

Use the Vite control plane after both ResourceReleases are ready to confirm:

- SDK iframe initialization and host theme propagation;
- the overview names the Organization Environment of the backend release's
  CodeRepositoryBranch and reports no `Environment:` failure;
- data-product and asset resource pagination;
- viewer read access and viewer launch denial;
- operator Job discovery, preflight, typed confirmation, and execution; and
- the accepted JobRun link after a launch.

For both releases, verify that `automatic_deployment` is enabled, the nested
tag policy is null (every pushed commit), `revision_retention_count` is
three, and `active_revision` advances only after the desired revision is ready.
Inspect the correlated DeploymentRun; a successful Git push is not deployment
evidence.

For the API, read the branch's workflow runs (MCP `code_repository_branch.get`
with `include: "workflow_runs"`) and confirm the `migrate` step's JobRun
succeeded before the `deploy_api` DeploymentRun started. The Job's log has one
line per provider, for example
`valmer_connectors.migrations:migration: 0001 (migrated) on <DataSource UID>`
the first time and `0001 (already current)` afterwards.
`metatables migrations current` only reads the deployed revision.

## MetaTables 0.1.20 Local Migration Check (2026-10-04)

The Valmer provider was applied to a fresh, isolated MetaTables 0.1.20 local
SQLite runtime (system migrations through `0010_cascade_targets`) that already
held the ms-markets 2.2.5 schema (revision `0018`, 62 active tables), as a
hosted DataSource does once the ms-markets deployment has migrated it:

- `metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`
  applied revision `0001` and finalized all four Valmer catalog tables as
  `active` with `failed_count: 0`: `valmer_connectors__alembic_version`,
  `valmer_connectors__valmerassetdetails`,
  `valmer_connectors__vector_de_precios_valmer` and
  `ms_markets__index_values__t_1d`. `migrations current` reports `0001 (head)`,
  and all 66 catalog tables are `active`.
- The asset-details key to `ms_markets__asset.uid` is `ON DELETE CASCADE` in
  the SQLite schema; the other three keys on Valmer-owned tables are
  `RESTRICT`.
- `scripts/migrate_valmer.py`, run through `metatables.configure_local_client()`,
  printed `valmer_connectors.migrations:migration: 0001 (already current) on
  <DataSource UID>`. After `downgrade base`, it printed `0001 (migrated)` and
  the same four tables were `active` again. Both runs exited 0.

The 0.1.18 check on 2026-10-03 also found that `get_impact(action="delete_rows")`
on `ms_markets__asset` lists the asset-details key as a `cascade_delete` edge,
reports the action allowed and has no blockers, and that Alembic autogenerate
finds no difference between the provider metadata and the migrated schema.

SQLite does not exercise PostgreSQL. The hosted `migrate-valmer` Job is
unverified until its first workflow run. See `.agents/status.md` for the
runtime identifiers.

## Current Platform Verification

The local MetaTable data source was rebuilt on 2026-08-21. The ms-markets
provider is at revision `0015`; the Valmer provider is at its clean
current-schema baseline `0001`. The Valmer provider owns exactly three tables:

- `valmer_connectors__valmerassetdetails`;
- `valmer_connectors__vector_de_precios_valmer`;
- `ms_markets__index_values__t_1d`.

There is no obsolete reference-rate table, compatibility storage, or migration
decoder. After recreation, FRED, Banxico policy, Banxico fixing, both Valmer
quote producers, TIIE, SOFR, XCCY, vector history, and the government curve were
run again.

The final governed-storage audit on 2026-08-21 verified:

- 8,633 reference-rate observations and 81 Valmer quote observations in
  `IndexValuesTS.1d`;
- 26,430 observations across seven Banxico fixing indices;
- 13,083 government-vector observations over 248 dates, including 9,029 CETES
  and 4,054 M Bonos rows;
- 248 government curves and one current row for each of TIIE, SOFR, and XCCY;
- all 55 quote-backed key nodes resolve to exact-date Index observations;
- all 12,761 government key nodes resolve to exact-date Asset observations.

`scripts/verify_current_pipeline.py` is the repeatable verification command. It
derives expected government-curve coverage from the current governed vector
dates and fails on missing required products, duplicate source observations,
coverage gaps, truncated governed queries, invalid key-node schemas, or
unresolved source references. It does not freeze historical row counts.

The 2026-08-30 run found 489 government-vector dates through 2026-08-20 but
only 248 government-curve dates through 2025-08-27. The resulting 241-date
coverage gap is a real pipeline readiness failure; run the approved
`Valmer MXN Government Curve Refresh` Job before expecting verification to
succeed.
