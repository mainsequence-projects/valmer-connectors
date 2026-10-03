# Deployment

## Publishing

A plain `git push` publishes this repository. The workflow files under
`.mainsequence/workflows/` decide what deploys. Every resource in them declares
`tag_regex: null`, so every push to the branch is eligible:

- `valmer-control-plane-api.yaml` (workflow API 2.3.0) prepares the code
  repository image, runs the `migrate-valmer` Job from it, and only then deploys
  the control-plane FastAPI release (`needs: [migrate]`).
- `valmer-control-plane-jobs.yaml` (workflow API 2.1.0) declares the pipeline
  Jobs. They are outside the execution graph and redeploy on their own.

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

The Job applies only the Valmer provider. The ms-markets schema that the Valmer
tables reference is migrated by the ms-markets deployment.

Do not run `metatables migrations upgrade` or `downgrade` against a hosted
runtime, and do not migrate at application startup. The control plane lists the
migration Job but refuses to launch it by hand. Local development applies the
provider with
`metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`.

## Workflow Declarations

The control-plane Jobs and their platform-owned descriptions are declared in
`.mainsequence/workflows/valmer-control-plane-jobs.yaml`. The standard pipeline
is the only scheduled Job; leaf Jobs and source/rebuild variants are manual
operations. The platform catalog deliberately excludes launch configurations that
depend on a developer workstation or exist only to run UI, debugging, examples,
or migration administration.

The API is declared as `kind: fastapi` with
`source_path: api/valmer_control_plane/main.py`. Workflow API 2.3.0 files carry
no resource or release UIDs; the backend binds the existing release through the
workflow file path, the resource key `valmer-control-plane-api`, and the release
kind, so neither the file nor the key may be renamed. Validate an edited
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
`valmer_connectors.migrations:migration: 0001 (already current) on <DataSource UID>`.
`metatables migrations current` only reads the deployed revision.

## MetaTables 0.1.14 Local Migration Check (2026-10-03)

The Valmer provider was applied to an isolated MetaTables 0.1.14 local SQLite
runtime. Alembic applied revision `0001`, and `migrations current` reports
`0001 (head)`, but catalog finalization was refused with
`400 POST /meta-tables/finalize-managed/: cascading_foreign_key_unsupported`.
Revision `0001` declares
`fk__valmer_connectors__valmerassetdetails__asset_uid_cc247d13d3` with
`ON DELETE CASCADE`, and MetaTables rejects any active table that has, or is
referenced by, a foreign key whose delete or update rule is not `RESTRICT` or
`NO ACTION`. The same rule exists in its PostgreSQL access layer, so the hosted
`migrate-valmer` Job is expected to fail the same way, which blocks the API
deployment, until the foreign key is changed by a new revision. The ms-markets
2.1.1 provider could not be applied to the same runtime either (secondary index
name longer than 63 characters on `ms_markets__portfolioeventledgerts`, plus
cascading foreign keys of its own), so the ms-markets tables that the Valmer
foreign keys reference were missing locally. See `.agents/status.md` and
`.agents/tasks.md`.

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
