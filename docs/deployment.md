# Deployment

## Publishing

A plain `git push` publishes this repository. `.mainsequence/workflows/` holds
one workflow file, `valmer-control-plane-api.yaml` (workflow API 3.0.0), and it
decides what deploys. It declares the control-plane FastAPI release, the
`migrate-valmer` Job and the eighteen pipeline Jobs. Every resource declares
`tag_regex: null`, so every push to the branch is eligible:

- the execution graph prepares the code repository image, runs the
  `migrate-valmer` Job from it, and only then deploys the API
  (`needs: [migrate]`);
- no step references the eighteen pipeline Jobs, all of which the API
  launches, so each one redeploys on every push by itself; its new image is
  active as soon as the commit image is verified, before the migration runs
  (see [Workflow Declarations](#workflow-declarations));
- every workload declares `reader` on the MetaTables repository's branch, every
  workload that calls the Markets API also on the `MainSequenceMarkets`
  branch, and the API declares `runner` on the eighteen Jobs (see
  [Teams And Namespaces](#teams-and-namespaces) for table access and
  [Markets API Access](#markets-api-access) for registry writes).

Push only when the Environment is ready for this image:

- its MetaTables API, the API release of the `MetaTables` repository's branch
  in that Environment, runs the release of the locked `mainsequence-metatable`
  client (ms-markets 3.0.1 needs 0.1.57 or later). Keep the client and the
  API on one release. Clients since
  0.1.35 ignore unknown `/runtime-context/` fields, so the failure of the 0.1.20
  client against an API on 0.1.18 or older
  ([MetaTables issue #33](https://github.com/mainsequence-projects/MetaTables/issues/33))
  no longer applies;
- its ms-markets deployment runs 3.0.1, that deployment's migration Job has
  applied revision `0022` in the same DataSource (see [Migrations](#migrations)),
  and its Markets API (the `api` release of the `MainSequenceMarkets`
  repository) is deployed;
- the Valmer Team owns `valmer_connectors` and holds the table roles on
  `mainsequence.markets` that [Teams And Namespaces](#teams-and-namespaces)
  lists, and the Job and release workload Users that the push creates are
  added to it before the `migrate` step starts;
- for the first 0.2.0 push, the schedules are held: from that image on Valmer
  writes and reads only its own tables, so its history is copied before they
  resume (steps 1 to 7 of `docs/time-series-instances.md`).

When `migrate-valmer` fails, `deploy_api` never runs and the push deploys no
API. The pipeline Jobs still take the new image, because the backend activates
it before the migration runs.

To check the MetaTables API, run `metatables runtime status` without
`--local`, after `mainsequence login`, from a checkout whose branch is
registered in that Environment. It is a read-only request. With 0.1.35 its
`bootstrap.current_revisions` is `["0012_table_creator"]` and nothing is
pending.

The API installs the SDK request identity (see "Request Identity" in
`docs/control-plane.md`). The platform's FastAPI launcher, PodDeploymentOrchestrator
since `3057764`, refuses to start an app without it ("Application request identity
is not installed"), and the previous revision keeps serving. Once deployed, every
hosted request must carry the platform-signed caller assertion, so the Environment
needs the backend's caller-assertion signing and key discovery
(`/fastapi/caller-keys/`) and a gateway that forwards
`X-MainSequence-Caller-Assertion`. Without them the new revision starts but every
API request answers 401 (503 while key discovery is down); nothing falls back to
`X-User-UID`. The assertion must state the caller's `is_organization_admin`;
since SDK 9.0.29, which ms-markets 3.0 requires, it must not list the caller's
`team_uids` (membership is checked on demand), and an assertion that does is
refused. The Environment's backend must therefore sign that claim set before
this image serves requests; an image on SDK 9.0.22 to 9.0.28 expected
`team_uids`, and SDK 9.0.6 only its older, exact claim set.

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

## Skill Refresh

Run these only when the update is explicitly requested:

```bash
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository update-platform-skills --path .
metatables copy-metatables-skills --path .
msm copy-msm-skills --path .
```

`update-agent-skills` copies the SDK-owned skills offline and replaces
`.agents/skills/mainsequence/`; `mainsequence code-repository update-sdk` also
refreshes them after it upgrades the SDK. `update-platform-skills` needs the
signed-in session and installs the platform skills under
`.agents/skills/mainsequence_platform/`. Since SDK 9.0.24 the SDK guidance
lives only in those skills: `AGENTS.md` is the repository's own file, no SDK
command writes it, and it carries no SDK-managed block.

## Migrations

Hosted environments are migrated only by the `migrate-valmer` Job of
`valmer-control-plane-api.yaml`. It runs `scripts/migrate_valmer.py` from the
candidate image, which calls
`metatables.upgrade_application("valmer_connectors.migrations:migration")` and
prints one line with the revision, whether it migrated, and the DataSource. A
database already at head is left unchanged. A failed Job blocks the API
deployment and the previous release keeps serving; fix the revision and push
again. Redeploying an older image does not roll the schema back.

The Job needs the Environment's MetaTables API on the release of the locked
client, and its workload User in the Valmer Team (see
[Teams And Namespaces](#teams-and-namespaces)). `upgrade_application` waits up
to five minutes for a waking API, then finalizes the provider's tables in
batches. A workload User without Writer on a namespace the provider registers
tables in fails with `namespace_not_writable` and names the `caller_user_uid`;
nothing of that table is registered and the API is not deployed. Add the
member, then run the Job again or push a new commit: a failed workflow run does
not resume, and each commit gets its own run. Do not migrate the Environment by
hand from a workstation.

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
runtime, and do not migrate at application startup. The hosted control plane
is not granted the migration Job, and its preflight refuses that Job wherever
it is listed. Local development applies the
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

Only the API and `migrate-valmer` are in `execution.steps`. The eighteen
pipeline Jobs, the `JOB_LAUNCH_PROFILES` of
`src/valmer_connectors/control_plane/catalog.py` with the scheduled
`standard-pipeline-refresh` and `pipeline-verification` among them, stay out
of the graph on purpose, and each one redeploys on every push by itself:

- a step cannot hold a Job's new image back until the migration has
  succeeded. At tdag-django `f16c42249` (2026-10-11) the backend starts the
  image deployment run of a Job that a `prepare_image` step names when it
  prepares the step's connected group, and that run activates the new image,
  and re-enables the Job's schedule, as soon as the shared commit image is
  verified, whatever the step's `needs` say;
- a Job in the graph would join the API's connected group, which is applied
  together and admits a push only when every one of its targets does under
  its own promotion policy. A failed Job declaration would then stop the
  migration and the API, and a Job taken off every-push promotion would make
  the group skip later pushes.

A scheduled or manual run can therefore start on a new image before
`migrate-valmer` has finished, and keeps that image if the migration fails.
Keep every revision compatible with the previous release: add tables and
nullable or defaulted columns first, and drop or rename only in a later
release.

The API is declared as `kind: fastapi` with
`source_path: api/valmer_control_plane/main.py`. Workflow API 3.0.0 files carry
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

## Teams And Namespaces

Every workload of the workflow declares
`access.branches: [{repository: MetaTables, policy: reader}]`, `reader` on the
MetaTables repository's branch in its Environment, which a workload needs to
discover that Environment's MetaTables API. Without it `upgrade_application`
and every table read or write fail to resolve the API. Every workload except
`migrate-valmer`, `pipeline-verification` and `time-series-backfill` also declares
`{repository: MainSequenceMarkets, policy: reader}`, which it needs to discover
the Markets API (see [Markets API Access](#markets-api-access)). A grant on another
repository's branch needs a push that the platform resolves to a person (a
member whose GitHub account an Organization admin linked) who manages the
workload and holds `reader` on that branch. Otherwise the resource fails with
`declared_access_not_granted`, and nothing of its group is applied.

The API also declares `access.resources` with `policy: runner` on each of the
eighteen Jobs it launches. A workload has no implicit access to the other
workloads of its branch: the hosted API lists, and can launch, only the Jobs
granted to it. It lists a Job, runs it and reads its runs, never changes it,
which `runner` covers. `migrate-valmer` is not granted, so the hosted Jobs
collection does not list it; wherever it is listed, as in a local review, the
preflight refuses its execution path.

A named Job must exist when the API is applied. On the push that creates the
Jobs that holds: the backend applies the resources outside the graph while it
processes the push, and the workflow run applies the API's group only after
that has committed (tdag-django `f16c42249`). A Job whose own declaration fails
on that push leaves the API's grant `not_created`, which stops the migration
and the API rollout of that push.

Table access is granted in MetaTables, not in the workflow file. The Valmer
provider registers its tables in two namespaces:

| Namespace | Tables registered by `valmer_connectors.migrations:migration` |
| --- | --- |
| `valmer_connectors` | `valmer_connectors__alembic_version`; `valmer_connectors__valmerassetdetails` (`valmer_connectors.ValmerAssetDetails`); `valmer_connectors__vector_de_precios_valmer` (`valmer_connectors.vector_de_precios_valmer`); since revision `0002`, Valmer's curve and fixing instances `valmer_connectors__discountcurvests__valmer` and `valmer_connectors__indexfixingsts__valmer`; since revision `0003`, Valmer's daily Index values `valmer_connectors__index_values__t_1d` (`valmer_connectors.IndexValuesTS.1d`) |
| `mainsequence.markets` | `ms_markets__index_values__t_1d` (`IndexValuesTS.1d`), the daily Index values Valmer wrote before its own table; kept until a later release (step 8 of `docs/time-series-instances.md`) |

Markets registries (assets, Index and Index type rows, conventions, curves,
market-data bindings, current pricing details) and the Markets tables Valmer
publishes into (asset snapshots, timestamped pricing details) are no longer
written with MetaTables grants: the Jobs reach them through the Markets API
(see [Markets API Access](#markets-api-access)). Valmer's curves, fixings and
daily Index values are in its own tables in `valmer_connectors`, and no
producer, reader or seed touches the Markets time-series tables. Only the
one-off move (`time-series-backfill`, `docs/time-series-instances.md`) still
reads the Markets discount-curve (`ms_markets__discountcurvests`), Index-fixing
(`ms_markets__indexfixingsts`) and daily Index-value
(`ms_markets__index_values__t_1d`) tables and, on rollback, writes them; it is
the only Valmer process that attaches a table in `mainsequence.markets`. As the
MetaTables guide
(`deploy-application-migrations.md` in the `metatables-migrations` skill)
describes, one Team per Environment owns the Valmer tables, for example
`valmer-connectors-production`. With the MetaTables role names (`owner`,
`editor`, `data_writer`, `data_viewer`, `metadata_viewer`):

| Grant to the Valmer Team | Why |
| --- | --- |
| `editor` (or `owner`) on the namespace `valmer_connectors` | its tables, the migration and the updaters that write them |
| table `editor` on `ms_markets__index_values__t_1d`, `ms_markets__discountcurvests` and `ms_markets__indexfixingsts`, until a later release | the one-off move reads them and its rollback runs the copy updater on them (`tables.operate_updaters`); `migrate-valmer` needs the role on `ms_markets__index_values__t_1d` (below) |
| `metadata_viewer` on the tables those three reference (`ms_markets__index`, `ms_markets__indexformuladefinition`, `ms_markets__curve`) and on the tables their foreign keys reach in turn, until the same release | the move attaches the Markets tables, and attaching a Markets-namespace storage also attaches its foreign-key targets |
| nothing else on `mainsequence.markets` | registries go through the Markets API |

The Markets workload and every application that prices through the default
market-data set need `data_viewer` on Valmer's curve and fixing tables, which
the set's bindings name. The Markets workload (its Index dataset catalog) and
every application that reads Valmer's daily Index values need `data_viewer` on
`valmer_connectors__index_values__t_1d`. Step 3 of
`docs/time-series-instances.md` grants them.

The migration provider registers `ms_markets__index_values__t_1d` in
`mainsequence.markets`, and migration admission checks every table of the
provider, so `migrate-valmer` keeps the table `editor` role on it
(`tables.migrate`) until a later Valmer release drops the table from its
provider (step 8 of `docs/time-series-instances.md`). Only an Environment where
revision `0001` has never run needs more: creating the table there takes
`tables.create`, that is `editor` on the `mainsequence.markets` namespace, for
that first migration. These grants follow the MetaTables permission matrix
(`security/permissions.md` in the `metatables-migrations` skill); confirm them
in each Environment before removing the old namespace-wide grant.

The Team's members are the workload Users of `migrate-valmer` and of every Job
that writes (each pipeline Job except `pipeline-verification`), and of the two
readers, `pipeline-verification` and the API. A `data_viewer` grant would do for
those two; membership keeps the Team the tables' one owner. Each Job and
release reports its `workload_user_uid`.

A Job or release created by a push, including one moved to another file or
key, has a new workload User that must be added. Until `migrate-valmer`'s
workload User is a member and the namespace exists, the migration fails with
`namespace_not_writable` and blocks the API rollout.

## Markets API Access

Valmer reaches Markets registries through the Markets API (ms-markets ADR 0047),
not through MetaTables grants on `mainsequence.markets`:

- its composition roots select ms-markets' remote row backend with
  `valmer_connectors.runtime.use_markets_api()`: every Job script that touches
  Markets, the `valmer-connectors` CLI and the control-plane API module. The
  backend finds the Markets API (the FastAPI release `api` of the
  `MainSequenceMarkets` repository) on the first row call; `MARKETS_API_URL`
  and `MARKETS_API_TOKEN` select a local one;
- `bootstrap_runtime()` attaches only Valmer's own tables (none in
  `mainsequence.markets`); `Asset.filter`/`Asset.upsert_many`, `Index.upsert_many`,
  `AssetSnapshots.publish`, `AssetPricingDetails.add_many`,
  `AssetCurrentPricingDetails.get_many_by_asset_uid`, the curve, convention
  and market-data binding composites run on the Markets API;
- library code never selects a backend: an application that imports
  `valmer_connectors` keeps the one it chose, and one that keeps the runtime
  backend attaches the Markets registries and pricing tables it uses itself.

In each Environment the workflow grants every workload except `migrate-valmer`,
`pipeline-verification` and `time-series-backfill` `reader` on the
`MainSequenceMarkets` branch. That grant is what lets a workload reach the
Markets API and write through it.

The control-plane API reads current pricing details through the Markets API
and Valmer's own tables through MetaTables, so it attaches no markets runtime.

Still outside the Markets API, each for a stated reason:

- the one-off move: the `time-series-backfill` Job copies the history of the
  Markets daily Index-value, discount-curve and Index-fixing tables into
  Valmer's with ms-markets' `msm.storage_copies`, reading the Markets tables
  through MetaTables, and copies Valmer's rows back on a rollback
  (`docs/time-series-instances.md`). The migration provider keeps registering
  `ms_markets__index_values__t_1d` until a later release;
- `scripts/patch_valmer_asset_pricing_details.py`, an operator-only repair
  that stays on the runtime backend: it finds stale payloads with JSON queries
  on the core pricing-detail tables, and no Markets route reads timestamped
  pricing details or filters pricing details by payload;
- in-process pricing (for example `build_valuation_position_from_sheet`)
  reads curves and fixings from the table the default market-data set's
  bindings name (Valmer's own), through MetaTables, as every `msm_pricing`
  consumer does.

### Objects Outside MetaTables

The source Jobs read Secrets, a Constant and a Bucket of their Environment by
name. The workflow declares each one in the `access` of every Job that reads
it, with the narrowest policy the code needs, so the push gives that Job's
workload User the policy:

| Object | Kind | Declared policy | Jobs |
| --- | --- | --- | --- |
| `FRED_API_KEY` | Secret | `value_reader` | `fred-reference-rates-refresh`, `standard-pipeline-refresh` |
| `BANXICO_TOKEN` | Secret | `value_reader` | `banxico-fixings-refresh`, `banxico-tiie-fixings-refresh`, `banxico-policy-refresh`, `standard-pipeline-refresh` |
| `VALMER_ONEDRIVE_TENANT_ID`, `VALMER_ONEDRIVE_CLIENT_ID`, `VALMER_ONEDRIVE_CLIENT_SECRET` | Secrets | `value_reader` | `vector-onedrive-refresh` |
| `VALMER_ONEDRIVE_DRIVE_ID` | Constant | `reader` | `vector-onedrive-refresh` |
| `Hitorical Valmer Vector Analytico` | Bucket | `reader` | `vector-refresh`, `standard-pipeline-refresh` |

Each Job reads the value of its Secrets (`reader` on a Secret shows only its
name) and of the Constant, and lists and downloads the Bucket's Artifacts
without uploading any (uploading takes `contributor`).
`standard-pipeline-refresh` runs the vector, Banxico and FRED scripts in its
own container, so it declares what those Jobs declare. The API and the other
Jobs read none of these objects.

A declaration gives a policy; it does not create the object. Create each one
in the Environment before its Jobs run, or they fail when they reach it. The
names are the defaults: the `VALMER_*_NAME` environment variables of
`src/valmer_connectors/settings.py` rename some of them, and a renamed object
needs its declaration renamed too. What a push does with a declaration whose
object is missing, or whose policy the pushing person cannot give, has not
been checked for workflow API 3.0.0, so read the `access` results of the
workflow action after the push. To check one grant:
`mainsequence secrets access explain <secret uid> secrets.read_value --user <workload user uid>`
or
`mainsequence constants access explain <constant uid> constants.read --user <workload user uid>`.

Two grants stay outside the workflow:

- **The vector Artifacts.** A Bucket `reader` lists only the Artifacts the
  workload can already see, because each Artifact has its own policies. The
  workload Users of `vector-refresh` and `standard-pipeline-refresh` need
  `viewer` on every Artifact in the Bucket, given by a person who holds
  `owner` on it (whoever uploaded it). The workflow does not name them, because
  every new vector file is a new Artifact. The CLI has no Artifact commands;
  with the SDK:

  ```python
  from mainsequence.client import Artifact

  for artifact in Artifact.filter(bucket__name="Hitorical Valmer Vector Analytico"):
      artifact.attach_policy("viewer", principal_type="user", principal_uid="<workload user uid>")
  ```

  Grant each new Artifact when it is uploaded. A Job that a push recreates has
  a new workload User, which needs these grants again; a grant to the Valmer
  Team (`principal_type="team"` and its UID), whose members both workload
  Users already are ("Teams And Namespaces"), survives it.
- **The MetaTable `dbo.vector_precios_gubernamental_pip_vista`.** It is
  granted in MetaTables, not in the workflow: `data_viewer` on the table for
  the workload Users of `vector-metatable-refresh` and of the API, which reads
  it in that Job's preflight. With the locked MetaTables client, call
  `grant_role("<workload user uid>", "data_viewer")` on the registered
  `MetaTable`, or `grant_role("<team uid>", "data_viewer", principal_kind="team")`
  for the Valmer Team, whose members both workload Users already are ("Teams
  And Namespaces"), so the grant survives a recreated Job.

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

## MetaTables 0.1.34 Local Migration Check (2026-10-11, 2026-10-14)

The Valmer provider was applied to a fresh, isolated MetaTables 0.1.34 local
SQLite runtime (system migrations through `0012_table_creator`) that already
held the ms-markets 2.2.10 schema (revision `0020`, 62 active tables), as a
hosted DataSource does once the ms-markets deployment has migrated it. The
runtime had its own storage directory and port, so the laptop's shared local
runtime was not touched. The check ran on 2026-10-11 with mainsequence 9.0.22
and again on 2026-10-14, on another fresh runtime, with mainsequence 9.0.24
and mcp 2.3.0; every result below held both times:

- `metatables --local migrations upgrade --provider valmer_connectors.migrations:migration head`
  ran `-> 0001, current_schema_baseline` and finalized all four Valmer catalog
  tables as `active` with `failed_count: 0`: `valmer_connectors__alembic_version`
  and `valmer_connectors__valmerassetdetails` and
  `valmer_connectors__vector_de_precios_valmer` in namespace
  `valmer_connectors`, and `ms_markets__index_values__t_1d` (`IndexValuesTS.1d`)
  in namespace `mainsequence.markets`, all under provider key
  `valmer_connectors:valmer_connectors`. `migrations current` reports
  `0001 (head)`, and all 66 catalog tables are `active`.
- The asset-details key to `ms_markets__asset.uid` is `ON DELETE CASCADE` in
  the SQLite schema; the other three keys on Valmer-owned tables are
  `RESTRICT`.
- `scripts/migrate_valmer.py`, run unchanged through
  `metatables.configure_local_client()`, printed
  `valmer_connectors.migrations:migration: 0001 (already current) on
  <DataSource UID>`. After `downgrade base` (three tables deleted, the version
  table kept, `failed_count: 0`), it printed `0001 (migrated)` and all 66
  tables were `active` again. Both runs exited 0.
- Alembic autogenerate finds no difference between the provider metadata and
  the migrated schema; the same comparison reports a column added to the
  models, so it does detect drift.
- `get_impact(action="delete_rows")` on `ms_markets__asset` is allowed with no
  blockers, lists the asset-details table as a `cascade_delete` edge and the
  vector table as a `restrict` edge.

SQLite does not exercise PostgreSQL. The hosted `migrate-valmer` Job is
unverified until its first workflow run, which in Production also creates the
tables, because none of them exist there yet. See `.agents/status.md` for the
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
