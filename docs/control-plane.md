# Valmer Control Plane

## Repository Boundary

The control plane is one application implemented across two Main Sequence CodeRepositories:

- `valmer-connectors` owns the FastAPI resource, operational read models, specialized Job launch
  profiles, job execution authorization, and the single backend-managed workflow file that
  declares the FastAPI release and every Job.
- `ValmerConnectorsMonitor` owns the Vite/React static site. It is the only control-plane user
  interface and extends `@dev-mainsequence/command-center-sdk` for embedding, theme, navigation,
  page layout, resource lists, discovery, and bulk actions.

The legacy Streamlit dashboard is not part of this control-plane surface.

## Backend Surface

`api/valmer_control_plane/main.py` exports the FastAPI application. Business logic is reusable
under `src/valmer_connectors/control_plane/`; route handlers remain thin.

The API publishes:

- `GET /health`, a liveness answer for an authenticated caller (the platform's readiness probe
  is the launcher's own `/ms-health-deployment`, answered outside the app);
- `GET /api/v1/control-plane/me` for the request-bound viewer/operator projection;
- `GET /api/v1/control-plane/overview` for headline metrics and active failures;
- `GET /api/v1/control-plane/pipeline` for the dependency-ordered pipeline model;
- canonical resource collection and discovery endpoints for data products, assets, Jobs, and Job
  runs; and
- canonical Job-run bulk-action preflight and execution endpoints.

Collection responses use the SDK `command-center.resource_collection@v1` shape. Discovery,
preflight, and execution use the installed Command Center SDK 0.1.18 contracts. Discovery
payloads are validated with the SDK runtime parser rather than a copied schema.

The Assets collection joins persisted platform state by responsibility: canonical identity comes
from `AssetTable`, vendor descriptors come from `ValmerAssetDetailsTable`, pricing-target status
comes from the existence of an `AssetCurrentPricingDetails` row, and time-varying price and risk
observations come from the published Valmer vector TimeIndexMetaTable. It never reclassifies
pricing targets from columns in the latest vector observation.

The data-product catalog contains only product names, descriptions, approved identity filters, and
freshness thresholds. Runtime table identifiers are derived from the installed SQLAlchemy storage
models and resolved against registered MetaTables. Cadence, observation time, current row count,
asset fields, pricing-detail coverage, Job state, and JobRun state come from live platform
resources. The API does not publish synthetic identity counts or `unknown` fallback states.

### Organization Environment

The overview's `environment` is the display name of the Organization Environment of the
CodeRepositoryBranch that runs the API. The Environment belongs to that branch, not to the tables
the control plane reads: MetaTables carry no Environment, and the MetaTables client selects its API
deployment in the Environment the SDK resolves for the same branch. The gateway therefore reads the
SDK's process CodeRepository context, the Git-resolved branch that also scopes the Jobs queries.
`require_code_repository_branch_context()` and `resolve_organization_environment_uid()` establish
the branch and its Environment UID. The name is the resolved branch's read-only
`organization_environment_name` projection, or the `OrganizationEnvironment` read for that UID
when the branch omits it. Like the rest of the SDK context, it is a process snapshot: a renamed
Environment appears after the API restarts.

No table is consulted and there is no Environment override or fallback. On an unregistered branch,
or a registered branch without an Environment, `environment` is null and the overview reports the
SDK's own CodeRepositoryBranch or Environment error as an `Environment:` failure. A loopback
`METATABLES_API_URL` development API bypasses MetaTables Environment discovery; data products then
come from that development API while the header still names the branch's Environment.

## Request Identity

`create_app()` in `src/valmer_connectors/control_plane/api.py` calls
`mainsequence.server.fastapi.install_request_identity(app)` once, after mounting the routes, on
every application it builds: the deployed `api/valmer_control_plane/main.py`, the VS Code local
API and the tests. The SDK middleware authenticates the caller before any route runs:

- hosted, it verifies the platform-signed `X-MainSequence-Caller-Assertion`: an EdDSA signature
  from the platform's published key, checked against the issuer, release and Environment that the
  deployment's trusted configuration names, with a lifetime of at most five minutes, and since
  SDK 9.0.22 stating the caller's `team_uids` and `is_organization_admin`;
- locally, without hosted markers, it validates a direct `Authorization: Bearer` token against
  `MAINSEQUENCE_ENDPOINT/api/v1/users/me/`.

Routes get the caller through one dependency that calls `User.get_logged_user()`. A missing or
invalid caller is rejected with 401, and an unavailable verifier with 503, before the route runs.
There is no `X-User-UID` or other header fallback and no fallback to the process's own SDK
account, and a hosted deployment cannot select local mode. The API never trusts the static-site
iframe user field. `request.state.user` and `request.state.user_uid` are SDK compatibility
projections that the application does not read.

The workflow declares no `public_ingress`, so every route, `/health` included, needs an
authenticated caller. If a deployment ever declared a route public, a route that needs a caller
would still refuse the anonymous request.

The installer records `app.state.mainsequence_request_identity`: `installed`, `mode`
(`"assertion"` hosted, `"local"` otherwise) and the deployment's `FASTAPI_PUBLIC_INGRESS` pairs.
The platform launcher (PodDeploymentOrchestrator since `3057764`) checks that declaration right
after importing the app and refuses to serve an app without it. Installing twice on one
application raises, so `create_app(app=...)` takes an application without request identity.

## Human Authorization

The caller identity answers who called; the control plane still decides what that caller may do.
Viewer access is read-only. Operators are configured explicitly through the comma-separated
`VALMER_CONTROL_PLANE_OPERATOR_UIDS` release environment variable. An absent or empty value grants
no operator access. The team UIDs and Organization-admin flag in the caller identity grant
nothing here: an Organization admin outside that list is a viewer. Job launch behavior is
fail-closed:

1. the current caller must be an operator;
2. discovery must advertise the action for that caller;
3. the SDK performs preflight before confirmation;
4. selection must contain exactly one Job UID;
5. the Job must belong to the current CodeRepositoryBranch and, hosted, be one of the Jobs
   the workflow grants the API;
6. the latest run cannot already be pending or running; and
7. the Job image must be ready.

The execution endpoint does not accept arbitrary execution paths or command arguments. Every
branch Job is discoverable and parameterless by default. Jobs with specialized runtime controls
have an execution-path launch profile containing an explicit typed parameter allowlist. The API
validates those values, converts them to fixed command flags, and appends only those flags to the
registered Job entrypoint.
Successful launches produce a structured application log containing the human UID, request UID,
Job UID, Job name, returned JobRun UID, and command-argument count.

The VS Code local review runs as the developer whose access token the review host sends, and that
developer is a viewer unless the local `.env` lists their UID in
`VALMER_CONTROL_PLANE_OPERATOR_UIDS`. Production authorization remains controlled only by the
FastAPI release environment. After the platform accepts a launch, the Jobs view links the
returned JobRun UID to the existing Job runs resource filtered to that exact execution; it does not
create a second JobRun presentation or polling implementation.

## Jobs And Scheduling

`.mainsequence/workflows/valmer-control-plane-api.yaml`, the repository's only workflow file,
declares the source, fixing, curve, verification, and standard-pipeline Jobs next to the FastAPI
release, including their platform-owned display descriptions. It includes distinct immutable
entrypoints for the production-safe VS Code variants: Artifact, OneDrive Graph, and MetaTable
vector sources; the four-series TIIE-only fixing refresh; and the forced current XCCY rebuild.
Only the dependency-ordered standard pipeline is scheduled (`0 13 * * 1-5`). Individual Jobs
remain unscheduled recovery operations that an authorized operator can launch from the static
site.

The `migrate-valmer` Job is declared in the same file. Its execution graph prepares the image,
runs that Job (`scripts/migrate_valmer.py`), and deploys the FastAPI release only after it
succeeds. No step references the pipeline Jobs: the backend activates a Job's new image,
schedule included, as soon as the commit image is verified, whatever a step's `needs` say, and
a Job in the graph would tie its promotion policy to the API's group. The pipeline Jobs keep
their own every-push redeployment (see "Workflow Declarations" in `docs/deployment.md`).

The workflow grants the API `edit` on each of the sixteen Jobs it launches. Hosted, the API
sees only the Jobs granted to its workload User, so its Jobs and Job-runs collections list
those sixteen and not the migration Job. Wherever the migration Job is listed, as in a local
review that runs with the developer's own access, preflight blocks it: hosted migrations run
only inside a deployment, never by hand.

`GET /api/v1/control-plane/jobs/{job_uid}/run-parameters` returns the application-owned runtime
controls for one branch Job. Unprofiled Jobs return an empty parameter list. Vector Jobs expose
pricing-detail refresh and cursor-reprocessing booleans. Provider Jobs that support a cutoff expose
an optional inclusive end date. The static site renders these controls inside the SDK confirmation
dialog and sends their values through the normal SDK preflight and execution payload.

The MetaTable source itself is not a runtime control. Both
`scripts/update_vector_valmer_metatable.py` and the control-plane preflight import
`VALMER_METATABLE_SOURCE_CONFIG_RESOURCE` from `valmer_connectors.settings`. The JSON is packaged
under `valmer_connectors/config` and resolved with `importlib.resources`, so it is available from
both a repository checkout and an installed wheel. For a governed MetaTable,
preflight verifies active provisioning and physical binding, compares its registered column
contract with the Valmer mapping, and performs a one-row governed read probe. For the explicit
direct-MSSQL compatibility setting, it performs a one-row read through the same compatibility
reader used by the Job and verifies every mapped source column. Neither path accepts a source
override from the launch request.

The local upload-folder vector launch is intentionally not a platform Job because its configured
path exists only on the developer workstation. The migration current/revision/upgrade launchers
(all three target the MetaTables local runtime), Streamlit, the example script, and the
local API/Vite host are development or administration launchers, not production workload Jobs.

The workflow declaration is desired repository state, not proof that the Jobs exist. Overview and
Pipeline responses reconcile all sixteen configured pipeline launch profiles with live branch
Jobs by execution path. Missing pipeline Jobs are reported as configuration failures, and an
existing Job without a run is reported as
`not-run`. Launch responses must contain the platform JobRun UID. When the platform omits a status,
the control plane reports the accepted launch as `ACCEPTED` and links to that returned JobRun.

The Jobs collection is one frontend collection request. Its backend adapter performs one bulk
branch-scoped Job query without a name allowlist and one bulk JobRun query for all returned Job
UIDs; it does not issue one JobRun request per Job. Job names, descriptions, execution paths,
schedules, and image state come from the platform Job. Adding another branch Job therefore needs
no display-catalog or Vite change.

`scripts/run_control_plane_pipeline.py` is a thin Job entry point. The reusable runner under
`src/valmer_connectors/control_plane/pipeline.py` starts every existing producer in its own Python
process and stops immediately when a stage fails. Persisted-data verification remains a separate
manual Job because its current audit asserts a fixed rebuilt baseline rather than a rolling daily
count.

## Static-Site Integration

The `ValmerConnectorsMonitor` static site uses:

- `createStaticSiteIframeClient` for the `mainsequence.valmer-control-plane` version-one child
  protocol;
- host theme propagation through SDK theme presets and tokens;
- `ApplicationNavigationShell` for the control plane's internal Monitoring and Operations routes;
- `ApplicationPage`, `ApplicationPageHeader`, `ApplicationPageStack`, `ApplicationCard`, and
  `ApplicationCardGrid` for page composition;
- `ResourceListPage` and SDK resource cells for all four domain collections; and
- SDK discovery, preflight, confirmation, and execution for Job launches.

All backend calls use `StaticSiteIframeClient.fetchFastApi` with the exact FastAPI
ResourceRelease UID. The static site never receives the Command Center session, never constructs
an authorization header, never persists a delegated token, and has no direct-link credential
fallback.

The platform injects the reserved exact `VITE_COMMAND_CENTER_ORIGIN`. The static release sets
`VITE_FASTAPI_RESOURCE_RELEASE_UID` to the backend release. It can name the release instead of
pinning its UID, as
`{resource_release: {repository: valmer-connectors, key: valmer-control-plane-api}}`, which the
backend resolves at every build. A pinned UID stays valid while the API keeps its workflow file
and key.

## Release Sequence

The two repositories must be released in dependency order:

1. confirm that the Environment's MetaTables API runs 0.1.34, that its ms-markets deployment
   runs 2.2.10 and its migration Job has succeeded in the same DataSource, and that the Valmer
   Team holds Writer on the `valmer_connectors` and `mainsequence.markets` namespaces (see
   "Publishing" and "Teams And Namespaces" in `docs/deployment.md`), then push
   `valmer-connectors`. Its API 2.3.0 workflow declares the FastAPI
   release by `source_path` (`api/valmer_control_plane/main.py`) with every-commit automatic
   redeployment, three retained revisions, and the platform's default static-site CORS policy,
   gates it on the `migrate-valmer` Job, keeps the pipeline Jobs outside the graph, declares
   MetaTables branch access on every workload and grants the API edit on the Jobs it launches.
   Before the migration step starts, add the new Jobs' workload Users to the Valmer Team (see
   "Teams And Namespaces" in `docs/deployment.md`);
2. confirm that the workflow run's `migrate` step succeeded before its `deploy_api` step;
3. confirm that only one Job per pipeline Job name remains, and delete the old Jobs that are
   still bound to the removed `valmer-control-plane-jobs.yaml` (see "Cleanup After The Push"
   in `docs/deployment.md`);
4. wait until the FastAPI release has a ready `active_revision`;
5. configure the installed SDK's exact stable FastAPI release target for the Vite application;
6. validate and push the existing `ValmerConnectorsMonitor` static-site workflow, which deploys
   the root Vite SPA on every pushed commit and retains three revisions;
7. add a Command Center navigation link only with an explicit human audience grant; and
8. verify the embedded handshake, dark/light themes, overview reads, resource pagination, viewer
   denial, operator preflight, Job execution, and JobRun polling in Command Center.

The workflow file carries no resource, release or Job UIDs; the backend binds every target
through the workflow file path and resource key, so neither may be renamed. Do not grant
organization-wide navigation visibility as a deployment default.
The release URLs remain stable: repository events create desired immutable revisions and traffic
switches only when the desired revision becomes the ready `active_revision`.

## Verification

### Local VS Code review

Open the backend repository in VS Code and run the compound launch
`Control Plane: API + Vite (local review)`. It starts the development-only API
wrapper on `127.0.0.1:8017`, starts the sibling Vite repository on
`127.0.0.1:5187`, and opens `http://127.0.0.1:5187/`.

The review host uses the installed Command Center SDK host and child protocol.
The wrapper (`.vscode/control_plane_dev.py`) only adds CORS for the review origin
around `create_app()`, so the local API authenticates as described in
[Request Identity](#request-identity), in local mode. When the review host
starts, its launch runs `mainsequence auth token` and the host sends that token
as the Bearer credential; the API validates it against `MAINSEQUENCE_ENDPOINT`
from `.env`, so the CLI session and `.env` must name the same backend.
Development-backend access tokens last five minutes and the review host does not
renew its token: restart `Control Plane: Vite Review Host` when requests start
answering 401. The developer is a viewer unless `.env` lists their UID as an
operator, so a visual review does not start real platform workloads. The launch
waits for the API with `.vscode/wait_for_http.py`, which treats any answer below
500 from `/health`, including that route's 401, as a started server.

Backend checks:

```bash
.venv/bin/ruff check src/valmer_connectors/control_plane api/valmer_control_plane \
  scripts/run_control_plane_pipeline.py tests/test_control_plane_api.py \
  tests/test_control_plane_pipeline.py
.venv/bin/pytest -q tests/test_control_plane_api.py tests/test_control_plane_pipeline.py
```

Static-site checks in `ValmerConnectorsMonitor`:

```bash
npm run check
```

`npm run check` runs the closed SDK theme-token audit, Vitest contract and theme tests, TypeScript
compilation, and the production Vite build.
