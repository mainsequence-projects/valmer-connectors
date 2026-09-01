# Valmer Control Plane

## Repository Boundary

The control plane is one application implemented across two Main Sequence CodeRepositories:

- `valmer-connectors` owns the FastAPI resource, operational read models, specialized Job launch
  profiles, job execution authorization, and backend-managed Job workflow declarations.
- `ValmerConnectorsMonitor` owns the Vite/React static site. It is the only control-plane user
  interface and extends `@dev-mainsequence/command-center-sdk` for embedding, theme, navigation,
  page layout, resource lists, discovery, and bulk actions.

The legacy Streamlit dashboard is not part of this control-plane surface.

## Backend Surface

`api/valmer_control_plane/main.py` exports the FastAPI application. Business logic is reusable
under `src/valmer_connectors/control_plane/`; route handlers remain thin.

The API publishes:

- `GET /health` for release health checks;
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
Organization Environment, asset fields, pricing-detail coverage, Job state, and JobRun state come
from live platform resources. The API does not publish synthetic identity counts or `unknown`
fallback states.

## Human Authorization

FastAPI handlers read the platform-injected `request.state.user_uid`; they do not call
`User.get_logged_user()` and do not trust the static-site iframe user field for authorization.

Viewer access is read-only. Operators are configured explicitly through the comma-separated
`VALMER_CONTROL_PLANE_OPERATOR_UIDS` release environment variable. An absent or empty value grants
no operator access. Job launch behavior is fail-closed:

1. the current caller must be an operator;
2. discovery must advertise the action for that caller;
3. the SDK performs preflight before confirmation;
4. selection must contain exactly one Job UID;
5. the Job must belong to the current CodeRepositoryBranch;
6. the latest run cannot already be pending or running; and
7. the Job image must be ready.

The execution endpoint does not accept arbitrary execution paths or command arguments. Every
branch Job is discoverable and parameterless by default. Jobs with specialized runtime controls
have an execution-path launch profile containing an explicit typed parameter allowlist. The API
validates those values, converts them to fixed command flags, and appends only those flags to the
registered Job entrypoint.
Successful launches produce a structured application log containing the human UID, request UID,
Job UID, Job name, returned JobRun UID, and command-argument count.

The VS Code local-review compound binds its fixed review user UID as an operator so the SDK action
can exercise discovery and preflight locally. Production authorization remains controlled only by
the FastAPI release environment. After the platform accepts a launch, the Jobs view links the
returned JobRun UID to the existing Job runs resource filtered to that exact execution; it does not
create a second JobRun presentation or polling implementation.

## Jobs And Scheduling

`.mainsequence/workflows/valmer-control-plane-jobs.yaml` declares the source, fixing, curve,
verification, and standard-pipeline Jobs, including their platform-owned display descriptions. It
includes distinct immutable entrypoints for
the production-safe VS Code variants: Artifact, OneDrive Graph, and MetaTable vector sources;
the four-series TIIE-only fixing refresh; and the forced current XCCY rebuild. Only the
dependency-ordered standard pipeline is scheduled (`0 13 * * 1-5`). Individual Jobs remain
unscheduled recovery operations that an authorized operator can launch from the static site.

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
path exists only on the developer workstation. Migration inspection/revision/upgrade, Streamlit,
the example script, and the local API/Vite host are development or administration launchers, not
production workload Jobs.

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

The platform injects the reserved exact `VITE_COMMAND_CENTER_ORIGIN`. The static release must set
`VITE_FASTAPI_RESOURCE_RELEASE_UID` to the deployed backend release UID.

## Release Sequence

The two repositories must be released in dependency order:

1. sync `valmer-connectors` so the platform indexes `api/valmer_control_plane/main.py` as a FastAPI
   CodeRepositoryResource;
2. resolve that resource's public UID and add a backend API 2.1.0 workflow declaration using the
   real UID, every-commit automatic redeployment, three retained revisions, and the platform's
   default static-site CORS policy;
3. sync the backend again and wait until the FastAPI release has a ready `active_revision`;
4. configure the installed SDK's exact stable FastAPI release target for the Vite application;
5. validate and sync the existing `ValmerConnectorsMonitor` static-site workflow, which deploys
   the root Vite SPA on every synchronized commit and retains three revisions;
6. add a Command Center navigation link only with an explicit human audience grant; and
7. verify the embedded handshake, dark/light themes, overview reads, resource pagination, viewer
   denial, operator preflight, Job execution, and JobRun polling in Command Center.

Do not invent the FastAPI resource or release UID in a workflow before the backend repository has
been synchronized. Do not grant organization-wide navigation visibility as a deployment default.
The release URLs remain stable: repository events create desired immutable revisions and traffic
switches only when the desired revision becomes the ready `active_revision`.

## Verification

### Local VS Code review

Open the backend repository in VS Code and run the compound launch
`Control Plane: API + Vite (local review)`. It starts the development-only API
wrapper on `127.0.0.1:8017`, starts the sibling Vite repository on
`127.0.0.1:5187`, and opens `http://127.0.0.1:5187/`.

The review host uses the installed Command Center SDK host and child protocol.
Its short-lived local capability is accepted only by the development wrapper;
the production FastAPI app and static-site transport are unchanged. The local
identity is deliberately a viewer and Job launch actions remain unavailable,
preventing a visual review from starting real platform workloads.

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
