# AGENTS.md

## Project-Specific Instructions

Project state is limited to `.agents/brief.md`, `.agents/status.md`, `.agents/tasks.md`, and
`.agents/record.md` when those files are needed. Keep only actionable work in `tasks.md`, keep
durable verified context in the other state files, and rely on git history for completed work.
Do not introduce additional historical agent-state logs.

`.mainsequence/workflows/valmer-control-plane-api.yaml` is the repository's only workflow file.
Declare every API, agent and Job in it, and keep its path and resource keys: the backend identifies
each target by workflow file path and key, so a new file or key creates a new target. In
`execution.steps` both APIs (`valmer-control-plane-api` and `valmer-data-api`) wait for the
`migrate-valmer` Job; the operational and scheduled Jobs stay out of the graph, because a step would
not hold their new image back and would tie their promotion policy to the APIs' group (see "Workflow
Declarations" in `docs/deployment.md`). Keep every revision backward compatible (expand first). The
file is workflow API 3.0.0, whose access entries name policies: every workload declares `reader` on
the `MetaTables` branch, every workload that calls the Markets API also `reader` on the
`MainSequenceMarkets` branch, and the control-plane API declares `runner` on each Job of
`JOB_LAUNCH_PROFILES`: add a launch profile and its grant together. No resource declares a Secret,
Constant or Bucket: the deployment needs none, and a declaration on an object the Environment lacks
blocks the whole push. Grants on the objects the source Jobs read are by hand ("Objects Outside
MetaTables" in `docs/deployment.md`). A push needs the Environment's MetaTables API on the locked
client's release (0.1.57 or later), its ms-markets deployment on 3.0.2 with its migration (revision
`0022`) applied and its Markets API deployed, the Valmer Team owning `valmer_connectors` with the
table roles on `mainsequence.markets` that "Teams And Namespaces" lists, including every new
workload User. See "Publishing", "Teams And Namespaces" and "Markets API Access" in
`docs/deployment.md`.

Other applications read Valmer data only through the Valmer data API (ADR 0010,
`docs/data-api.md`); none of them installs or imports `valmer_connectors`. The SQL that joins
Valmer's tables for them lives in `src/valmer_connectors/data_api/`. Its routes change only by
addition: removing or renaming a route or field needs a new API version, and a column a consumer
reads is never renamed or dropped in place. A consumer pipeline that needs a Valmer time series as
an input references the table by the identifier the `GET /api/v1/datasets/` route publishes.

Markets registries are reached only through the Markets API (ms-markets ADR 0047): the composition
roots (Job scripts, the CLI, the control-plane API module) call
`valmer_connectors.runtime.use_markets_api()`, and Markets rows are read and written with set-based
`msm.api` row calls (`filter(...__in=...)`, `upsert_many`, `get_many_by_*`, publication composites),
never one call per row. Never import a Markets table class to build a statement, never use a Markets
row class's `_active_context()`, and never select a backend in library code: applications import
`valmer_connectors` and choose their own. `bootstrap_runtime()` only attaches tables; the static
registry rows are written by the `registry-seed` Job (`valmer-connectors registries seed`).
Discount curves, Index fixings and daily Index values live only in Valmer's own tables:
`valmer_discount_curves_instance()` and `valmer_index_fixings_instance()` (`valmer_connectors.storage`)
and `ValmerDailyIndexValuesStorage`. Every producer, reader, bootstrap and seed names those; the core
`DiscountCurvesStorage`, `IndexFixingsStorage` and `MarketsDailyIndexValuesStorage` appear only in the
one-off move (`services/time_series_backfill.py`, `docs/time-series-instances.md`). Add no environment
switch, deprecated alias or other backward-compatibility layer for them.

The control-plane API learns its caller only from the SDK request identity that `create_app` in
`src/valmer_connectors/control_plane/api.py` installs once per application; route code gets the
caller through `User.get_logged_user()`. Do not read the caller from headers or `request.state`, and
do not add a UID-header, process-account or other unauthenticated fallback. The platform launcher
refuses to serve the app without that installation, and hosted requests must carry the
platform-signed caller assertion, which must state the caller's `is_organization_admin` and, since
SDK 9.0.29 (which ms-markets 3.0 requires), must not list its `team_uids`: membership is checked on
demand. Operators are only the UIDs in `VALMER_CONTROL_PLANE_OPERATOR_UIDS`; the admin flag and team
membership grant nothing here. The SDK API-surfaces skill describes this contract (the vendored copy
under `.agents/skills/mainsequence/` is still the 9.0.24 one until the skills are refreshed). See "Request Identity" in `docs/control-plane.md`.

Main Sequence SDK guidance: `.agents/skills/mainsequence/`; MetaTables and ms-markets guidance:
`.agents/skills/metatables/` and `.agents/skills/ms_markets/`.
