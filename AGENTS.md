# AGENTS.md

## Project-Specific Instructions

Project state is limited to `.agents/brief.md`, `.agents/status.md`, `.agents/tasks.md`, and
`.agents/record.md` when those files are needed. Keep only actionable work in `tasks.md`, keep
durable verified context in the other state files, and rely on git history for completed work.
Do not introduce additional historical agent-state logs.

`.mainsequence/workflows/valmer-control-plane-api.yaml` is the repository's only workflow file.
Declare every API, agent and Job in it, and keep its path and resource keys: the backend identifies
each target by workflow file path and key, so a new file or key creates a new target. In
`execution.steps` the API waits for the `migrate-valmer` Job; the operational and scheduled Jobs
stay out of the graph, because a step would not hold their new image back and would tie their
promotion policy to the API's group (see "Workflow Declarations" in `docs/deployment.md`). Keep
every revision backward compatible (expand first). Every workload declares view on the
`MetaTables` branch, and the API declares edit on each Job of `JOB_LAUNCH_PROFILES`: add a launch
profile and its grant together.
A push needs the Environment's MetaTables API on 0.1.35 (the locked client's release), its
ms-markets deployment on 2.2.10 with its migration applied, and the Valmer Team with Writer on the
`valmer_connectors` and `mainsequence.markets` namespaces, including every new workload User. See
"Publishing" and "Teams And Namespaces" in `docs/deployment.md`.

The control-plane API learns its caller only from the SDK request identity that `create_app` in
`src/valmer_connectors/control_plane/api.py` installs once per application; route code gets the
caller through `User.get_logged_user()`. Do not read the caller from headers or `request.state`, and
do not add a UID-header, process-account or other unauthenticated fallback. The platform launcher
refuses to serve the app without that installation, and hosted requests must carry the
platform-signed caller assertion, which since SDK 9.0.22 must also state the caller's `team_uids`
and `is_organization_admin`. Operators are only the UIDs in `VALMER_CONTROL_PLANE_OPERATOR_UIDS`;
the admin flag and team UIDs grant nothing here. The installed SDK 9.0.24 API-surfaces skill
describes this contract. See "Request Identity" in `docs/control-plane.md`.

Main Sequence SDK guidance: `.agents/skills/mainsequence/`; MetaTables and ms-markets guidance:
`.agents/skills/metatables/` and `.agents/skills/ms_markets/`.
