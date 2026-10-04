# Changelog

## 0.1.47 (unreleased)

### Packaging

- The wheel ships only the `valmer_connectors` import package. The Valmer
  migration provider moved from the top-level `migrations` package to
  `valmer_connectors.migrations` (provider reference
  `valmer_connectors.migrations:migration`, no `PYTHONPATH`), and `banxico` and
  `fred` moved under `valmer_connectors`. Earlier wheels overwrote the
  top-level `migrations` package that ms-markets 2.0.x installed. The provider
  package, migration namespace, version table and revision `0001` are
  unchanged, so no database migration is needed.
- `BanxicoFixingsNode`, `BanxicoPolicyRatesNode` and `FredReferenceRatesNode`
  get new update hashes because their class import paths changed. Their output
  tables are unchanged, so they continue incrementally.

### Dependencies

- Requires `mainsequence>=9.0.6,<10`, `mainsequence-metatable>=0.1.20,<0.2`
  and `ms-markets>=2.2.5,<3`. The lock selects mainsequence 9.0.6,
  mainsequence-metatable 0.1.20 and ms-markets 2.2.5.
- Deploying 0.1.47 requires the Environment's hosted MetaTables API on 0.1.19
  or later; upgrade that API before pushing. The 0.1.20 client validates the
  API's `/runtime-context/` response with a model that forbids unknown fields,
  and an API on 0.1.18 or older still sends `runtime_switch_available`,
  `runtime_switch_error` and `hosted_environment_target`
  ([MetaTables issue #33](https://github.com/mainsequence-projects/MetaTables/issues/33)).
  Against such an API the `migrate-valmer` Job fails before it migrates
  anything, the API is not deployed, and the pipeline Jobs, which take the new
  image anyway, fail in their updater runs. Every consumer that locks 0.1.20
  has the same prerequisite.
- MetaTables registers tables with cascading foreign keys since 0.1.17, so
  revision `0001` finalizes with its asset-details key to
  `ms_markets__asset.uid` (`ON DELETE CASCADE`): deleting an Asset deletes its
  Valmer details row. Since 0.1.18, write on a table depends only on that
  table's own grants; under 0.1.17, writing `ms_markets__asset` also required
  write access to `valmer_connectors__valmerassetdetails`.
- MetaTables 0.1.19 runs one local API per local storage directory and
  publishes its address and private token in `server.json` there, so
  `metatables --local` and `metatables.configure_local_client()` reach it from
  any checkout and commits no longer require a restart. The local API has no
  Local/Hosted switch any more. 0.1.20 retries runtime-access requests that
  time out and requires SDK 9.0.6.
- SDK 9.0.6 never replays a POST, PUT, PATCH or DELETE and bounds read-only
  retries by one timeout budget, so a control-plane Job launch is sent at most
  once. PyJWT and cryptography are base SDK dependencies; the `server` extra is
  gone.
- ms-markets 2.2.2 requires MetaTables 0.1.17 and names the long
  portfolio-ledger index explicitly, so its provider applies to a new
  environment. Nothing here imports the `cli` or `command_center` packages that
  ms-markets 2.1 renamed, or what 2.2.0 removed: `msm_command_center`, the
  asset-monitor builders, pricing `response_mappings` and the `apps.v1`
  re-export modules. ms-markets 2.2.3 to 2.2.5 add the bulk-delete impact
  preflight and omit unset optional fields from the bulk-action and
  tabular-frame contracts; the control plane builds its own preflight and
  discovery payloads with every field set and uses none of those models.

### Deployment and migrations

- `.mainsequence/workflows/valmer-control-plane-api.yaml` moves to workflow API
  2.3.0. The API is declared as `kind: fastapi` by `source_path` under the same
  key and file, so the backend keeps the existing release. A new
  `migrate-valmer` Job runs `scripts/migrate_valmer.py`, which applies
  `valmer_connectors.migrations:migration` with
  `metatables.upgrade_application`, and the API deploys only after it
  succeeds: `image` -> `migrate` -> `deploy_api`, all from the same image.
- That file is now the repository's only workflow. The sixteen pipeline Jobs
  moved into it from `.mainsequence/workflows/valmer-control-plane-jobs.yaml`
  (workflow API 2.1.0), which is deleted. Keys, names, descriptions, execution
  paths, compute, runtimes, the `0 13 * * 1-5` standard-pipeline schedule and
  every-push redeployment are unchanged. No execution step references them, so
  each one keeps redeploying on its own. The backend identifies a workflow Job
  by file and key, so the push creates sixteen new Jobs; the old ones are
  deleted by a backend with ADR-077 or must be deleted by hand. See "Cleanup
  After The Push" in `docs/deployment.md`.
- Hosted environments are migrated only by the `migrate-valmer` Job. The
  `valmer-connectors migrations commands` output, the documentation and the
  VS Code launchers use `metatables --local ...` for local development,
  including `migrations revision`, whose autogenerate otherwise compares the
  models with the Environment's hosted database, and
  nothing here applies the ms-markets provider any more: the ms-markets
  deployment migrates its own schema. The Valmer foreign keys reference
  ms-markets tables, so the Job succeeds only in a DataSource where the
  ms-markets deployment has migrated that schema.
- The control plane lists the migration Job but refuses to launch it by hand.
- `.gitignore` ignores `.local/`, where the local MetaTables API records which
  SQLite file it serves (`runtime-data-sources.json`, next to its deployment
  configuration). Since MetaTables 0.1.19 the launcher's private client token
  is in `server.json` in the local storage directory, outside the checkout.
- Documentation describes publishing with `git push`:
  `mainsequence code-repository sync` only refreshes dependency files since
  SDK 9.0.5.

### Control-plane API

- The API installs the SDK request identity: `create_app()` calls
  `mainsequence.server.fastapi.install_request_identity(app)` once per
  application, and routes get the caller from `User.get_logged_user()` instead
  of reading `request.state.user_uid`. PodDeploymentOrchestrator refuses to
  serve a FastAPI app without this installation since `3057764`. Hosted
  requests need the platform-signed caller assertion; local runs validate a
  direct Bearer token against `MAINSEQUENCE_ENDPOINT`. Nothing falls back to
  `X-User-UID`, the viewer and operator rules are unchanged, and the workflow
  still declares no public ingress, so `/health` also needs an authenticated
  caller.
- The VS Code local review drops its static local capability and fixed review
  user. The review host sends an access token from `mainsequence auth token`,
  the local API validates it, and the developer is a viewer unless `.env`
  lists their UID as an operator.

### Agent skills

- Refreshed the SDK 9.0.6, MetaTables 0.1.20 and ms-markets 2.2.5 skills and
  the managed `AGENTS.md` block. The ms-markets refresh drops the asset-monitor
  skills that 2.2.0 removed, and the MetaTables local-development skill
  describes the one local API per laptop.
