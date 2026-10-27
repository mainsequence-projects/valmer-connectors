# Changelog

## 0.2.0 (2026-10-27)

Valmer reaches Markets through the Markets API (ms-markets ADR 0047) instead
of holding Writer on the `mainsequence.markets` namespace.

### Dependencies

- Requires exactly `ms-markets==3.0.0` (the remote row backend, `msm.client`,
  the publication composites and `msm.storage_instances`), and therefore
  `mainsequence>=9.0.29,<10` and `mainsequence-metatable>=0.1.57,<0.2`.
  `uv.lock` and `requirements.txt` resolve the published ms-markets 3.0.0,
  SDK 9.0.33 (9.0.31 is the first that parses the platform's Agent reads) and
  MetaTables 0.1.57.
- Deploy against an Environment whose ms-markets deployment runs 3.0.0, has
  applied revision `0022` and serves the Markets API, whose MetaTables API runs
  the locked client's release, and whose backend signs the SDK 9.0.29 caller
  assertion (no `team_uids`).

### Markets API boundary

- Valmer's composition roots (every Job script that touches Markets, the
  `valmer-connectors` CLI, the control-plane API module and the dashboards)
  call `valmer_connectors.runtime.use_markets_api()`, which selects
  ms-markets' remote row backend; the Markets API is found on the first row
  call. Library code never selects a backend.
- Assets are resolved with `Asset.filter(unique_identifier__in=...)` and
  written with `Asset.upsert_many`; asset snapshots go through
  `AssetSnapshots.publish`; current pricing details are read with
  `AssetCurrentPricingDetails.get_many_by_asset_uid`; Index and IndexType rows
  are written with `upsert_many`, which refuses a `calculation_method` change
  on an index with formulas or observations as `Index.upsert` does. Every
  Markets read or write is set-based.
- `read_valmer_asset_detail_alias_frame` and the other asset-detail readers
  query Valmer's own table and merge Markets assets from batch row calls
  instead of joining `AssetTable`.
- The control plane reads pricing-target status through the Markets API and
  attaches no markets runtime; its "Current pricing details" product now counts
  registered Valmer assets with current pricing details.
- The default pricing market-data set is created only when missing. Its
  `discount_curves` and `interest_rate_index_fixings` bindings name Valmer's
  own curve and fixing tables (below), written through the Markets API.
- Every workload except `migrate-valmer`, `pipeline-verification` and
  `time-series-backfill` declares view on the `MainSequenceMarkets` branch.
  The Valmer Team drops Writer on `mainsequence.markets`; it keeps table roles
  only on the three Markets time-series tables the one-off move copies (and
  `migrate-valmer` on `ms_markets__index_values__t_1d`, which its provider
  still registers) until a later release. The Markets
  `MARKETS_REGISTRY_WRITERS` constant must list the Valmer workloads (see
  "Teams And Namespaces" and "Markets API Access" in `docs/deployment.md`).

### Seeding and attachment (breaking for importers)

- `bootstrap_runtime()` defaults to `seed_static_rows=False` and attaches only
  Valmer's own tables, under either backend: it no longer attaches the Markets
  registries and pricing tables for an importer on ms-markets' runtime
  backend, which attaches the ones it uses itself. An importer that relied on
  the old default seeding must pass `seed_static_rows=True` or rely on the
  `registry-seed` Job.
- **Removed:** `valmer_pricing_runtime_models()` and
  `configure_valmer_discount_curves_cadence()` (`instruments.curve_bootstrap`);
  `attach_valmer_curve_pricing_runtime()` attaches only the models it is given.
- The static registry rows are written by the new `registry-seed` Job
  (`scripts/seed_valmer_registries.py`, `valmer-connectors registries seed`),
  the first stage of the standard pipeline and launchable from the control
  plane. `runtime validate` keeps seeding explicitly.

### Curves, fixings and daily Index values in Valmer's namespace

- Revision `0002` creates Valmer's instances of the Markets discount-curve and
  Index-fixing storages, `valmer_connectors__discountcurvests__valmer` and
  `valmer_connectors__indexfixingsts__valmer`, in `valmer_connectors`
  (`msm.storage_instances.configured_time_index_instance`; the migration
  provider references `ms_markets__curve` for the new foreign key).
- Valmer owns its daily Index values (ADR 0047, decision 4): revision `0003`
  creates `valmer_connectors__index_values__t_1d`
  (`valmer_connectors.IndexValuesTS.1d`), `ValmerDailyIndexValuesStorage`.
  The Markets table `ms_markets__index_values__t_1d` stays registered by
  revision `0001` until a later release.
- From this release on Valmer writes and reads only these tables: the curve,
  Banxico fixing, curve-quote, FRED and Banxico policy producers, the curve
  builders, queries, verification, control plane and dashboard name
  `valmer_discount_curves_instance()`, `valmer_index_fixings_instance()` and
  `ValmerDailyIndexValuesStorage` directly, and `registry-seed` always binds
  the default market-data set's `discount_curves` and
  `interest_rate_index_fixings` concepts to Valmer's tables. There is no
  environment switch. The core tables appear only in the one-off move.
- **Update hashes rotate once** for five updaters, and their data continues
  from the rows the backfill copies into Valmer's tables:
  - the TIIE, SOFR and USD/MXN cross-currency curve updaters:
    `ValmerQuoteBackedCurveConfig.quote_storage_table` now defaults to
    `ValmerDailyIndexValuesStorage` and
    `ValmerXccyCurveConfig.curve_storage_table` to Valmer's discount-curve
    table, and a storage class in an updater configuration is hashed by its
    registered table uid;
  - the Banxico fixings (`BanxicoFixingsNode`) and MXN government curve
    (`ValmerMxnGovernmentBondDiscountCurvesNode`) updaters: their
    configurations embed ms-markets' `IndexFixingConfiguration` and
    `CurveConfig`, which ms-markets 3.0 moved to `msm_pricing.time_index`
    (ADR 0048), and a configuration object is hashed with its class's module
    and name. `msm rename-preflight rotation` reports their pre-3.0 hashes.

  The other updaters keep their hashes: an output table is not part of
  `update_hash`, and Valmer's own updater and configuration classes keep their
  names and modules.
- Valmer imports only ms-markets 3.0 names (`msm.time_index`,
  `msm_pricing.time_index` and the `*Updater` classes), never the deprecated
  `data_nodes` aliases that ms-markets 4.0 removes; a boundary test runs
  `msm rename-preflight scan` over the code. The renamed imports resolve to
  the same classes, so they change no update hash.
- The `time-series-backfill` Job (`valmer-connectors time-series backfill`)
  copies, never recomputes, Valmer's rows from the three Markets tables into
  Valmer's with ms-markets' `msm.storage_copies`: it only counts without
  `--execute`, and fails while a row is missing; `--rollback` copies the rows
  Valmer's tables gained back. It is the only process that attaches the
  Markets tables. The moves are also `msm series move|rollback
  valmer_connectors.services.time_series_backfill:valmer_series_moves`.
  `docs/time-series-instances.md` is the runbook: hold the schedules, push
  (the migration applies `0002` and `0003`), grant readers `data_viewer`,
  backfill with `--execute`, rebind with `registry-seed`, verify, resume; or
  roll back.
- **Removed:** `valmer_connectors.data_nodes.canonical_index_values.DailyIndexValuesStorage`
  and its `valmer_connectors.data_nodes` re-export, which named the Markets
  table. Valmer's daily Index values are `ValmerDailyIndexValuesStorage`;
  `MarketsDailyIndexValuesStorage` names the Markets table for the move only.

### Tests

- The control-plane caller-assertion tests sign the claim set of
  `mainsequence>=9.0.29`, which refuses an assertion that lists `team_uids`.

## 0.1.48 (unreleased)

0.1.47 was tagged in a local checkout but never published. Consumers move
from 0.1.46 straight to 0.1.48, so they should also read the 0.1.47 entry
below; where the two differ, this entry applies.

### Dependencies

- Requires `mainsequence>=9.0.24,<10`, `mainsequence-metatable>=0.1.35,<0.2`
  and exactly `ms-markets==2.2.10`, the release the Environment's Markets
  deployment runs; `requires-python` stays `>=3.13,<3.14`. The lock selects
  mainsequence 9.0.24, mainsequence-metatable 0.1.35 and ms-markets 2.2.10,
  and adds what MetaTables 0.1.35 depends on: fastembed (with onnxruntime,
  tokenizers and huggingface-hub) and `mainsequence[mcp]`, whose 9.0.24 extra
  selects mcp 2.3.0.
- Deploy against the Environment's MetaTables API on 0.1.35 and its Markets
  deployment on 2.2.10, with the ms-markets migration applied. The hosted
  MetaTables API 0.1.19 prerequisite of 0.1.47 no longer applies: the 0.1.35
  client ignores unknown `/runtime-context/` fields.
- SDK 9.0.22 and later require the caller's `team_uids` and
  `is_organization_admin` in every hosted caller assertion; SDK 9.0.6 rejects
  the assertions the backend signs now, so the hosted API must run this
  image. The control plane's
  identity code is unchanged, and operators are still only the
  `VALMER_CONTROL_PLANE_OPERATOR_UIDS`: the admin flag and team UIDs grant
  nothing.
- The Valmer provider, its tables and revision `0001`, the updater hashes and
  the bond pricing serialization are identical under the old and the new
  lock, so no database migration is needed.

### Deployment and migrations

- `valmer-control-plane-api.yaml` keeps its file and keys. Every workload
  declares view on the `MetaTables` repository's branch, and the API declares
  edit on the sixteen Jobs it launches; hosted, the control plane therefore
  lists those Jobs and not `migrate-valmer`. A push that grants access to
  another repository's branch needs a person the platform resolves.
- The execution graph is unchanged: `image` -> `migrate` -> `deploy_api`.
  The sixteen pipeline Jobs stay out of it and redeploy on every push by
  themselves. A `prepare_image` step with `needs: [migrate]` would not hold a
  Job's new image back (the backend activates it as soon as the commit image
  is verified) and would tie the Job's promotion policy to the API's group,
  so keep every revision compatible with the previous release.
- The Valmer tables belong to one Team per Environment with Writer on the
  `valmer_connectors` and `mainsequence.markets` namespaces; the workload
  Users that a push creates join it before the migration runs. See "Teams And
  Namespaces" in `docs/deployment.md`, which also lists the Secrets,
  Constant, Bucket and MetaTable source the Jobs read.

### Agent skills

- Refreshed the SDK 9.0.24, MetaTables 0.1.35 and ms-markets 2.2.10 skills.
  The MetaTables refresh removes the retired `metatables-upgrade-legacy-app`
  skill.
- SDK 9.0.24 no longer scaffolds `AGENTS.md`; the SDK guidance, including the
  skill routing, ships in `.agents/skills/mainsequence/`. `AGENTS.md` drops
  the SDK preamble, the marker notice and the managed block, keeps this
  repository's own instructions, and points to the skill namespaces.
  `docs/deployment.md` no longer lists the removed
  `code-repository update AGENTS.md` command.

### Fixed

- Resolving a Mexican reference index uid during bond pricing hydration
  (`resolve_reference_index_uid`) no longer writes registry rows. It used to
  call `upsert_mexican_reference_indexes`, so every pricing process upserted
  the interest-rate IndexType and seven Index rows just to read their uids. It
  now reads them with `Index.get_many_by_unique_identifier`; seeding stays
  `bootstrap_runtime`'s job, and a missing row raises the existing
  "bootstrapped Index row" error. The uid map is cached only once every
  reference index exists.

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
