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

## Roll valmer-connectors 0.1.47 out to consumers

- Scope: move qisfactory, mexicofundcompetition and marketsanalytics to the
  0.1.47 release together with ms-markets 2.2.2 and mainsequence-metatable
  0.1.18. Uninstalling 0.1.46 deletes the top-level `migrations` files it shared
  with ms-markets 2.0.x; ms-markets 2.1 and later install no top-level
  `migrations` package, so upgrading both together leaves none behind.
  Consumers do not apply the Valmer or ms-markets providers; those deployments
  migrate their own schemas.
- Expected output: each consumer has no top-level `migrations`, `banxico`,
  `fred`, `agents`, `cli`, `command_center` or `msm_command_center` modules,
  and `import msm_migrations, valmer_connectors.migrations` succeeds.
- Required evidence: the site-packages listing and the import check in each
  consumer environment.

## Publish and verify the two-repository control plane

- Scope: validate the API 2.3.0 workflow through
  `POST /api/v1/code-repository-branches/{uid}/validate-workflow/` (it was
  validated offline only, because the backend had no CodeRepositoryBranch for
  this repository), confirm that the ms-markets deployment's migration Job has
  succeeded in the target DataSource (the Valmer foreign keys need its tables),
  and push this backend. The workflow declares the FastAPI release by
  `source_path` and deploys it only after the `migrate-valmer` Job succeeds;
  a failed Job blocks every API deployment. Then configure the stable FastAPI
  release target for `ValmerConnectorsMonitor` and push its validated automatic
  Vite static-site declaration. Navigation placement remains a separate
  explicit human grant.
- Owning skills: `mainsequence-command-center-fastapi`, `resource-release`,
  `static-site`, and `command-center`.
- Expected output: the embedded SDK-native control plane reads current Valmer
  state and an authorized operator can launch one approved Job.
- Required evidence: the validate-workflow response, the ms-markets migration
  JobRun, the workflow run with a succeeded `migrate` step (its log line
  `valmer_connectors.migrations:migration: 0001 (migrated) on <DataSource UID>`)
  before `deploy_api`, both ready active revisions, automatic deployment enabled
  with null tag policies and three retained revisions, repository-event
  DeploymentRuns, iframe/theme verification, viewer denial, operator preflight
  and launch, JobRun polling, and application logs.

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
