# Open Tasks

## Make the Valmer schema finalize under MetaTables 0.1.14

- Scope: MetaTables refuses to finalize a table that has, or is referenced by,
  a cascading foreign key (`cascading_foreign_key_unsupported`). Revision
  `0001` declares `fk__valmer_connectors__valmerassetdetails__asset_uid_cc247d13d3`
  with `ON DELETE CASCADE`. Decide whether a new revision `0002` (never an edit
  of `0001`) should replace it with `RESTRICT` and change the model's
  `ondelete` to match; asset deletion would then fail while Valmer details
  exist instead of deleting them. ms-markets 2.1.1 owns its own blockers: a
  70-character default secondary index name on
  `ms_markets__portfolioeventledgerts` and 29 cascading foreign keys in its
  revision `0018`.
- Do not push the gated API workflow before this is resolved: a failed
  `migrate-valmer` Job blocks every API deployment.
- Owning skills: `metatables-migrations` and `metatables-local-development`.
- Expected output: on an isolated local runtime with the ms-markets schema
  applied, `metatables --local migrations upgrade --provider
  valmer_connectors.migrations:migration head` finalizes all four Valmer
  catalog tables as `active`.
- Required evidence: the upgrade and `current` output and the catalog table
  states.

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
  0.1.47 release together with ms-markets 2.1.1. Uninstalling 0.1.46 deletes the
  top-level `migrations` files it shared with ms-markets 2.0.x; ms-markets 2.1
  no longer installs a top-level `migrations` package, so upgrading both
  together leaves none behind. Consumers do not apply the Valmer or ms-markets
  providers; those deployments migrate their own schemas.
- Expected output: each consumer has no top-level `migrations`, `banxico`,
  `fred`, `agents`, `cli`, or `command_center` modules, and
  `import msm_migrations, valmer_connectors.migrations` succeeds.
- Required evidence: the site-packages listing and the import check in each
  consumer environment.

## Publish and verify the two-repository control plane

- Scope: once the migration task above is resolved, validate the API 2.3.0
  workflow against the branch template and push this backend. The workflow
  declares the FastAPI release by `source_path` and deploys it only after the
  `migrate-valmer` Job succeeds. Then configure the stable FastAPI release
  target for `ValmerConnectorsMonitor` and push its validated automatic Vite
  static-site declaration. Navigation placement remains a separate explicit
  human grant.
- Owning skills: `mainsequence-command-center-fastapi`, `resource-release`,
  `static-site`, and `command-center`.
- Expected output: the embedded SDK-native control plane reads current Valmer
  state and an authorized operator can launch one approved Job.
- Required evidence: the workflow run with a succeeded `migrate` step before
  `deploy_api`, both ready active revisions, automatic deployment enabled
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
