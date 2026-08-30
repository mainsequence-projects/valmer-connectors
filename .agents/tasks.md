# Open Tasks

## Finish the SDK 8 integration and migration-package validation

- Scope: restore or provide the project-owned `migrations` package expected by
  `tests/test_valmer_migrations.py`, then run the complete repository suite and
  one updater lifecycle against the migrated backend. `ms-markets==1.0.2` is
  now the first adopted release that declares Main Sequence SDK 8 compatibility;
  updater imports and all tests outside the missing migration package pass.
- Owning skills: `mainsequence-time-index-table-updates` and the relevant
  `mainsequence-markets-*` domain skills.
- Expected output: the project imports cleanly with Main Sequence SDK 8.0.4 or
  newer, all tests pass, and one project updater completes a backend run.
- Required evidence: dependency lock, clean legacy-name scan, passing full test
  suite and Ruff check, successful updater run, and persisted output readback.

## Publish and verify the two-repository control plane

- Scope: review and canonically sync this dirty backend so the FastAPI resource
  is indexed, add and validate its API 2.1.0 automatic-release declaration with
  the real resource UID, sync again, configure the stable FastAPI release target
  for `ValmerConnectorsMonitor`, and then sync its validated automatic Vite
  static-site declaration. Navigation placement remains a separate explicit
  human grant.
- Owning skills: `mainsequence-command-center-fastapi`, `resource-release`,
  `static-site`, and `command-center`.
- Expected output: the embedded SDK-native control plane reads current Valmer
  state and an authorized operator can launch one approved Job.
- Required evidence: both ready active revisions, automatic deployment enabled
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
