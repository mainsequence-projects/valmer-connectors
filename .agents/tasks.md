# Open Tasks

## Complete the Python 3.13 and Main Sequence 5 application refactor

- Scope: implement phases 1 through 4 of
  `docs/implementation/python-3-13-mainsequence-5-upgrade-plan.md`. Owning
  skills: `mainsequence-markets-index-workflow`,
  `mainsequence-data-nodes`, and `mainsequence-metatable-migrations`. Expected
  output: current `IndexUpsert` payloads, unit-free canonical observations,
  quote queries that do not project `IndexValuesTS.unit`, and a migration
  provider referencing `IndexFormulaDefinitionTable` with no compatibility
  module. Evidence: full pytest collection and suite pass without ignored
  files, exact-date quote query tests pass against the current table shape,
  Ruff passes, migration-provider imports pass, and the Python 3.13 package
  build contains the expected Python/Main Sequence metadata.
- Current gap: pytest collection fails on
  `msm.models.index_calculations`; with that test excluded, 236 tests and 36
  subtests pass and four tests fail on the removed `unit` column.

## Make the Python 3.13 release reproducible and run a canary

- Scope: replace local Main Sequence and ms-markets source paths with immutable
  sources available to the image builder, authenticate the CLI and private
  registry, refresh Main Sequence scaffold/skills together, build the Python
  3.13 image, apply migration preflights, and run the ordered quote-to-curve
  canary. Owning skills: `mainsequence-orchestration-and-releases` and
  `mainsequence-metatable-migrations`. Expected output: a portable locked
  environment, verified Python 3.13 project image, current migration heads, and
  one successful canary plus immediate no-op rerun. Evidence: fresh-checkout
  locked sync, image version inspection, migration output, job/run logs, and
  exact-date Index/curve reads.
- Current blockers: the CLI token is invalid and GHCR denied the anonymous
  manifest check. Main Sequence 5.0.1 is now on PyPI, but the project and
  linked ms-markets checkout still require local source overrides to be
  reconciled for a portable release lock.

## Restore the local platform API and complete the canonical pipeline run

- Scope: restore `tsorm_web_local` without changing this repository's data
  contracts, then execute FRED, Banxico, IRS MXN quotes, IRS USD quotes, TIIE,
  SOFR, USD/MXN XCCY, and MXN government curve launchers in that order. Owning
  skills: `mainsequence-data-nodes`,
  `mainsequence-markets-index-workflow`, and
  `mainsequence-markets-fixed-income-curve-building`. Expected output: all 81
  current Valmer quote observations and all four deleted curve families are
  republished through normal typed DataNodes. Evidence: successful command
  exits, exact-date platform reads, typed Index source-reference reconciliation
  for every quote-backed key node, Asset reconciliation for government helpers,
  and immediate second normal runs with no duplicate keys.
- Current blocker: the unrelated local backend checkout fails Django system
  checks because `pod_manager.DeploymentRun` is missing and resets requests on
  port 8000. Do not bypass the DataNodes with direct database inserts.

## Retire the obsolete reference-rate catalog row

- Scope: remove protected catalog UID
  `3d112772-65e6-4fa7-9610-cc455c06aa0d` using the supported Alembic-provider
  retirement/reset operation. Owning skills: `mainsequence-metatable-migrations`
  and `doc-bug-auditor`. Expected output: no obsolete catalog entry; the
  canonical `IndexValuesTS.1d` entry remains active with all 8,637 migrated
  observations. Evidence: catalog lookup returns no obsolete UID, canonical
  per-index counts/date bounds/sums remain unchanged, and migration revision is
  still `0004`.
- Prior blocker: SDK 4.4.32 did not expose the backend-required schema-management
  override. Re-evaluate the supported retirement operation with SDK 5 only
  after authentication; do not bypass Alembic ownership.

## Schedule and verify the data jobs

- Scope: synchronize `scheduled_jobs.yaml` after the pipeline live run. Owning
  skill: `mainsequence-orchestration-and-releases`. Expected output: weekday
  quote jobs at 13:00/13:05 UTC, TIIE/SOFR/XCCY at 13:10/13:15/13:20,
  government curve at 13:25, plus FRED, Banxico, vector, and fixings coverage.
  Evidence: job configuration, one successful run per data job, ordered logs,
  and platform reads proving the expected published dates.

## Assess project-jobs CLI project resolution

- Scope: classify why `mainsequence project jobs list --path .` cannot obtain a
  backend row id for project UID `64338319-bd19-48db-bf14-945d8debb9be` while
  direct project-UID filtering works. Owning skill: `doc-bug-auditor`. Expected
  output: concrete SDK or backend fix recommendation. Evidence: CLI reproduction
  and comparison with the direct SDK response.

## Assess SDK/backend raw-query mismatch

- Scope: classify the HTTP 415 returned by documented
  `TimeIndexMetaTable.run_query(...)` requests with `text/plain`. Owning skill:
  `doc-bug-auditor`. Expected output: concrete SDK or backend request-contract
  fix recommendation. Evidence: reproduce against a live MetaTable while the
  governed compiled-SQL operation returns the expected rows.
