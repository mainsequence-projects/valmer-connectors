# Project Status

Last verified: 2026-07-25

## ADR 0009: External Reference-Rate Observations

The implementation and MetaTable migration `0003` are applied in the authenticated
project. The normal FRED and Banxico DataNodes load five calendar years when their
scoped identities have no progress and use the same stable updater identities for
later incremental executions. There is no separate production backfill or smoke
updater path.

Local verification:

- SDK and managed skills pin: `4.4.32`
- generated migration upgrade: `0002 -> 0003` against a local SQLite baseline
- authenticated project migration: `0002 -> 0003` applied and finalized; MetaTable
  UID `3d112772-65e6-4fa7-9610-cc455c06aa0d`
- full repository test suite: 247 tests passing under `PYTHONPATH=src`
- Ruff checks for all affected Python files: passing
- strict MkDocs build: passing
- scheduled job contract and reference-rate CLI surface: validated
- VS Code launch configurations for the normal FRED and Banxico updates: validated
- source-only FRED and Banxico launchers attach the migrated runtime without seeding
  unrelated curve definitions; focused producer tests pass
- the five FRED identities' previous 334-row short-history population was deleted
  with an exact `index_identifier` filter; no Banxico rows were targeted
- normal FRED updater hash:
  `fredreferenceratesnode_133ac739a0de0a7c8b6388dfa5263ecf`
- initial normal update: 6,818 persisted rows, all beginning `2021-07-19`
- persisted FRED coverage: 1,826 Fed target-upper rows through `2026-07-18` and
  1,248 rows for each Treasury series through `2026-07-16`
- immediate second normal execution reused the same updater hash, returned no new
  data, and exited successfully

FRED and Banxico live-source initial-load and incremental-identity verification is
complete. Scheduled-job verification remains pending.

Banxico live verification on 2026-07-19:

- live SIE metadata for `SF61745` uses title `Tasa objetivo`; the validator now
  accepts the exact series id plus the `TASA` and `OBJETIVO` policy-target terms
- canonical `BANXICO_POLICY_TARGET` `interest_rate` Index UID:
  `803d298b-85d9-4a98-8b0a-1c88a5082953`
- normal Banxico updater hash:
  `banxicopolicyratesnode_ceb109148a6b2d99bfe90231d34a1124`
- initial normal update: 1,819 persisted rows from `2021-07-19` through
  `2026-07-18`, with decimal rates from `0.0425` to `0.1125` and latest rate `0.065`
- immediate second normal execution reused the same updater hash, returned no new
  data, and exited successfully
- direct authenticated `Job.filter(project__uid=...)` returned zero project jobs;
  the repository `scheduled_jobs.yaml` has not been synchronized to the platform

## Curve Helper Quote History Gap

An authenticated review on 2026-07-19 confirmed that the previously published
Valmer curves embedded construction quotes only in compressed `key_nodes`; those
quotes were never published as normalized Index history.

Verified platform state:

- `DiscountCurvesStorage` UID is `db193848-50b1-44b9-875a-fb652bc6e89b` with
  daily grain `(time_index, curve_identifier)` and compressed `curve` and
  `key_nodes` payloads
- the pre-delete scoped read found eight rows: five
  `VALMER_MXN_GOVERNMENT_BOND` rows, two `VALMER_TIIE_OVERNIGHT` rows, one
  `VALMER_MXN_USD_COLLATERAL_DISCOUNT` row, and no
  `VALMER_USD_SOFR_OVERNIGHT` row
- an exact `curve_identifier`-scoped `delete_after_date(None, ...)` removed all
  eight rows; the authoritative response reported `deleted_count=8` and
  `table_empty=true`
- an independent post-delete read across the same four curve identifiers returned
  zero rows, so no incorrectly published Valmer curve observation remains
- representative TIIE OIS, SOFR OIS, Fed Funds OIS, Fed Funds/SOFR basis, FX
  swap, and cross-currency helper identifiers returned no `AssetTable` rows and
  no `ValmerVectorPricesStorage` rows
- `FED_FUNDS_TARGET_UPPER` is stored separately as an analytical policy-target
  observation; its latest verified value is `0.0375` on `2026-07-18`. It is not
  an effective Fed Funds fixing, a Fed Funds OIS quote, or a SOFR/Fed Funds basis
  quote

The repository explicitly classifies Fed Funds OIS and Fed Funds/SOFR basis
source rows but excludes them from the SOFR curve and rejects them from SOFR
`key_nodes`. This matches ADR 0006, where Fed Funds support is a separate future
curve-policy decision. The Valmer vector publisher is scoped to registered
priceable target bonds, so it cannot close the helper-quote history gap.

Operationally, `scheduled_jobs.yaml` contains only a TIIE curve job, points that
job at the absent `scripts/update_tiie_zero_curve.py` compatibility path, and has
no USD SOFR or USD/MXN cross-currency curve jobs. The authenticated project still
has zero synchronized jobs.

The approved implementation direction is recorded in
`docs/implementation/valmer-curve-quote-index-pipeline-refactor.md`: recognized
FRED, Banxico, and Valmer source-published daily observations all move to the
cadence-specific canonical `IndexValuesTS.1d` storage. The six existing
FRED/Banxico identities and decimal values are preserved through an exact
legacy-to-canonical copy and producer cutover. After a recoverable export,
checksum/coverage reconciliation, and two successful scheduled canonical cycles,
the protected `ReferenceRateObservationsStorage` table is removed through a
generated SDK migration and guarded DataNode-storage catalog deletion. Named TIIE,
SOFR, and XCCY curve DataNodes will depend on the Valmer quote producers; XCCY
will additionally depend on exact-date TIIE and SOFR curve nodes. Fed Funds OIS
and Fed Funds/SOFR basis rows will be persisted but remain observation-only. This
is a documented target state, not yet an implemented or platform-verified
pipeline; the old storage remains live until those gates pass.

## Typed Curve Key-Node Compatibility

The local-path ms-markets dependency and copied managed skills are now at
`0.0.97`. That release defines `CurveKeyNodeSourceReference` with
`type="asset" | "index"`, rejects legacy top-level `asset_identifier` and
`index_identifier`, and applies the strict contract during normal decompression.

The local Valmer implementation now conforms to that contract:

- TIIE OIS, SOFR future/OIS, USD/MXN spot/forward, and XCCY basis helpers emit a
  deterministic canonical Index `source_reference`, retain the raw Valmer row as
  `source_instrument_identifier`, and record `source_observation_time`;
- CETES and M Bonos helpers retain their registered identity through an Asset
  `source_reference` without changing quote or helper economics;
- project validators require the correct source type and reconcile each quote
  Index identifier against the raw Valmer source row;
- obsolete top-level identity fields remain strictly rejected.

Local evidence: 54 focused strict curve tests and all 247 repository tests pass;
Ruff and `git diff --check` pass. Fixture curve reconstruction and
government-bond helper economics remain unchanged.

The incompatible Valmer curve rows were removed with exact identity scope after
fresh browser authentication. They were not read, translated, or used to recover
quote history. Future curve rows may be published only through normal typed
DataNodes after canonical Index history exists.

The post-delete launch audit originally found direct-download curve launchers.
Those targets have now been replaced by quote-producer and dependency-backed
curve launches. `DiscountCurvesStorage` remains empty until the blocked live run
completes.

## Canonical Index and Quote-Backed Curve Implementation

Verified on 2026-07-19:

- project revision `0004` is at head and `IndexValuesTS.1d` is registered as
  physical table `ms_markets__index_values__t_1d`;
- all 8,637 FRED/Banxico observations migrated exactly, with unchanged
  per-index row counts, UTC date bounds, and value sums;
- the physical `valmer_connectors__reference_rate_observations` table was
  dropped and its runtime Python storage/model was removed;
- canonical MXN/USD quote nodes, exact-date query helpers, and named
  dependency-backed TIIE/SOFR/XCCY DataNodes are implemented;
- source normalization covers 34 MXN plus 47 USD rows, including Fed Funds OIS
  and Fed Funds/SOFR basis observations;
- quote-backed key nodes use typed `source_reference.type="index"`; government
  bond key nodes use typed Asset references;
- direct-download production curve functions and migration-only
  decoder/translator/export scripts are absent;
- CLI commands, scripts, VS Code launch configurations, and weekday schedules
  now cover quote producers and the full curve order;
- 245 tests and 39 subtests pass.

Live completion is not yet claimed. The obsolete protected catalog row remains
because the backend rejects individual deletion of Alembic-managed MetaTables
and SDK 4.4.32 does not expose the required schema-management override. The
first post-migration FRED run was then interrupted during runtime attachment
when `tsorm_web_local` auto-reloaded into an unrelated Django system-check
failure for missing `pod_manager.DeploymentRun`; port 8000 now resets requests.
No quote or curve rows were directly inserted to bypass that failure.

## Python 3.13 And Main Sequence 5

The local environment was rebuilt on 2026-07-25:

- `.venv` uses CPython `3.13.11`;
- installed versions are Main Sequence `5.0.1`, ms-markets `0.0.99`, and
  valmer-connectors `0.1.23`;
- the repository contract is `requires-python = ">=3.13,<3.14"` and
  `mainsequence>=5.0.0,<6`;
- `.python-version`, Ruff, VS Code, JetBrains, and the Dockerfile now target
  Python 3.13;
- `uv.lock` resolves 97 packages for Python `==3.13.*`, `uv lock --check`
  passes, and `requirements.txt` is synchronized;
- native imports for QuantLib, NumPy, pandas, SciPy, PyArrow, `pymssql`,
  `psycopg2`, and Streamlit pass;
- project CLI version/help smoke tests and Ruff over `src`, `tests`, and
  `scripts` pass;
- MkDocs strict build and `uv build` pass; the wheel metadata requires Python
  `>=3.13,<3.14` and Main Sequence `>=5.0.0,<6`.

The linked Main Sequence checkout is at tag `v5.0.0.1`, reports package version
`5.0.1`, and contains uncommitted work. Main Sequence 5.0.1 is available on
PyPI, but the current lock still uses the checkout's local path. This is
acceptable for the rebuilt linked-development environment, not for a portable
release image.

Application compatibility is not complete. Full pytest collection fails on the
removed `msm.models.index_calculations` module. With that migration test
excluded, 236 tests and 36 subtests pass; four fail because ms-markets 0.0.98
removed the canonical `IndexValuesTS.unit` column. The production quote-query
projection still requests that column, so the Index identity, observation,
query, and migration contracts must be refactored before a platform run.

The executable task is
`docs/implementation/python-3-13-mainsequence-5-upgrade-plan.md`.
The private `py313` base-image manifest could not be verified anonymously
because GHCR denied the request. No image or platform job was claimed.

## SDK / Platform Discrepancies

- `TimeIndexMetaTable.run_query(...)` from SDK `4.4.32` sends the documented
  `text/plain` request body, but the current local backend returns HTTP 415 and says
  that `text/plain` is unsupported. The standard time-series read endpoint works and
  was used to verify the rows. The backend parser and SDK/docs contract should be
  reconciled so the documented diagnostic query path works.
- `mainsequence project update-sdk --path .` updated the lock and environment to
  `4.4.32` but did not refresh the exported `requirements.txt`; the export was
  reconciled explicitly with the repository's documented `uv export --locked
  --no-dev --no-hashes` command.
- `mainsequence project jobs list --path .` cannot resolve the current project UID
  to a backend row id and exits with `Backend row id is not available`; direct SDK
  filtering by `project__uid` works and was used for the zero-job verification.

## Managed Skill Refresh

- On 2026-07-19, `mainsequence project update-sdk --path .` completed and kept the
  installed Main Sequence SDK at `4.4.32`; the existing
  `.agents/skills/mainsequence/PINNED_FROM.txt` pin already matched, so no generic
  `mainsequence project update_agent_skills` refresh was needed.
- On 2026-07-19, `msm copy-msm-skills --path .` refreshed the managed
  `.agents/skills/ms_markets` namespace from installed `ms-markets` `0.0.97`.
- On 2026-07-25 the same command refreshed the namespace to `0.0.98`, then
  refreshed it again after the linked package advanced to `0.0.99`; its
  current Index skill is `indices/index_workflow`.
- The refreshed fixed-income guidance requires typed
  `source_reference` and rejects the two legacy top-level identity fields.
- Main Sequence 5.0.1 updated the scaffold-managed `AGENTS.md` locally, but
  `mainsequence project update_agent_skills --path .` then failed with
  `Token is invalid` because it now retrieves authenticated platform skills.
  The partial scaffold update was rolled back; the existing scaffold and
  managed Main Sequence skills remain consistently pinned to `4.4.32` until
  both can be refreshed together after login.
- `mainsequence project refresh_token --path .` previously failed with
  `Not logged in`. No new authenticated platform state was claimed; live row
  counts above remain from the earlier verified read.
