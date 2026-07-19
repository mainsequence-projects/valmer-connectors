# Project Status

Last verified: 2026-07-19

## ADR 0009: External Reference-Rate Observations

The implementation is complete and MetaTable migration `0003` is applied in the
authenticated project. It includes the shared observation storage, FRED and Banxico
producers, canonical index registration, CLI entry points, scheduled jobs, tests,
and the operator runbook.

Local verification:

- SDK and managed skills pin: `4.4.32`
- generated migration upgrade: `0002 -> 0003` against a local SQLite baseline
- authenticated project migration: `0002 -> 0003` applied and finalized; MetaTable
  UID `3d112772-65e6-4fa7-9610-cc455c06aa0d`
- full repository test suite: 246 tests passing
- Ruff checks for all affected Python files: passing
- strict MkDocs build: passing
- scheduled job contract and reference-rate CLI surface: validated
- VS Code launch configuration for the namespaced FRED smoke update: validated
- source-only FRED and Banxico launchers attach the migrated runtime without seeding
  unrelated curve definitions; 16 focused producer tests pass
- namespaced FRED smoke launch `adr-0009-fred-smoke`: completed successfully with
  updater hash `fredreferenceratesnode_55787e85c3f036d0c36786691e56589e`
- persisted FRED observations: 334 rows across all five configured identities
  verified through the standard time-series read endpoint
- FRED coverage starts at `2026-04-20`; Treasury series contain 61 rows each
  through `2026-07-16`, and the Fed target-upper series contains 90 rows through
  `2026-07-18`

FRED live-source verification is complete. Banxico source verification, bounded
backfill verification, the first non-namespaced incremental run, and scheduled-job
verification remain pending.

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
