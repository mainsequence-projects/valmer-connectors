# Open Tasks

## Complete ADR 0009 live verification

- Scope: run the isolated Banxico smoke update with an explicit hash namespace.
  Owning skills: `mainsequence-data-nodes` and
  `mainsequence-markets-asset-indexed-data-nodes`. Expected output: observations for
  the configured Banxico policy index with decimal rates and no filled source gaps.
  Evidence: a successful update log plus queried storage rows for the identity.
- Scope: execute the documented bounded backfill and then normal incremental updates.
  Owning skill: `mainsequence-data-nodes`. Expected output: the requested historical
  range followed by stable normal producer identity. Evidence: storage bounds and
  per-identity progress from update logs or platform inspection.
- Scope: synchronize `scheduled_jobs.yaml` and verify both reference-rate jobs.
  Owning skill: `mainsequence-orchestration-and-releases`. Expected artifact: enabled
  weekday FRED and Banxico jobs using the normal incremental scripts. Evidence:
  platform job configuration, one successful run per job, and corresponding logs.

## Assess SDK / backend read-query mismatch

- Scope: classify the HTTP 415 returned by
  `TimeIndexMetaTable.run_query(...)` when SDK `4.4.32` sends the documented
  `text/plain` body to the local backend.
  Owning skill: `doc-bug-auditor`. Expected decision: identify whether the backend
  parser or SDK request contract must change and prepare a concrete upstream fix.
  Evidence: reproduce the 415 against MetaTable UID
  `3d112772-65e6-4fa7-9610-cc455c06aa0d` while the standard time-series read API
  continues to return the same persisted rows.

## Completed ADR 0009 platform work

- Project MetaTable migration `0003` was applied and finalized in the authenticated
  project on 2026-07-18. The reference-rate MetaTable UID is
  `3d112772-65e6-4fa7-9610-cc455c06aa0d`.
- The namespaced FRED smoke launch `adr-0009-fred-smoke` completed successfully on
  2026-07-19 using the project-readable `FRED_API_KEY` Secret. Updater hash
  `fredreferenceratesnode_55787e85c3f036d0c36786691e56589e` uploaded 334 rows.
- Standard time-series reads verified all five FRED identities: 61 rows for each
  Treasury series through `2026-07-16`, plus 90 Fed target-upper rows through
  `2026-07-18`. All five series begin at `2026-04-20`.
