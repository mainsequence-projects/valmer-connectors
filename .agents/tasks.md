# Open Tasks

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
  their normal DataNode identities.
- Owning skills: `mainsequence-data-nodes` and
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
