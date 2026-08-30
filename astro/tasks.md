# Active Tasks

## SDK 8 Compatibility, Live Verification, And Deployment

- Upgrade `ms-markets` when a release compatible with Main Sequence SDK 8.0.4
  is published; `0.0.114` still pins SDK 7.0.2.
- Refresh CodeRepository authentication with
  `mainsequence code-repository refresh-token --path .`.
- Refresh the SDK/platform skill scaffold, then sync the current repository
  head and build a CodeRepository image.
- Migrate the legacy root `scheduled_jobs.yaml` declaration to the canonical
  `.mainsequence/workflows/` contract before publishing scheduled jobs.
- Re-run backend verification with `code-repository current`, `jobs list`,
  `time-index-table-updates list`, `resources list`, `images list`, and
  `markets portfolios list`.
- Remove or explicitly document the non-canonical historical jobs that still
  clutter this CodeRepository.

## Runtime Follow-Up

- Run `python scripts/validate_runtime.py` against the live backend and confirm
  the TIIE curve loads and the sample bond prices successfully.
- Decide whether fixing-rate ETLs stay outside this repository or should be
  added here as first-class project-owned builders.
