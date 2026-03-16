# Active Tasks

## Live Verification And Deployment

- Upgrade the local SDK with `mainsequence project update-sdk --path .` in a
  networked shell.
- Refresh project JWTs with `mainsequence project refresh_token --path .`.
- Sync the current repo head, build a project image, and submit
  `scheduled_jobs.yaml`.
- Re-run backend verification for project `113` with:
  `project current`, `jobs list`, `data-node-updates list`,
  `project_resource list`, `images list`, and `markets portfolios list`.
- Remove or explicitly document the non-canonical historical jobs that still
  clutter project `113`.

## Runtime Follow-Up

- Run `python scripts/validate_runtime.py` against the live backend and confirm
  the TIIE curve loads and the sample bond prices successfully.
- Decide whether fixing-rate ETLs stay outside this repository or should be
  added here as first-class project-owned builders.
