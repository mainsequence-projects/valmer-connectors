# Deployment

## Required Sequence

Run these commands in a networked, authenticated shell before backend
verification:

```bash
mainsequence project update-sdk --path .
mainsequence project refresh_token --path .
mainsequence project sync --path . -m "Sync valmer connector runtime and dashboard"
mainsequence project images create 113 --path .
mainsequence project schedule_batch_jobs scheduled_jobs.yaml 113 --path .
```

## Verification Commands

After sync and image creation, verify the deployed state with:

```bash
mainsequence project current --debug
mainsequence project jobs list 113 --timeout 60
mainsequence project data-node-updates list 113 --timeout 60
mainsequence project project_resource list 113 --path . --timeout 60
mainsequence project images list 113 --timeout 60
mainsequence markets portfolios list --timeout 60
python scripts/validate_runtime.py
```

Use the dashboard after deployment to confirm:

- source node coverage
- pricing hydration
- curve publication health

## Current Local Limitation

This pass could not execute the live MainSequence CLI flow from the sandboxed
environment because the installed SDK tries to contact
`https://main-sequence.app/orm/api/pods/job/get_job_startup_state/` during CLI
startup.

The exact commands above remain the required live verification sequence once
networked access and valid auth are available.
