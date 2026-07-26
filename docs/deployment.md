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
valmer-connectors runtime validate
valmer-connectors reference-rates update-fred
valmer-connectors reference-rates update-banxico-policy
valmer-connectors quotes update-irs-mxn
valmer-connectors quotes update-irs-usd
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-usd-mxn-xccy
valmer-connectors curves update-mxn-government
```

Use the dashboard after deployment to confirm:

- source node coverage
- pricing hydration
- curve publication health
- external reference-rate coverage and freshness

## Current Platform Verification

Revision `0004` and the historical row reconciliation are complete: 8,637 FRED
and Banxico observations are present in `IndexValuesTS.1d`, and the obsolete
physical table is gone. The quote/curve launch sequence is not yet live-verified
because the local `tsorm_web_local` backend currently fails its Django system
check for the unrelated missing `pod_manager.DeploymentRun` model and resets SDK
requests. Restore that backend first, then run the commands above in order and
inspect immediate no-op reruns. The repository job batch has not yet been
synchronized; job creation, run status, and logs remain required before
scheduled production readiness is claimed.
