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
python scripts/verify_current_pipeline.py
```

Use the dashboard after deployment to confirm:

- source node coverage
- pricing hydration
- curve publication health
- external reference-rate coverage and freshness

## Current Platform Verification

The local MetaTable data source was rebuilt on 2026-08-21. The ms-markets
provider is at revision `0015`; the Valmer provider is at its clean
current-schema baseline `0001`. The Valmer provider owns exactly three tables:

- `valmer_connectors__valmerassetdetails`;
- `valmer_connectors__vector_de_precios_valmer`;
- `ms_markets__index_values__t_1d`.

There is no obsolete reference-rate table, compatibility storage, or migration
decoder. After recreation, FRED, Banxico policy, Banxico fixing, both Valmer
quote producers, TIIE, SOFR, XCCY, vector history, and the government curve were
run again.

The final governed-storage audit on 2026-08-21 verified:

- 8,633 reference-rate observations and 81 Valmer quote observations in
  `IndexValuesTS.1d`;
- 26,430 observations across seven Banxico fixing indices;
- 13,083 government-vector observations over 248 dates, including 9,029 CETES
  and 4,054 M Bonos rows;
- 248 government curves and one current row for each of TIIE, SOFR, and XCCY;
- all 55 quote-backed key nodes resolve to exact-date Index observations;
- all 12,761 government key nodes resolve to exact-date Asset observations.

`scripts/verify_current_pipeline.py` is the repeatable verification command. It
fails on missing rows, unexpected counts, truncated governed queries, invalid
key-node schemas, or unresolved source references.
