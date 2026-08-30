# Deployment

## Required Sequence

Run these commands in a networked, authenticated shell before backend
verification:

```bash
mainsequence code-repository update-sdk --path .
mainsequence code-repository refresh-token --path .
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository update AGENTS.md --path .
mainsequence code-repository sync --path . -m "Sync Valmer control-plane backend"
```

The approved control-plane Jobs are declared in
`.mainsequence/workflows/valmer-control-plane-jobs.yaml`. The standard pipeline
is the only scheduled Job; leaf Jobs are manual recovery operations.

The first backend sync indexes `src/apis/valmer_control_plane/app.py`. Resolve that
FastAPI resource's public UID, add the API `resource_release` declaration using
that real UID, validate it through the branch workflow endpoint, and run a
second canonical sync. The declaration must enable every-commit automatic
redeployment and retain three immutable revisions. The backend then owns exact
image creation; do not select a caller image for this workflow-owned release.

The Vite repository contains
`.mainsequence/workflows/valmer-control-plane-static-site.yaml`. It uses the
same every-commit promotion policy and retains three revisions. Synchronize the
static repository only after the FastAPI release exists and the installed
Command Center SDK's exact stable release target has been configured. See
`docs/control-plane.md` for the two-repository release order.

## Verification Commands

After sync and image creation, verify the deployed state with:

```bash
mainsequence code-repository current --debug
mainsequence code-repository jobs list --path . --timeout 60
mainsequence code-repository time-index-table-updates list --timeout 60
mainsequence code-repository resources list --path . --timeout 60
mainsequence code-repository images list --path . --timeout 60
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

Use the Vite control plane after both ResourceReleases are ready to confirm:

- SDK iframe initialization and host theme propagation;
- data-product and asset resource pagination;
- viewer read access and viewer launch denial;
- operator Job discovery, preflight, typed confirmation, and execution; and
- JobRun polling after a launch.

For both releases, verify that `automatic_deployment` is enabled, the nested
tag policy is null (every synchronized commit), `revision_retention_count` is
three, and `active_revision` advances only after the desired revision is ready.
Inspect the correlated DeploymentRun; a successful Git push is not deployment
evidence.

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
