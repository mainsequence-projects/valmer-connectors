# External Reference-Rate Observations

This project publishes six externally supplied analytical rates through
`valmer_connectors.reference_rate_observations`. The table is separate from
pricing `IndexFixingsStorage`: Treasury constant-maturity yields and policy
targets are analytical observations, not coupon or swap fixings.

## Published Series

| Index identifier | Provider series | Meaning |
| --- | --- | --- |
| `US_TREASURY_CMT_2Y` | FRED `DGS2` | 2-year Treasury constant-maturity yield |
| `US_TREASURY_CMT_5Y` | FRED `DGS5` | 5-year Treasury constant-maturity yield |
| `US_TREASURY_CMT_10Y` | FRED `DGS10` | 10-year Treasury constant-maturity yield |
| `US_TREASURY_CMT_30Y` | FRED `DGS30` | 30-year Treasury constant-maturity yield |
| `FED_FUNDS_TARGET_UPPER` | FRED `DFEDTARU` | Federal Funds target-range upper limit |
| `BANXICO_POLICY_TARGET` | Banxico `SF61745` | Banco de Mexico policy target |

Each row has this grain and unit:

```text
(time_index, index_identifier) -> rate
```

`time_index` is nanosecond-resolution UTC and `rate` is an annualized decimal.
The source percentage is divided by `100.0` exactly once. Missing FRED `"."`
values and Banxico unavailable markers are omitted; observations are never
forward-filled.

## Required Secrets

- `FRED_API_KEY`
- `BANXICO_TOKEN`

Each producer checks the environment first and a project-readable Main Sequence
Secret second. Secrets are runtime inputs and never enter DataNode configuration,
hashes, metadata, or logs.

## Migration

Apply the core ms-markets migration before the Valmer provider migration:

```bash
mainsequence migrations upgrade --provider msm.migrations:migration head
mainsequence migrations upgrade --provider migrations:migration head
```

Revision `0003` creates
`valmer_connectors__reference_rate_observations`. `IndexTable` is included only
as foreign-key reference metadata; it is not a Valmer-owned migration model.

## Initial 90-Day Smoke

The first run against shared storage must use an explicit hash namespace:

```bash
valmer-connectors reference-rates update-fred \
  --smoke \
  --hash-namespace adr-0009-fred-smoke

valmer-connectors reference-rates update-banxico-policy \
  --smoke \
  --hash-namespace adr-0009-banxico-smoke
```

With no persisted progress, each producer requests exactly the most recent 90
inclusive calendar days ending yesterday UTC. Metadata validation runs before
publication. Confirm all six identities, latest dates, plausible decimal rates,
and the absence of duplicate keys before any non-namespaced run.

## Five-Year Backfill

Record the smoke request start date. Backfill only the older missing range,
ending on the day before that smoke start:

```bash
valmer-connectors reference-rates update-fred \
  --backfill-start <FIVE_YEAR_START>T00:00:00Z \
  --backfill-end <SMOKE_START_MINUS_ONE_DAY>T00:00:00Z

valmer-connectors reference-rates update-banxico-policy \
  --backfill-start <FIVE_YEAR_START>T00:00:00Z \
  --backfill-end <SMOKE_START_MINUS_ONE_DAY>T00:00:00Z
```

Both timestamps are required, inclusive, timezone-aware, and hashed. A bounded
backfill therefore has a deterministic update identity distinct from normal
incremental execution. Revalidate the complete 1Y, 3Y, and 5Y windows after the
backfill.

## Normal Incremental Jobs

`scheduled_jobs.yaml` defines independent jobs:

- `banxico-policy-target-refresh` at `13:20 UTC` on weekdays
- `fred-reference-rates-refresh` at `23:30 UTC` on weekdays

The scripts leave backfill bounds unset. Each identity starts one calendar day
after its own latest persisted observation. Provider failures remain isolated
because the jobs execute separately.

## Repairing Revised Source History

Repairs must reset only the affected identity tail before republishing:

```python
ReferenceRateObservationsStorage.get_time_index_meta_table().delete_after_date(
    "<INCLUSIVE_UTC_CUTOFF>",
    dimension_filters={"index_identifier": ["<INDEX_IDENTIFIER>"]},
)
```

Do not use raw SQL or an unscoped `delete_after_date(None)`.

## Verification

After the code is pushed and an image is available:

```bash
mainsequence project schedule_batch_jobs scheduled_jobs.yaml <IMAGE_ID> --path .
mainsequence project jobs list
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
mainsequence data-node list
```

Production readiness requires authenticated migration application, live source
metadata checks, the namespaced smoke, the bounded five-year backfill, and job
run/log inspection. Local tests alone do not prove those platform outcomes.

Spread construction remains outside these producers. Applications select the
M-Bond benchmark, inner-join common observation dates, calculate basis-point
spreads and statistics, and apply explicit policy-rate as-of rules.
