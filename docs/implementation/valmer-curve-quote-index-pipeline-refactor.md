# Implementation Task: Canonical Index Observations and Quote-Backed Curves

## Outcome

Publish every daily reference-rate and Valmer curve-input observation to the
canonical `IndexValuesTS.1d` storage, then build TIIE, SOFR, and USD/MXN curves
only from persisted observations. Curve `key_nodes` are audit provenance and
must identify the persisted quote `Index`; they are not the quote-history store.

The required graph is:

```text
FRED + Banxico -----------------------------> IndexValuesTS.1d

Valmer IRS_MXN_CURVE.csv
  -> ValmerIrsMxnIndexValuesNode
  -> IndexValuesTS.1d
  -> ValmerTiieDiscountCurveNode
  -> DiscountCurvesStorage[VALMER_TIIE_OVERNIGHT]

Valmer IRS_USD_CURVE.csv
  -> ValmerIrsUsdIndexValuesNode
  -> IndexValuesTS.1d
  -> ValmerUsdSofrDiscountCurveNode
  -> DiscountCurvesStorage[VALMER_USD_SOFR_OVERNIGHT]

persisted MXN quotes + same-date TIIE + same-date SOFR
  -> ValmerUsdMxnCollateralDiscountCurveNode
  -> DiscountCurvesStorage[VALMER_MXN_USD_COLLATERAL_DISCOUNT]
```

## Non-Negotiable Contracts

- Quote observations are `Index` values, not `Asset` prices.
- Daily grain is `(time_index, index_identifier)`.
- `value` is canonical; percentage rates and basis points are normalized once
  to decimal.
- Source-published rows use `definition_uid = null` and carry bounded source
  metadata.
- TIIE/SOFR/XCCY builders perform no HTTP requests and receive no raw CSV bytes.
- Every quote-backed key node contains:

  ```text
  source_reference.type       = "index"
  source_reference.identifier = VALMER_CURVE_QUOTE.<stable identity>
  source_instrument_identifier = <raw Valmer identifier>
  ```

- Quote-backed key nodes must not contain a top-level `asset_identifier` or
  `index_identifier`.
- Government-bond helpers remain Asset-backed because CETES and M Bonos are
  real registered instruments.
- `FED_FUNDS_TARGET_UPPER` is a policy target. It is not a Fed Funds fixing,
  OIS quote, or curve input.
- Fed Funds OIS and Fed Funds/SOFR basis rows from Valmer are persisted even
  though the SOFR helper selector excludes them and this task creates no Fed
  Funds curve.
- There is no runtime decoder, translator, export command, direct-download
  curve fallback, or dual-write path.

## Implemented Repository Changes

### Canonical daily storage

- `src/valmer_connectors/data_nodes/canonical_index_values.py` owns the single
  `configured_index_values_storage(cadence="1d")` class and common observation
  normalization.
- FRED and Banxico producers subclass the canonical node and emit
  `value`, `unit`, `definition_uid`, `observation_status`, `source_as_of`, and
  `metadata_json`.
- Runtime bootstrap and the Valmer migration provider bind the same storage
  class object.
- The obsolete project-specific reference-rate Python module is deleted.

### Valmer quote producers

- `ValmerIrsMxnIndexValuesNode` publishes all 34 recognized MXN source rows:
  15 TIIE OIS, 9 cross-currency basis, 9 FX forwards including ON/TN, and one
  USD/MXN spot observation.
- `ValmerIrsUsdIndexValuesNode` publishes all 47 recognized USD source rows:
  14 SOFR futures, 11 SOFR OIS, 10 Fed Funds OIS, and 12 Fed Funds/SOFR basis
  observations.
- Unknown or duplicate source rows fail the complete-snapshot validation.
- Source quote, source unit, source family, source file, quote side, and raw
  source identity are retained in `metadata_json`.

### Dependency-backed curve nodes

- `ValmerTiieDiscountCurveNode.dependencies()` returns the MXN quote node.
- `ValmerUsdSofrDiscountCurveNode.dependencies()` returns the complete USD
  quote node; its helper selector consumes only SOFR futures and SOFR OIS.
- `ValmerUsdMxnCollateralDiscountCurveNode.dependencies()` returns the MXN
  quote node plus the TIIE and SOFR curve nodes.
- XCCY loads the two exact-date persisted upstream curve observations and
  rejects missing dates; it does not reconstruct upstream curves from vendor
  files.
- Quote and curve storage classes are part of the hashed configurations and
  are used by the query layer.

### Execution surfaces

- CLI:
  - `valmer-connectors quotes update-irs-mxn`
  - `valmer-connectors quotes update-irs-usd`
  - `valmer-connectors curves update-tiie-irs-mxn`
  - `valmer-connectors curves update-usd-sofr`
  - `valmer-connectors curves update-usd-mxn-xccy`
- Matching scripts and VS Code launch configurations exist, including the
  government-curve rebuild from persisted vector observations.
- `scheduled_jobs.yaml` runs MXN quotes at 13:00 UTC, USD quotes at 13:05,
  TIIE at 13:10, SOFR at 13:15, XCCY at 13:20, and the government curve at
  13:25 on weekdays.

## Migration and Data-Cutover Evidence

Revision `0004` creates `ms_markets__index_values__t_1d`, copies all six
historical series, and drops the obsolete physical table in one transactional
migration.

The pre/post migration inventory matched exactly:

| Index | Rows | First UTC date | Last UTC date |
| --- | ---: | --- | --- |
| `BANXICO_POLICY_TARGET` | 1,819 | 2021-07-19 | 2026-07-18 |
| `FED_FUNDS_TARGET_UPPER` | 1,826 | 2021-07-19 | 2026-07-18 |
| `US_TREASURY_CMT_2Y` | 1,248 | 2021-07-19 | 2026-07-16 |
| `US_TREASURY_CMT_5Y` | 1,248 | 2021-07-19 | 2026-07-16 |
| `US_TREASURY_CMT_10Y` | 1,248 | 2021-07-19 | 2026-07-16 |
| `US_TREASURY_CMT_30Y` | 1,248 | 2021-07-19 | 2026-07-16 |

Total: 8,637 canonical observations. Per-index value sums also matched the
pre-migration inventory.

The obsolete physical table no longer exists. Its protected catalog row cannot
be removed with the current SDK delete flags because it is Alembic-managed. The
backend requires a confirmed provider-reset/cascade operation with
`override_schema_management_protection=true`, which SDK 4.4.32 does not expose
on `mainsequence data-node delete`. This catalog-only cleanup must be completed
through the platform's Alembic-provider retirement workflow; do not restore the
old table or runtime model to work around it.

## Verification Gates

### Completed locally

- 245 tests and 39 subtests pass.
- Pure complete-source normalization yields exactly 34 MXN and 47 USD rows.
- Pure quote-backed construction yields 15 TIIE, 24 SOFR, and 17 XCCY helpers.
- Every emitted helper quote reconciles to its canonical persisted-observation
  representation in the integration contract tests.
- Static search finds no runtime import of the removed reference-rate storage
  and no production direct-download curve builder.

### Required live run

Run in this order, stopping on the first failure:

```bash
valmer-connectors reference-rates update-fred
valmer-connectors reference-rates update-banxico-policy
valmer-connectors quotes update-irs-mxn
valmer-connectors quotes update-irs-usd
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-usd-mxn-xccy
valmer-connectors curves update-mxn-government
```

Then verify:

1. `IndexValuesTS.1d` contains all 81 Valmer quote identities for the benchmark
   date: 34 MXN plus 47 USD.
2. TIIE has 15 key nodes and SOFR has 24; XCCY has the complete selected helper
   set for the same date.
3. For every quote-backed key node, resolve `source_reference.identifier` at
   the curve date and compare canonical value, unit, source quote, source unit,
   and raw vendor identifier.
4. Every source date equals the stored observation date; no latest-value
   fallback is allowed.
5. A second normal run inserts no duplicate Index or curve keys.
6. The government curve is repopulated after the earlier exact-scope curve-data
   deletion.
7. Schedule the batch, inspect each run and its logs, and confirm that no curve
   job executes before its dependencies are available.

## Current Live Blocker

The canonical migration and row reconciliation completed. During the subsequent
live launcher run, the local `tsorm_web_local` backend auto-reloaded and failed
its Django system check because `pod_manager.DeploymentRun` is missing. Port
8000 now resets SDK requests, so the producer/curve run and catalog-only cleanup
cannot be truthfully marked complete until that unrelated backend checkout is
restored. The repository must not bypass this by writing curve rows directly.

## Completion Checklist

- [x] Canonical daily storage implemented and migration-managed.
- [x] FRED and Banxico code writes only canonical rows.
- [x] All 8,637 historical rows reconciled after migration.
- [x] Obsolete physical reference-rate table dropped.
- [x] Obsolete runtime storage code removed.
- [x] Complete MXN and USD quote nodes implemented.
- [x] TIIE/SOFR/XCCY nodes declare and consume persisted dependencies.
- [x] Key nodes use typed Index source references.
- [x] CLI, scripts, launch configurations, schedules, and tests updated.
- [ ] Remove the stale protected catalog row through the supported provider
  retirement operation.
- [ ] Restore the local Main Sequence backend and run the eight live launchers
  in the stated order.
- [ ] Verify exact live Index/key-node reconciliation and immediate no-op reruns.
- [ ] Schedule and inspect the platform jobs and logs.
