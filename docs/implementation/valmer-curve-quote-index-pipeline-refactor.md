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
- FRED and Banxico producers subclass the canonical node and emit `value`,
  `definition_uid`, `observation_status`, `source_as_of`, and `metadata_json`.
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
- Canonical quote unit, source quote, source unit, source family, source file,
  quote side, and raw source identity are retained in `metadata_json`.

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

The data source was recreated from the current schema rather than translated.
ms-markets revision `0015` was applied first. Valmer revision `0001` then
created only `ValmerAssetDetailsTable`, `ValmerVectorPricesStorage`, and
`IndexValuesTS.1d`, with current Index and formula-definition foreign keys.
There is no project-specific reference-rate table or migration-only catalog
object.

The first source repopulation after recreation produced:

| Producer | Stored rows |
| --- | ---: |
| FRED reference rates | 6,814 |
| Banxico policy target | 1,819 |
| Banxico supported fixings | 26,430 |
| Valmer MXN curve quotes | 34 |
| Valmer USD curve quotes | 47 |

These are source-run results, not copied rows.

## Verification Gates

### Completed

- The current-only migration heads are ms-markets `0015` and Valmer `0001`.
- Pure complete-source normalization yields exactly 34 MXN and 47 USD rows.
- Persisted curve construction yields 15 TIIE, 23 SOFR, and 17 XCCY key nodes.
- Every emitted helper quote reconciles to its canonical persisted-observation
  representation at the exact source date.
- All 12,761 government key nodes resolve to the exact Asset observation used
  by the curve snapshot.
- Government vector history contains 13,083 observations over 248 dates:
  9,029 CETES rows and 4,054 M Bonos rows.
- Curve storage contains 248 government rows and one current row for each of
  TIIE, SOFR, and XCCY.
- Static search finds no runtime import of the removed reference-rate storage
  and no production direct-download curve builder.
- `scripts/verify_current_pipeline.py` performs the governed-storage audit and
  rejects truncated query results.

### Reproduction order

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

Then run `python scripts/verify_current_pipeline.py`. It verifies:

1. `IndexValuesTS.1d` contains all 81 Valmer quote identities for the benchmark
   date: 34 MXN plus 47 USD.
2. TIIE has 15 key nodes and SOFR has 23; XCCY has the complete selected helper
   set for the same date.
3. For every quote-backed key node, resolve `source_reference.identifier` at
   the curve date and compare canonical value, metadata `quote_unit`, source
   quote, source unit, and raw vendor identifier.
4. Every source date equals the stored observation date; no latest-value
   fallback is allowed.
5. The government curve is repopulated after the earlier exact-scope curve-data
   deletion.
6. Schedule the batch, inspect each run and its logs, and confirm that no curve
   job executes before its dependencies are available.

## Completion Checklist

- [x] Canonical daily storage implemented and migration-managed.
- [x] FRED and Banxico code writes only canonical rows.
- [x] Current-only MetaTables recreated from clean migration baselines.
- [x] FRED, Banxico policy, and Banxico fixing histories repopulated from source.
- [x] Obsolete runtime storage code removed.
- [x] Complete MXN and USD quote nodes implemented.
- [x] Complete MXN and USD quote snapshots repopulated after recreation.
- [x] TIIE/SOFR/XCCY nodes declare and consume persisted dependencies.
- [x] Key nodes use typed Index source references.
- [x] CLI, scripts, launch configurations, schedules, and tests updated.
- [x] Complete all curve and vector launchers in dependency order.
- [x] Verify exact live Index/key-node reconciliation.
- [ ] Run and record immediate no-op reruns for each scheduled producer.
- [ ] Schedule and inspect the platform jobs and logs.
