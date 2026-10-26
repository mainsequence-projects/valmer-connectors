# Time-Series Instances

Valmer produces three Markets time series besides its own tables: the discount
curves (TIIE overnight, USD SOFR overnight, USD/MXN cross-currency, MXN
government), the Banxico TIIE and CETE fixings, and the canonical daily Index
values (Valmer curve quotes, FRED Treasury yields and Fed Funds target, the
Banco de Mexico policy target). ms-markets ADR 0047 has each provider keep such
series in its own namespace, migrated by the provider: curves and fixings as
instances of the Markets storage contract, found by Markets through the default
pricing market-data set's bindings; Index values in the provider's own
Index-value table, found by Markets' Index dataset catalog (decision 4). This
page is the runbook for moving Valmer's three series there. Run nothing here
before reading it to the end.

## What Exists

| Series | Markets table (today) | Valmer table | Found by Markets through |
| --- | --- | --- | --- |
| Discount curves | `ms_markets__discountcurvests` (`DiscountCurvesTS`) | `valmer_connectors__discountcurvests__valmer` (`valmer_connectors.DiscountCurvesTS.valmer`, revision `0002`) | the default market-data set's `discount_curves` binding |
| Index fixings | `ms_markets__indexfixingsts` (`IndexFixingsTS`) | `valmer_connectors__indexfixingsts__valmer` (`valmer_connectors.IndexFixingsTS.valmer`, revision `0002`) | the `interest_rate_index_fixings` binding |
| Daily Index values | `ms_markets__index_values__t_1d` (`IndexValuesTS.1d`, created by Valmer's revision `0001`) | `valmer_connectors__index_values__t_1d` (`valmer_connectors.IndexValuesTS.1d`, revision `0003`) | the Index dataset catalog, which discovers it |

The curve and fixing instances are built with
`msm.storage_instances.configured_time_index_instance`, the Index-value table
with `configured_index_values_storage(cadence="1d", namespace=, storage_app=,
base=)` (`src/valmer_connectors/storage.py`,
`src/valmer_connectors/data_nodes/canonical_index_values.py`): the Markets
table's columns, unique time index, secondary indexes and foreign keys (to
`ms_markets__curve`, `ms_markets__index` and `ms_markets__indexformuladefinition`),
in the `valmer_connectors` namespace. In code they are
`valmer_discount_curves_instance()`, `valmer_index_fixings_instance()` and
`ValmerDailyIndexValuesStorage`; the Markets Index-value table is
`MarketsDailyIndexValuesStorage`, and `DailyIndexValuesStorage` is a deprecated
alias of `ValmerDailyIndexValuesStorage`.

`VALMER_TIME_SERIES_STORAGE` selects which one every Valmer process uses:

| Value | Producers write | Curve builders, queries, verification, control plane, dashboards read | `registry-seed` binds | Attached |
| --- | --- | --- | --- | --- |
| `markets` (default, unset) | Markets tables | Markets tables | the Markets tables | Markets tables |
| `valmer` | Valmer's tables | Valmer's tables | Valmer's instances | Valmer's tables only, nothing in `mainsequence.markets` |

Switching never changes an updater's `update_hash`: output tables are not part
of it. Each producer bound to Valmer's table gets its own update record
(MetaTables keys update records by hash and output table), and continues from
that table's last observation. The exceptions are configurations that name a
table they read: the TIIE, SOFR and cross-currency curves name the Index-value
table they read quotes from (`quote_storage_table`), and the cross-currency
curve the curve storage it reads its upstream curves from
(`curve_storage_table`), so their hashes change with the switch, and so do
their update records. Each Index-value producer reconciles the Index dataset
catalog after a run as before; the catalog discovers whichever table it wrote.

Out of scope: pricing details stay as they are. They are written through the
Markets API (`AssetPricingDetails.add_many`), which writes the core table and
the current pricing details together, and Markets has no catalog through which
it would find a provider instance of them.

## Before You Start

- This release (`0.2.0`) is deployed and `migrate-valmer` has applied
  revisions `0002` and `0003` in the Environment (`metatables migrations current
  --provider valmer_connectors.migrations:migration` reports `0003`). Until the
  switch Valmer's tables are empty and nothing writes them.
- The Valmer Team still holds its table roles on `ms_markets__discountcurvests`,
  `ms_markets__indexfixingsts` and `ms_markets__index_values__t_1d`: the copy
  reads them.
- Grant `data_viewer` on Valmer's three tables to their readers. On the curve
  and fixing instances: the Markets workload (Markets reads curves and fixings
  by the binding's table uid) and every application that prices with
  `msm_pricing` through the default market-data set, which reads the bound
  table with its own identity. On `valmer_connectors__index_values__t_1d`: the
  Markets workload (the Index dataset catalog and its value reads) and every
  application that reads Valmer's Index values directly, for example through
  `valmer_connectors.storage.daily_index_values_storage()` or the quote
  queries. A reader without the grant fails once the switch is made.
- Applications that import `DailyIndexValuesStorage` from Valmer now get
  Valmer's table (with a `DeprecationWarning`); import
  `MarketsDailyIndexValuesStorage` until the switch, or use the selector.
- The default market-data set has one `discount_curves` and one
  `interest_rate_index_fixings` binding. Moving them to Valmer's instances moves
  the whole set: rows another producer wrote to the core tables are no longer
  read through it. Check the core tables for non-Valmer identities first:

  ```sql
  SELECT curve_identifier, count(*) FROM ms_markets__discountcurvests
  GROUP BY curve_identifier;
  SELECT index_identifier, count(*) FROM ms_markets__indexfixingsts
  GROUP BY index_identifier;
  ```

  Valmer's are `VALMER_TIIE_OVERNIGHT`, `VALMER_MXN_GOVERNMENT_BOND`,
  `VALMER_USD_SOFR_OVERNIGHT`, `VALMER_MXN_USD_COLLATERAL_DISCOUNT` and the
  seven `TIIE_*` and `CETE_*` fixings. Anything else needs its own binding or
  set before you continue. The Index-value move needs no such check: only
  Valmer writes `ms_markets__index_values__t_1d`, and the move copies only
  Valmer's identifiers (the FRED and Banxico policy ones and every
  `VALMER_CURVE_QUOTE.*` in the tables' statistics).
- The Markets `MARKETS_REGISTRY_WRITERS` constant lists the `registry-seed` and
  `standard-pipeline-refresh` workload Users under
  `pricing-market-data-set-binding`, the resource the seed writes the bindings
  through.

## Steps

1. **Copy the history.** Launch `time-series-backfill` (Valmer Time-Series
   Backfill) from the control plane, or run
   `valmer-connectors time-series backfill` where the Valmer workload's grants
   apply. It moves all three series (`--series` selects some) with ms-markets'
   shared copy (`msm.storage_copies`): it reads both tables through MetaTables,
   copies, and never recomputes, every row of Valmer's identities that Valmer's
   table lacks (with empty update statistics, so a later pass fills gaps), then
   prints per identity the rows in the source,
   the target and the instance, the rows still missing and the rows copied, and
   fails if any row is missing. `--dry-run` (the control plane's "Only count")
   only reports. The TIIE, SOFR and cross-currency curves come from same-day
   source files and cannot be rebuilt, so the copy is the only source of their
   history. The same moves run with ms-markets' own command, which prints a
   table and copies only with `--execute`:

   ```bash
   msm series move valmer_connectors.services.time_series_backfill:valmer_series_moves
   msm series move valmer_connectors.services.time_series_backfill:valmer_series_moves --execute
   ```
2. **Switch.** Add the variable to every resource of
   `.mainsequence/workflows/valmer-control-plane-api.yaml` except
   `migrate-valmer`, in the same push:

   ```yaml
       spec:
         env_vars:
           - name: VALMER_TIME_SERIES_STORAGE
             value: valmer
   ```

   (the API already has an `env_vars` list; add the entry to it). Every Job
   takes the new image on that push and from then on writes and reads Valmer's
   tables, and the Index-value producers' catalog reconcile records Valmer's
   table.
3. **Rebind.** Right after the push, launch `registry-seed`. It points the
   default market-data set's `discount_curves` and `interest_rate_index_fixings`
   bindings at the instances (their `storage_table_uid`). The scheduled
   standard pipeline does the same in its first stage.
4. **Catch up.** Launch `time-series-backfill` again: it copies the rows the
   core tables received between steps 1 and 2. Its report must show
   `"complete": true` and no missing rows.
5. **Verify.** Launch `pipeline-verification`, which reads Valmer's tables
   now, check that pricing through the default market-data set finds the curves
   (for example `Curve.get_discount_curve_nodes` through the Markets API), and
   that the Markets Index catalog lists `valmer_connectors.IndexValuesTS.1d` as
   a dataset of Valmer's Indexes with values (`Index.list_datasets` or
   `GET /api/v1/index/{uid}/datasets/`).
6. **Revoke.** After a soak period, remove the Valmer Team's table roles on
   `ms_markets__discountcurvests` and `ms_markets__indexfixingsts` and their
   `metadata_viewer` on `ms_markets__curve`. Valmer's old rows stay in the core
   tables; deleting them is Markets' decision.
7. **Drop the legacy Index-value table from Valmer (a later release).** Once
   every Environment has switched, a release removes
   `MarketsDailyIndexValuesStorage` from Valmer's migration provider, and the
   rollback of the Index values with it; the Valmer Team and `migrate-valmer`
   then drop their roles on `ms_markets__index_values__t_1d`, and Valmer holds
   nothing in `mainsequence.markets`. The table itself stays registered there
   until Markets decides what to do with it: Valmer's provider removing a
   model does not delete its table. Until that release the migration Job keeps
   its table role on it, because migration admission checks every table of the
   provider.

## Rolling Back

Before step 6 (step 7 for the Index values), while the Valmer Team still holds
its roles on the Markets tables (the rollback reads Valmer's tables and writes
the Markets tables):

1. Set `VALMER_TIME_SERIES_STORAGE` back to `markets` (or remove it) on every
   resource of the workflow, and push. The Jobs write the core tables again.
2. Copy the rows Valmer's tables gained back: launch `time-series-backfill` with
   "Roll back" set, or run `valmer-connectors time-series backfill --rollback`
   (`msm series rollback
   valmer_connectors.services.time_series_backfill:valmer_series_moves
   --execute` does the same). Like the forward copy it writes only the rows the
   Markets tables lack, so rows the Jobs wrote after the push are kept, and it
   fails while a row is missing. The Index values go back into
   `ms_markets__index_values__t_1d`.
3. Launch `registry-seed`: it binds the default market-data set to the core
   tables again.

Valmer's tables keep their rows; nothing deletes them.
