# Time-Series Instances

Valmer produces two Markets time series besides its own tables: the discount
curves (TIIE overnight, USD SOFR overnight, USD/MXN cross-currency, MXN
government) and the Banxico TIIE and CETE fixings. ms-markets ADR 0047 has each
provider keep such series in its own namespace: an instance of the Markets
storage contract, migrated by the provider and found by Markets through the
default pricing market-data set's bindings. This page is the runbook for moving
Valmer's two series there. Run nothing here before reading it to the end.

## What Exists

| Series | Core Markets table (today) | Valmer instance (revision `0002`) |
| --- | --- | --- |
| Discount curves | `ms_markets__discountcurvests` (`DiscountCurvesTS`) | `valmer_connectors__discountcurvests__valmer` (`valmer_connectors.DiscountCurvesTS.valmer`) |
| Index fixings | `ms_markets__indexfixingsts` (`IndexFixingsTS`) | `valmer_connectors__indexfixingsts__valmer` (`valmer_connectors.IndexFixingsTS.valmer`) |

The instances are built with `msm.storage_instances.configured_time_index_instance`
(`src/valmer_connectors/storage.py`): the core storage's columns, unique time
index and foreign keys (to `ms_markets__curve.unique_identifier` and
`ms_markets__index.unique_identifier`), in the `valmer_connectors` namespace.

`VALMER_TIME_SERIES_STORAGE` selects which one every Valmer process uses:

| Value | Producers write | Curve builders, verification, control plane, dashboards read | `registry-seed` binds | Attached |
| --- | --- | --- | --- | --- |
| `markets` (default, unset) | core tables | core tables | nothing; it reads the existing bindings | core tables |
| `valmer` | instances | instances | the instances | instances |

Switching never changes an updater's `update_hash`: output tables are not part
of it. Each producer bound to an instance gets its own update record (MetaTables
keys update records by hash and output table), and continues from the
instance's last observation. One exception: the cross-currency curve's
configuration names the curve storage it reads its upstream TIIE and SOFR
curves from (`curve_storage_table`), so its hash changes with the switch, and so
does its update record.

Out of scope: the daily Index values table `ms_markets__index_values__t_1d`.
The Valmer migration provider creates it in the Markets namespace, and Markets
must adopt it in its own migrations before Valmer can stop creating it; until
then Valmer keeps writing it there. Pricing details also stay as they are: they
are written through the Markets API (`AssetPricingDetails.add_many`), which
writes the core table and the current pricing details together, and Markets has
no catalog through which it would find a provider instance of them.

## Before You Start

- This release (`0.2.0`) is deployed and `migrate-valmer` has applied revision
  `0002` in the Environment (`metatables migrations current --provider
  valmer_connectors.migrations:migration` reports `0002`). Until the switch the
  instances are empty and nothing writes them.
- The Valmer Team still holds its table roles on `ms_markets__discountcurvests`
  and `ms_markets__indexfixingsts`: the backfill reads them.
- Grant `data_viewer` on both instance tables to every reader of the default
  market-data set: the Markets workload (Markets reads curves and fixings by
  the binding's table uid) and every application that prices with
  `msm_pricing` through that set, which reads the bound table with its own
  identity. A reader without the grant fails once the bindings move.
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
  set before you continue.
- The Markets `MARKETS_REGISTRY_WRITERS` constant lists the `registry-seed` and
  `standard-pipeline-refresh` workload Users under
  `pricing-market-data-set-binding`, the resource the seed writes once the
  bindings point at the instances.

## Steps

1. **Copy the history.** Launch `time-series-backfill` (Valmer Time-Series
   Backfill) from the control plane, or run
   `valmer-connectors time-series backfill` where the Valmer workload's grants
   apply. It copies, and never recomputes, every row of Valmer's identities that
   an instance lacks, through the normal time-index write path, then prints per
   identity the rows in the core table, in the instance and still missing, and
   fails if any row is missing. `--dry-run` only reports. The TIIE, SOFR and
   cross-currency curves come from same-day source files and cannot be rebuilt,
   so the copy is the only source of their history.
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
   takes the new image on that push and from then on writes and reads the
   instances.
3. **Rebind.** Right after the push, launch `registry-seed`. It points the
   default market-data set's `discount_curves` and `interest_rate_index_fixings`
   bindings at the instances (their `storage_table_uid`). The scheduled
   standard pipeline does the same in its first stage.
4. **Catch up.** Launch `time-series-backfill` again: it copies the rows the
   core tables received between steps 1 and 2. Its report must show
   `"complete": true` and no missing rows.
5. **Verify.** Launch `pipeline-verification`, which reads the instances now,
   and check that pricing through the default market-data set finds the curves
   (for example `Curve.get_discount_curve_nodes` through the Markets API).
6. **Revoke.** After a soak period, remove the Valmer Team's table roles on
   `ms_markets__discountcurvests` and `ms_markets__indexfixingsts` and their
   `metadata_viewer` on `ms_markets__curve`. Valmer's old rows stay in the core
   tables; deleting them is Markets' decision.

## Rolling Back

Before step 6: remove `VALMER_TIME_SERIES_STORAGE` from the workflow and push,
then rebind the core tables with ms-markets
(`msm_pricing.bootstrap.seed_default_pricing_market_data_bindings(replace=True)`
from the Markets deployment), because Valmer under the Markets API does not
write bindings to tables it does not attach. The rows written to the instances
in between are not in the core tables; copy them back before you rebind, or
accept the gap.
