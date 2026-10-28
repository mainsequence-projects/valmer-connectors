# Time-Series Instances

Valmer produces three Markets time series besides its own tables: the discount
curves (TIIE overnight, USD SOFR overnight, USD/MXN cross-currency, MXN
government), the Banxico TIIE and CETE fixings, and the canonical daily Index
values (Valmer curve quotes, FRED Treasury yields and Fed Funds target, the
Banco de Mexico policy target). ms-markets ADR 0047 has each provider keep such
series in its own namespace, migrated by the provider: curves and fixings as
instances of the Markets storage contract, found by Markets through the default
pricing market-data set's bindings; Index values in the provider's own
Index-value table, found by Markets' Index dataset catalog (decision 4).

From 0.2.0 on Valmer writes and reads only its own tables. There is no switch:
the release that ships them is the one that starts writing them. This page is
the runbook for deploying it, which copies the history once from the Markets
tables, and for rolling it back. Since 0.3.0 the copy covers curves and fixings
only; daily Index values have no move (see "Daily Index Values"). Run nothing
here before reading it to the end.

## What Exists

| Series | Valmer table | Markets table it replaces (copy source) | Found by Markets through |
| --- | --- | --- | --- |
| Discount curves | `valmer_connectors__discountcurvests__valmer` (`valmer_connectors.DiscountCurvesTS.valmer`, revision `0002`) | `ms_markets__discountcurvests` (`DiscountCurvesTS`) | the default market-data set's `discount_curves` binding |
| Index fixings | `valmer_connectors__indexfixingsts__valmer` (`valmer_connectors.IndexFixingsTS.valmer`, revision `0002`) | `ms_markets__indexfixingsts` (`IndexFixingsTS`) | the `interest_rate_index_fixings` binding |
| Daily Index values | `valmer_connectors__index_values__t_1d` (`valmer_connectors.IndexValuesTS.1d`, revision `0003`) | none since 0.3.0 (see "Daily Index Values") | the Index dataset catalog, which discovers it |

The curve and fixing instances are built with
`msm.storage_instances.configured_time_index_instance`, the Index-value table
with `configured_index_values_storage(cadence="1d", namespace=, storage_app=,
base=)` (`src/valmer_connectors/storage.py`,
`src/valmer_connectors/data_nodes/canonical_index_values.py`): the Markets
table's columns, unique time index, secondary indexes and foreign keys (to
`ms_markets__curve`, `ms_markets__index` and `ms_markets__indexformuladefinition`),
in the `valmer_connectors` namespace. In code they are
`valmer_discount_curves_instance()`, `valmer_index_fixings_instance()` and
`ValmerDailyIndexValuesStorage`. Every producer, reader, the attachment
(`bootstrap_runtime()`) and `registry-seed` name them directly. The Markets
curve and fixing tables appear only in the one-off move
(`valmer_connectors.services.time_series_backfill`), as copy source and
rollback target.

## Daily Index Values

Daily Index values have no move since 0.3.0. Revision `0001` still creates the
Markets table `ms_markets__index_values__t_1d` where it has never run, because
applied revisions are immutable, but Valmer's migration provider no longer
registers that table, so a fresh Environment migrates without any role on
`mainsequence.markets` (valmer-connectors#4). Valmer neither writes nor reads
it.

An Environment that still holds Valmer rows in `ms_markets__index_values__t_1d`
(the FRED and Banxico policy identifiers and `VALMER_CURVE_QUOTE.*`) copies
them before it deploys 0.3.0, with the Index-value move of 0.2.3: from a 0.2.3
checkout, `valmer-connectors time-series backfill --series index_values
--execute`. The curve quotes come from same-day source files and cannot be
rebuilt; the FRED and Banxico policy producers load five years from their
sources when their table is empty.

An output table is not part of an updater's `update_hash`, so most producers
keep their hashes: each gets its own update record for Valmer's table and
continues from that table's last observation, which the copy provides. Five
hashes rotate once with this release, and their data continues from the
copied rows too:

- configurations that name a table they read: the TIIE, SOFR and
  cross-currency curves name the Index-value table they read quotes from
  (`quote_storage_table`), and the cross-currency curve the curve table it
  reads its upstream curves from (`curve_storage_table`). A storage class in a
  configuration is hashed by its registered table uid;
- configurations that embed an ms-markets configuration class that
  ms-markets 3.0 moved to `msm_pricing.time_index` (ADR 0048): the Banxico
  fixings (`IndexFixingConfiguration`) and the MXN government curve
  (`CurveConfig`). A configuration object is hashed with its class's module
  and name; `msm rename-preflight rotation` reports their pre-3.0 hashes.

Each Index-value producer reconciles the Index dataset catalog after a run as
before; the catalog discovers the table it wrote.

Out of scope: pricing details stay as they are. They are written through the
Markets API (`AssetPricingDetails.add_many`), which writes the core table and
the current pricing details together, and Markets has no catalog through which
it would find a provider instance of them.

## Before You Start

- The Valmer Team holds table `editor` on `ms_markets__discountcurvests` and
  `ms_markets__indexfixingsts`, and `metadata_viewer` on the tables they
  reference ("Teams And Namespaces" in `docs/deployment.md`): the copy reads
  them, and its rollback writes them.
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

## Steps

1. **Hold the schedules.** Disable the schedule of `standard-pipeline-refresh`,
   the only scheduled Job, and until step 7 launch no Valmer Job but the ones
   the steps below name.
   A push re-enables a Job's schedule when it activates the new image
   ("Workflow Declarations" in `docs/deployment.md`), so check it again right
   after the push, or run the whole procedure between two scheduled runs
   (`0 13 * * 1-5`).
2. **Push.** `migrate-valmer` applies revisions `0002` and `0003` before the
   API rolls out (`metatables migrations current --provider
   valmer_connectors.migrations:migration` reports `0003`). Every Job takes the
   new image and from then on writes and reads only Valmer's tables, which are
   still empty.
3. **Grant readers.** Grant `data_viewer` on Valmer's three tables to their
   readers outside the Valmer Team: the Markets workload on all three (Markets
   reads curves and fixings by the binding's table uid, and its Index dataset
   catalog and value reads use the Index-value table), every application that
   prices with `msm_pricing` through the default market-data set, which reads
   the bound curve and fixing tables with its own identity, and every
   application that reads Valmer's daily Index values directly (on
   `valmer_connectors__index_values__t_1d`). A reader without the grant fails
   as soon as it reads Valmer's tables: pricing once the bindings move in step
   5.
4. **Copy the history.** Launch `time-series-backfill` (Valmer Time-Series
   Backfill) from the control plane with "Copy rows" set, or run
   `valmer-connectors time-series backfill --execute` where the Valmer
   workload's grants apply. It moves the curves and the fixings (`--series`
   selects one) with ms-markets' shared copy
   (`msm.storage_copies`): it reads both tables through MetaTables, copies, and
   never recomputes, every row of Valmer's identities that Valmer's table lacks
   (with empty update statistics, so a later pass fills gaps), then prints per
   identity the rows in the source, the target and the instance, the rows still
   missing and the rows copied, and fails if any row is missing. Its report
   must show `"complete": true`. Without "Copy rows" (`--execute`) it only
   counts; a counting run first shows what will be copied. The TIIE, SOFR and
   cross-currency curves come from same-day source files and cannot be rebuilt,
   so the copy is the only source of their history. The same moves run with
   ms-markets' own command:

   ```bash
   msm series move valmer_connectors.services.time_series_backfill:valmer_series_moves
   msm series move valmer_connectors.services.time_series_backfill:valmer_series_moves --execute
   ```
5. **Rebind.** Launch `registry-seed`. It points the default market-data set's
   `discount_curves` and `interest_rate_index_fixings` bindings at Valmer's
   instances (their `storage_table_uid`).
6. **Verify.** Launch `pipeline-verification`, which reads Valmer's tables, and
   check that pricing through the default market-data set finds the curves (for
   example `Curve.get_discount_curve_nodes` through the Markets API).
7. **Resume.** Enable the schedule of `standard-pipeline-refresh` again. Its
   first stage rebinds as `registry-seed` did, and its producers continue from
   the copied rows (the TIIE, SOFR, cross-currency and MXN government curves
   and the Banxico fixings under their new update hashes). After that run, check that the Markets Index catalog lists
   `valmer_connectors.IndexValuesTS.1d` as a dataset of Valmer's Indexes with
   values (`Index.list_datasets` or `GET /api/v1/index/{uid}/datasets/`).
8. **Clean up (a later release).** After a soak period, a release removes the
   one-off move (and with it the rollback). The Valmer Team then drops its
   table roles on `ms_markets__discountcurvests` and
   `ms_markets__indexfixingsts` and its `metadata_viewer` on the tables they
   reference, and Valmer holds nothing in `mainsequence.markets`.
   `migrate-valmer` needs no role there since 0.3.0. Valmer's old rows stay in
   the Markets tables: deleting them is Markets' decision.

## Rolling Back

Before step 8, while the Valmer Team still holds its roles on the Markets tables
(the rollback reads Valmer's tables and writes the Markets tables):

1. **Hold the schedules** as in step 1.
2. **Deploy the previous release.** It writes the Markets tables and
   registries with its own MetaTables grants (Writer on `mainsequence.markets`
   in its deployment guide) and pins its own ms-markets release, so restore
   those grants and check that the Environment's Markets deployment still
   serves it first. Its processes seed the default bindings onto the Markets
   tables again when they start.
3. **Copy the rows Valmer's tables gained back.** The previous release has no
   backfill Job, so run the rollback from a checkout of this release where the
   Valmer Team's grants apply: `valmer-connectors time-series backfill
   --rollback --execute`, or `msm series rollback
   valmer_connectors.services.time_series_backfill:valmer_series_moves
   --execute`. Like the forward copy it writes only the rows the Markets tables
   lack and fails while a row is missing. Daily Index values are not copied
   back since 0.3.0; Valmer's table keeps them.
4. **Resume** the schedule.

Valmer's tables keep their rows; nothing deletes them.
