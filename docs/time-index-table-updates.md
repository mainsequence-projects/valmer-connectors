# TimeIndexTableUpdaters

This page documents the time-index table publication boundary only. Asset registration,
static asset details, and pricing hydration are separate workflows documented in
`markets.md` and `pricing.md`.

## Current TimeIndexTableUpdater

`ImportValmer` in `src/valmer_connectors/data_nodes/nodes.py` publishes Valmer
vector observations into the storage class:

- `ValmerVectorPricesStorage`
- `__metatable_identifier__ = "valmer_connectors.vector_de_precios_valmer"`
- authored storage identifier: `vector_de_precios_valmer`
- project namespace: `valmer_connectors`
- storage app segment: `valmer_connectors`
- observation cadence: daily Valmer source vector data

The storage class lives in:

- `src/valmer_connectors/data_nodes/valmer_vector_storage.py`

The updater is built by the package service:

- `src/valmer_connectors/services/vector_update.py`
- `build_import_valmer(...)`

The operational entry point is:

- `valmer-connectors vector update`

`scripts/update_vector_valmer.py` is only a compatibility wrapper around the
package service.

## Storage Relationship

`ValmerVectorPricesStorage` is an asset-indexed time-series table. It stores
time-varying observations keyed by Valmer's canonical project asset identifier.

```text
+------------------------------------+
| AssetTable                         |
|------------------------------------|
| uid                                |
| unique_identifier UNIQUE           |
| asset_type                         |
+------------------------------------+
              ^
              |
              | FK: unique_identifier
              |
+------------------------------------+
| ValmerVectorPricesStorage          |
|------------------------------------|
| time_index                         |
| asset_identifier                   |
| valuation_date                     |
| clean_price, dirty_price           |
| accrued_interest                   |
| yield, spread, duration, risk      |
+------------------------------------+
```

Row grain:

```text
(time_index, asset_identifier)
```

`time_index` is the UTC end-of-day timestamp derived from the source valuation
date. `asset_identifier` stores the Valmer asset key built as:

```text
tipovalor_emisora_serie
```

`asset_identifier` is the canonical `ms-markets` asset-indexed time-index table
dimension. Its value is still `AssetTable.unique_identifier`; only the time-index table
dimension name follows the current `ms-markets` contract.

## What The TimeIndexTableUpdater Publishes

The updater output table stores fields that can change from one Valmer vector date to the
next:

- valuation fields: `valuation_date`, `clean_price`, `dirty_price`,
  `accrued_interest`
- coupon/current state: `current_coupon`, `spread`, `amount_outstanding`,
  `adjusted_face_value`
- bid/ask and theoretical values: `theoretical_price`, `posted_bid`,
  `posted_ask`, `bid_yield`, `ask_yield`, `bid_spread`, `ask_spread`
- risk and analytics: `duration`, `monetary_duration`, `macaulay_duration`,
  `convexity`, `value_at_risk`, `standard_deviation`, `sensitivity`,
  `yield_rate`
- time-varying vendor state: ratings, marketability, liquidity, suspension
  status, market event, and change percentages

## Public Vector Query API

Downstream projects should read published Valmer vector storage through:

```python
from valmer_connectors.queries import (
    filter_valmer_vector_columns,
    latest_dirty_price_by_identifier,
    normalize_valmer_quote_frame,
    read_valmer_history,
    read_valmer_last_observation,
    read_valmer_yield_history,
    valmer_vector_table_ref,
    valmer_vector_table_identifier,
    valmer_vector_storage_columns,
)
```

The query API is read-only. It derives the published node identifier from
`ValmerVectorPricesStorage.__metatable_identifier__`, so callers do not need a
project-local vector identifier setting.

Core helpers:

- `valmer_vector_table_identifier()`: returns the canonical published identifier
  for the Valmer vector storage.
- `valmer_vector_table_ref()`: builds an `TimeIndexTableRef` from the runtime-bound
  Valmer vector storage MetaTable.
- `valmer_vector_storage_columns()`: returns the registered storage columns from
  `ValmerVectorPricesStorage.__table__`.
- `filter_valmer_vector_columns(...)`: keeps `time_index`,
  `asset_identifier`, and requested columns that exist in the storage schema.
- `read_valmer_history(...)`: reads historical rows for a set of
  `asset_identifier` values with `get_df_between_dates(...)`.
- `read_valmer_last_observation(...)`: uses the backend latest-observation
  endpoint to return the latest row per `asset_identifier` at or before an
  `as_of` timestamp.
- `latest_dirty_price_by_identifier(...)`: returns latest dirty prices keyed by
  canonical asset identifier.
- `read_valmer_yield_history(...)`: returns yield, duration, and monetary
  duration history without introducing spread-analysis assumptions.
- `normalize_valmer_quote_frame(...)`: resets Valmer index columns to ordinary
  columns, normalizes `time_index` to UTC, and coerces numeric vector fields.

Example:

```python
from datetime import datetime, UTC

from valmer_connectors.queries import read_valmer_last_observation


latest = read_valmer_last_observation(
    ["M_BONOS_241205", "BI_CETES_1"],
    as_of=datetime(2024, 12, 31, tzinfo=UTC),
    columns=["dirty_price", "yield_rate", "duration"],
)
```

This helper layer does not change the updater process and does not
modify `ValmerVectorPricesStorage`.

## Public Spread Analytics Helpers

Spread-oriented analytics should use the helper layer in:

```python
from valmer_connectors.analytics import (
    SPREAD_SNAPSHOT_COLUMNS,
    default_start_date,
    fetch_market_snapshot,
    fetch_yield_history,
)
```

These helpers are convenience wrappers over `valmer_connectors.queries`. They do
not read the vector time-index table directly, do not define storage identifiers, and do
not duplicate identifier cleanup.

Core helpers:

- `default_start_date()`: returns a five-year UTC lookback start date for
  exploratory spread analysis.
- `fetch_yield_history(...)`: reads Valmer yield history through
  `read_valmer_yield_history(...)` and returns a wide frame indexed by
  `time_index`, with one column per asset identifier.
- `fetch_market_snapshot(...)`: reads latest Valmer market fields through
  `read_valmer_last_observation(...)` and coerces the spread snapshot fields to
  numeric values.

Example:

```python
from valmer_connectors.analytics import fetch_market_snapshot, fetch_yield_history


identifiers = ["M_BONOS_241205", "BI_CETES_1"]
yield_history = fetch_yield_history(identifiers)
snapshot = fetch_market_snapshot(identifiers)
```

This analytics layer is intentionally separate from the storage query API. It
can be used by spread models, notebooks, dashboards, or downstream projects
without making spread analysis part of the core Valmer vector storage contract.

## What The TimeIndexTableUpdater Does Not Own

These concerns are intentionally outside `ImportValmer.update()`:

- source selection from bucket versus local files
- AssetTable registration
- static Valmer asset descriptors
- current pricing-detail hydration
- curve publication
- index/fixing reference-data bootstrap

The vector update service performs those steps before the updater run through
`ImportValmer.prepare_for_update()`.

## Update Flow

```text
valmer-connectors vector update
    |
    v
bootstrap_runtime()
    |
    v
build_import_valmer()
    |
    v
prepare_for_update()
    |
    +-- load Valmer source rows
    +-- filter rows from last vector observation per asset_identifier
    +-- select pricing-target assets from latest source rows
    +-- sync AssetTable rows for the selected registration scope
    +-- sync ValmerAssetDetailsTable rows for registered assets
    +-- hydrate current pricing details only for supported target bonds
    +-- scope vector publication to registered target assets by default
    |
    v
run(force_update=True)
    |
    +-- get_asset_list()
    |      returns the already prepared asset scope
    |
    +-- update()
           returns the time-series Valmer vector DataFrame
```

`get_asset_list()` must stay a scope handoff. It should not register assets,
upsert detail rows, or persist pricing details.

The vector updater does not use a global latest-source-date gate. Source rows
are filtered against `ValmerVectorPricesStorage` per asset:

```text
keep row when source time_index > latest stored time_index for asset_identifier
keep row when asset_identifier has no stored vector observation
```

`update()` still applies `filter_df_by_latest_value(...)` as the final
TimeIndexTableUpdater-level row filter when a run executes.

`valmer-connectors vector update` registers and publishes only the assets that
pass the pricing-detail target filter. This keeps the
`AssetTable` registration scope aligned with the storage table foreign key:
`ValmerVectorPricesStorage.asset_identifier` points to
`AssetTable.unique_identifier`.

The broader Valmer source universe is not registered by this project. The
Valmer vector contains multiple instrument types, so full-source registration
requires a separate asset-type classifier before rows can be safely written to
`AssetTable`.

Other extension libraries should not use `ImportValmer` as a generic vector
registration pipeline. A different vector TimeIndexTableUpdater should first normalize its
own source rows, classify each row into an explicit `asset_type`, upsert the
canonical `AssetTable` rows, write any static fields to that extension's own
detail table, and only then publish time-varying observations keyed by
`asset_identifier = AssetTable.unique_identifier`.

## Source Rows

Source import behavior is documented separately in `source-import.md`.

At the updater boundary, `ImportValmer.update()` expects `self.source_data` to
already contain normalized Valmer rows with a valid `unique_identifier`. It then
coerces fields according to `VALMER_TIMESERIES_SOURCE_COLUMN_SPECS`, builds the
time-series frame, indexes it by `(time_index, asset_identifier)`, filters it
through update statistics, and returns the final DataFrame.

## Current Operational Entry Point

Run:

```bash
valmer-connectors vector update
```

For local source files instead of the platform artifact bucket:

```bash
valmer-connectors vector update --debug-artifact-path /path/to/valmer/files
```

`scripts/update_vector_valmer.py` remains a compatibility wrapper.
