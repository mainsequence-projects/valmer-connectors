# DataNodes

This page documents the DataNode publication boundary only. Asset registration,
static asset details, and pricing hydration are separate workflows documented in
`markets.md` and `pricing.md`.

## Current DataNode

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

The updater is built by:

- `scripts/update_vector_valmer.py`

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
| open, high, low, close             |
| valuation_date                     |
| clean_price, dirty_price           |
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

`asset_identifier` is the canonical `ms-markets` asset-indexed DataNode
dimension. Its value is still `AssetTable.unique_identifier`; only the DataNode
dimension name follows the current `ms-markets` contract.

## What The DataNode Publishes

The DataNode stores fields that can change from one Valmer vector date to the
next:

- synthetic OHLC fields copied from dirty price: `open`, `high`, `low`, `close`
- execution placeholders: `volume`, `open_time`
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

## What The DataNode Does Not Own

These concerns are intentionally outside `ImportValmer.update()`:

- source selection from bucket versus local files
- AssetTable registration
- static Valmer asset descriptors
- current pricing-detail hydration
- curve publication
- index/fixing reference-data bootstrap

The vector update service performs those steps before the DataNode run through
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

By default, `valmer-connectors vector update` registers and publishes only the
assets that pass the pricing-detail target filter. This keeps the
`AssetTable` registration scope aligned with the storage table foreign key:
`ValmerVectorPricesStorage.asset_identifier` points to
`AssetTable.unique_identifier`.

For diagnostics or a deliberate full-source import, run:

```bash
valmer-connectors vector update --register-all-assets
```

That option restores the broader registration scope and publishes all source
rows loaded by the vector updater.

## Source Rows

Source import behavior is documented separately in `source-import.md`.

At the DataNode boundary, `ImportValmer.update()` expects `self.source_data` to
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
