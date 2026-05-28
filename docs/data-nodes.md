# DataNodes

## Source DataNode: `ImportValmer`

`ImportValmer` reads Valmer artifacts from the bucket:

- `Hitorical Valmer Vector Analytico`

The spelling above is intentional because it matches the existing backend
bucket resource name.

It normalizes the vendor headers, derives `unique_identifier` as
`tipovalor_emisora_serie`, and publishes the table:

- `vector_de_precios_valmer`

## Stored Schema

The table is indexed by:

- `time_index`
- `unique_identifier`

The stored columns are:

- derived OHLC fields from `dirty_price`: `open`, `high`, `low`, `close`
- derived execution fields: `volume`, `open_time`
- time-varying Valmer price, yield, spread, risk, and liquidity fields

Static repeated asset-descriptor fields no longer live in the DataNode. They
are stored in the 1:1 `ValmerAssetDetailsTable` keyed by `asset_uid`.

Key DataNode columns include:

- `valuation_date`
- `clean_price`, `dirty_price`, `accrued_interest`
- `current_coupon`, `spread`
- `theoretical_price`, `posted_bid`, `posted_ask`
- `bid_yield`, `ask_yield`, `bid_spread`, `ask_spread`
- `liquidity`, `daily_change_pct`, `weekly_change_pct`
- `duration`, `monetary_duration`, `macaulay_duration`, `convexity`
- `value_at_risk`, `standard_deviation`, `sensitivity`, `yield_rate`
- changing vendor state such as ratings, outstanding amount, adjusted face
  value, marketability, and suspension status

The translation contract is defined in `src/data_nodes/nodes.py` from the
sample workbook schema and is persisted with explicit English metadata and
typed casts for numeric, percentage, integer-count, and datetime fields. The
node uses `DataFrequency.one_d` to match the effective update cadence.

`ValmerAssetDetailsTable` stores static asset-level Valmer fields such as
`security_type`, `issuer`, `series`, `full_name`, `sector`, issue terms,
currency, underlying, and coupon terms.

## Operational Guidance

For large data volumes:

- test first in a test namespace
- limit the time range before running a full update or backfill

`ImportValmer.get_asset_list()` also registers or reuses assets, upserts
`ValmerAssetDetailsTable`, and updates pricing details for the target bond
subset selected by `_get_target_bonds(...)`.

## Curve Node

This repo only wires one curve execution path:

- the canonical `msm_pricing.data_nodes.DiscountCurvesNode` flow wired through
  `scripts/update_tiie_zero_curve.py`

The Valmer TIIE curve is keyed by `curve_unique_identifier`, not asset
`unique_identifier`, and writes to the canonical `discount_curves` DataNode.
The old standalone `valmer_mexder_tiie28_zero_curve` path has been removed from
the codebase and should be treated as legacy backend data only.
