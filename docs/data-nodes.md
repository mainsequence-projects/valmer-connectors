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
- the full 60-column Valmer source schema translated to English column names

Key translated source columns include:

- `valuation_date`, `security_type`, `issuer`, `series`
- `clean_price`, `dirty_price`, `accrued_interest`
- `issue_date`, `maturity_date`, `issue_currency`, `underlying`
- `coupon_frequency`, `coupon_rate`, `coupon_rule`
- `duration`, `monetary_duration`, `macaulay_duration`, `convexity`
- `fitch_rating`, `moodys_rating`, `sp_rating`, `hr_rating`

The translation contract is defined in `src/data_nodes/nodes.py` from the
sample workbook schema and is persisted with explicit English metadata and
typed casts for numeric, percentage, integer-count, and datetime fields. The
node uses `DataFrequency.one_d` to match the effective update cadence.

## Operational Guidance

For large data volumes:

- test first in a test namespace
- limit the time range before running a full update or backfill

`ImportValmer.get_asset_list()` also registers or reuses assets and updates
pricing details for the target bond subset selected by `_get_target_bonds(...)`.

## Standalone Curve Node: `MexDerTIIE28Zero`

The repository also contains a standalone curve node:

- `valmer_mexder_tiie28_zero_curve`

Its metadata is now stable and describes the stored compressed curve payload as
text instead of deriving metadata by executing a live update.

## Execution Note

This repo only wires one curve execution path:

- the standard `DiscountCurvesNode` flow wired through
  `scripts/update_tiie_zero_curve.py`

`MexDerTIIE28Zero` is present as a standalone implementation, but there is no
checked-in runner that instantiates or executes it.

The dashboard can read `valmer_mexder_tiie28_zero_curve` if that table already
exists in the backend, but this repository does not publish it as part of the
checked-in job flow.
