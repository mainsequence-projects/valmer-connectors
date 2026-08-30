# Canonical Daily Index Observations

FRED, Banxico, and Valmer curve quotes are published as daily `Index` values in
`IndexValuesTS.1d`. Treasury yields and policy targets are analytical market
observations, while Valmer par rates, basis spreads, futures prices, FX spot,
and forward points are curve inputs. None of them is stored as an Asset price or
pricing fixing.

## Storage Contract

| Property | Value |
| --- | --- |
| MetaTable | `IndexValuesTS.1d` |
| Physical table | `ms_markets__index_values__t_1d` |
| Grain | `(time_index, index_identifier)` |
| Value | `value` |
| Provenance | `definition_uid`, `observation_status`, `source_as_of`, `metadata_json` |
| Identity | `index_identifier -> IndexTable.unique_identifier` |

Source-published observations use `definition_uid = null`. Percentage-form
rates are divided by 100 exactly once. Display formatting belongs to the Index
identity; vendor units and curve-quote canonical units are stored in bounded
`metadata_json` provenance rather than a storage compatibility column.

## FRED and Banxico Series

| Index identifier | Source | Meaning |
| --- | --- | --- |
| `US_TREASURY_CMT_2Y` | FRED `DGS2` | 2-year Treasury constant-maturity yield |
| `US_TREASURY_CMT_5Y` | FRED `DGS5` | 5-year Treasury constant-maturity yield |
| `US_TREASURY_CMT_10Y` | FRED `DGS10` | 10-year Treasury constant-maturity yield |
| `US_TREASURY_CMT_30Y` | FRED `DGS30` | 30-year Treasury constant-maturity yield |
| `FED_FUNDS_TARGET_UPPER` | FRED `DFEDTARU` | Federal Funds target-range upper limit |
| `BANXICO_POLICY_TARGET` | Banxico `SF61745` | Banco de Mexico policy target |

`FED_FUNDS_TARGET_UPPER` is not an effective Fed Funds fixing, OIS quote, or
curve input.

## Valmer Curve Quote Series

The MXN producer publishes all 34 recognized rows from `IRS_MXN_CURVE.csv`; the
USD producer publishes all 47 recognized rows from `IRS_USD_CURVE.csv`. Each
stable identity starts with `VALMER_CURVE_QUOTE.` and retains source family,
vendor identifier, source quote/unit, quote side, and source file in metadata.

Run producers before curves:

```bash
valmer-connectors quotes update-irs-mxn
valmer-connectors quotes update-irs-usd
valmer-connectors curves update-tiie-irs-mxn
valmer-connectors curves update-usd-sofr
valmer-connectors curves update-usd-mxn-xccy
```

The curve TimeIndexTableUpdaters also declare these relationships through
`dependencies()`. Builders query exact persisted dates and never download raw
Valmer files.

## External Producer Operations

Required secrets:

- `FRED_API_KEY`
- `BANXICO_TOKEN`

Run:

```bash
valmer-connectors reference-rates update-fred
valmer-connectors reference-rates update-banxico-policy
```

On first execution each source node requests five inclusive calendar years
ending yesterday UTC. Later runs begin one calendar day after each Index's own
latest observation. Missing source values are omitted and never forward-filled.

## Migration

Apply core ms-markets migrations before the project provider:

```bash
mainsequence migrations upgrade --provider msm.migrations:migration head
PYTHONPATH=src mainsequence migrations upgrade --provider migrations:migration head
```

Project revision `0001` is a clean current-schema baseline. It creates the
unit-free daily table with current foreign keys to `IndexTable` and
`IndexFormulaDefinitionTable`; it does not create, copy, translate, or retain a
project-specific reference-rate table. Runtime code contains only canonical
producers. Rebuilding a data source therefore means applying the baseline and
rerunning the FRED and Banxico producers from their source APIs.

## Repair

Delete only the affected Index tail from the canonical storage before rerunning
its producer:

```python
DailyIndexValuesStorage.get_time_index_meta_table().delete_after_date(
    "<INCLUSIVE_UTC_CUTOFF>",
    dimension_filters={"index_identifier": ["<INDEX_IDENTIFIER>"]},
)
```

Never issue unscoped deletion or raw SQL.

## Verification

- Compare row counts, first/last dates, values, provenance, and duplicate keys
  per Index.
- Require 34 MXN plus 47 USD Valmer observations for a complete source date.
- Resolve every curve key node's typed Index source reference on the exact curve
  date and reconcile value, metadata `quote_unit`, source quote, source unit,
  and vendor identity.
- Run every producer twice and confirm the second run adds no duplicate keys.
- Inspect scheduled platform runs and logs; local tests do not prove a live job.
