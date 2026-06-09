# Markets And Asset Details

This page documents market identity and project-local MetaTables. Pricing
hydration and curve publication are documented in `pricing.md`. DataNode
publication is documented in `data-nodes.md`.

## Asset Identity

Valmer bond assets are canonical `ms-markets` `AssetTable` rows.

Project asset key:

```text
tipovalor_emisora_serie
```

The same value is used as:

- `AssetTable.unique_identifier`
- Valmer vector storage `asset_identifier`
- `ValmerAssetDetailsTable.valmer_unique_identifier`

`asset_identifier` is the `ms-markets` DataNode dimension name. Its value is the
same string as `AssetTable.unique_identifier`.

Valmer bond assets are registered with:

```text
asset_type = bond
```

The asset type constant comes from `msm.constants`.

## Single Asset Registration Helper

Asset registration is centralized in:

- `src/valmer_connectors/instruments/asset_identity.py`
- `upsert_valmer_assets(...)`
- `resolve_valmer_assets(...)`

`ImportValmer.prepare_for_update()` calls those helpers before the DataNode run.
`ImportValmer.get_asset_list()` only returns the prepared scope.

## ValmerAssetDetailsTable

Static Valmer descriptors do not belong in the time-indexed DataNode. They live
in the project-local MetaTable:

- `ValmerAssetDetailsTable`
- file: `src/valmer_connectors/meta_tables/valmer_asset_details.py`
- logical identifier: `ValmerAssetDetails`
- storage app segment: `valmer_connectors`

Relationship:

```text
+------------------------------------+
| AssetTable                         |
|------------------------------------|
| uid PK                             |
| unique_identifier UNIQUE           |
| asset_type                         |
+------------------------------------+
              |
              | 1:1 FK
              | AssetTable.uid -> ValmerAssetDetailsTable.asset_uid
              v
+------------------------------------+
| ValmerAssetDetailsTable            |
|------------------------------------|
| asset_uid PK/FK                    |
| valmer_unique_identifier UNIQUE    |
| details_asof                       |
| security_type, issuer, series      |
| full_name, sector                  |
| issue_date, maturity_date          |
| currency, underlying, coupon terms |
+------------------------------------+
```

`asset_uid` is both the primary key and the foreign key to `AssetTable.uid`.
There is intentionally no separate `uid` column.

## Fields Stored In ValmerAssetDetailsTable

The detail table stores latest static or slowly changing source descriptors:

- `security_type`
- `issuer`
- `series`
- `full_name`
- `sector`
- `issued_amount`
- `issue_date`
- `issue_term`
- `maturity_date`
- `face_value`
- `issue_currency`
- `underlying`
- `placement_yield`
- `placement_spread`
- `coupon_frequency`
- `coupon_rate`
- `coupon_rule`
- `coupons_at_issue`

The source row timestamp is stored as `details_asof`.

## Project-Owned MetaTable Migration

Project-owned Valmer market tables use the `ms-markets` storage app helper:

```text
__markets_storage_app__ = "valmer_connectors"
```

That applies to:

- `ValmerAssetDetailsTable`
- `ValmerVectorPricesStorage`

The project migration provider is:

```text
migrations:migration
```

The provider metadata includes `AssetTable` so foreign keys can resolve during
Alembic autogenerate, but DDL emission is filtered to Valmer-owned tables only.

Run project migrations after core `ms-markets` migrations:

```bash
mainsequence migrations current --provider msm.migrations:migration
mainsequence migrations upgrade --provider msm.migrations:migration head

mainsequence migrations current --provider migrations:migration
mainsequence migrations upgrade --provider migrations:migration head
```

## What This Page Does Not Own

- Valmer source file import: `source-import.md`
- Valmer vector time-series output: `data-nodes.md`
- bond instrument pricing hydration: `pricing.md`
- row-to-instrument mapping rules: `instruments.md`
- dashboard behavior: `dashboards.md`
