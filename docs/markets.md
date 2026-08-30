# Markets And Asset Details

This page documents market identity and project-local MetaTables. Pricing
hydration and curve publication are documented in `pricing.md`. TimeIndexTableUpdater
publication is documented in `time-index-table-updates.md`.

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

`asset_identifier` is the `ms-markets` time-index table dimension name. Its value is the
same string as `AssetTable.unique_identifier`.

Valmer bond assets are registered with:

```text
asset_type = bond
```

The asset type constant comes from `msm.constants`.

## Current Registration Boundary

Asset registration is centralized in:

- `src/valmer_connectors/instruments/asset_identity.py`
- `resolve_valmer_assets(...)`
- `resolve_valmer_asset_refs(...)`
- `resolve_valmer_asset_uids(...)`

`asset_identity.py` exposes Valmer identity builders and thin lookup helpers.
It does not expose a public helper that fully registers arbitrary Valmer assets.
The internal `_upsert_asset_table_rows(...)` helper writes only minimal
`AssetTable` rows and is not a Valmer registration API.

`ImportValmer.prepare_for_update()` calls those helpers before the updater run.
`ImportValmer.get_asset_list()` only returns the prepared scope.

Asset registration is not the same as current pricing-detail hydration.

During `valmer-connectors vector update`, source rows are filtered from the last
stored vector observation per `asset_identifier`. Then
`ImportValmer.prepare_for_update()` registers or resolves assets only from the
supported target-pricing subset selected by `ImportValmer._get_target_bonds(...)`.
Current pricing details and `ValmerAssetDetailsTable` rows are hydrated for that
same registration scope.

The broader imported Valmer source universe is intentionally not registered as
`AssetTable` rows. The Valmer vector contains multiple instrument types, and
this project only classifies the target bond subset today.

```text
+--------------------------------------------------------------+
| Valmer Vector Source                                        |
|--------------------------------------------------------------|
| bonds, government instruments, floating bonds, derivatives,  |
| reference rows, and other vendor instrument families         |
+-----------------------------+--------------------------------+
                              |
                              | target-bond classifier only
                              v
+--------------------------------------------------------------+
| Valmer Target-Bond Registration                              |
|--------------------------------------------------------------|
| unique_identifier = tipovalor_emisora_serie                  |
| asset_type        = msm.constants.ASSET_TYPE_BOND            |
+-----------------------------+--------------------------------+
                              |
                              v
+--------------------------------------------------------------+
| AssetTable + ValmerAssetDetailsTable + pricing hydration     |
+--------------------------------------------------------------+
```

The package exposes thin Valmer identity and lookup helpers that can be reused
when the source rows use the same Valmer identity convention:

- `build_valmer_unique_identifier(...)`
- `add_valmer_unique_identifier(...)`
- `normalize_valmer_unique_identifiers(...)`
- `resolve_valmer_asset_refs(...)`
- `resolve_valmer_asset_uids(...)`

Those helpers do not decide asset type. They only build or resolve identity.

The current target-bond flow delegates the final missing-asset insert to the
private `_upsert_asset_table_rows(...)` implementation detail after
`ImportValmer._get_target_bonds(...)` has classified the row as a supported
bond.

## Public Valmer Registration API

The public API is a batch Valmer registration service, not a raw `AssetTable`
helper. It accepts Valmer source rows and runs the full supported Valmer asset
workflow:

```text
normalized Valmer rows
    |
    v
derive unique_identifier = tipovalor_emisora_serie
    |
    v
classify supported Valmer asset type
    |
    v
resolve existing AssetTable refs
    |
    +-- raise on asset_type conflicts
    |
    v
upsert missing AssetTable rows
    |
    v
upsert ValmerAssetDetailsTable rows
    |
    v
publish AssetSnapshot rows
    |
    v
build and persist pricing details when requested and when a supported pricing
adapter exists
```

Import path:

```python
from valmer_connectors.assets import register_valmer_assets_from_rows
```

Call shape:

```python
result = register_valmer_assets_from_rows(
    rows,
    *,
    asset_type_classifier=None,
    include_pricing_details=True,
    publish_snapshots=True,
    strict_unsupported=False,
    strict_pricing=False,
    batch_size=None,
    logger=None,
)
```

Default classification is Valmer-specific. It classifies the currently
supported Valmer bond families as `msm.constants.ASSET_TYPE_BOND`, including
M BONOS, CETES, BONDESD, supported TIIE/CETE-linked rows, BPAS, and the existing
project-supported zero-coupon families. Unsupported rows are skipped by default
and returned in `result.skipped_unsupported`.

Callers that import a different vector or broaden the Valmer universe must pass
their own classifier:

```python
from msm.constants import ASSET_TYPE_BOND


def classify_asset_type(row):
    if (row["tipovalor"], row["emisora"]) in {("M", "BONOS"), ("BI", "CETES")}:
        return ASSET_TYPE_BOND
    return None


result = register_valmer_assets_from_rows(
    rows,
    asset_type_classifier=classify_asset_type,
)
```

Validation happens per stage:

```text
identity:
  requires tipovalor, emisora, serie
  creates unique_identifier = tipovalor_emisora_serie

classification:
  classifier must return an explicit non-empty asset_type for registered rows
  unsupported rows are skipped or raise when strict_unsupported=True

details:
  requires fecha for details_asof
  writes static Valmer descriptors to ValmerAssetDetailsTable

snapshot:
  requires unique_identifier, fecha, nombrecompleto when publish_snapshots=True
  maps nombrecompleto to AssetSnapshot.name

pricing:
  uses the instrument adapter as validator
  failures are returned in result.pricing_failed
  failures raise when strict_pricing=True
```

The result is structured enough for callers and tests to prove each layer:

- assets resolved or created by Valmer `unique_identifier`
- static detail rows upserted or skipped by `details_asof`
- snapshot rows published
- pricing details written, skipped, or failed
- unsupported rows reported explicitly
- type conflicts raised explicitly

Batch machinery:

```text
AssetTable:
  _upsert_asset_table_rows(...) [private helper]

ValmerAssetDetailsTable:
  upsert_valmer_asset_details(...)

AssetSnapshot:
  AssetSnapshot.set_snapshots(...).run(...)

Pricing details:
  _persist_valmer_pricing_details_batch(...)
  -> msm_pricing.api.add_many_pricing_details(...)
```

The API still does not claim support for the full Valmer vector universe.
Support is limited to rows that the classifier accepts and that have a pricing
adapter when pricing details are requested.

## Extension Library Contract

Another extension library that imports a different vector, or a broader Valmer
vector universe, must own its own asset classifier before writing to
`AssetTable`.

The required flow is:

```text
Other vector source
    |
    v
extension-owned parser and normalizer
    |
    v
extension-owned classifier
    |
    +-- unique_identifier
    +-- explicit asset_type
    +-- optional static detail payload
    |
    v
upsert AssetType rows when the asset type is project-owned
    |
    v
upsert AssetTable rows with explicit asset_type
    |
    v
upsert extension-owned detail table rows keyed by AssetTable.uid
    |
    v
publish time-varying rows through that extension's TimeIndexTableUpdater
```

Extension libraries should not import this package's private
`_upsert_asset_table_rows(...)` helper. A different vector should expose its own
service layer and use the canonical `ms-markets` asset model rule:

```text
AssetTable
  unique_identifier = stable canonical key
  asset_type        = explicit registered type

ExtensionDetailsTable
  asset_uid         = FK to AssetTable.uid
  static fields     = instrument/provider/reference fields owned by extension

ExtensionDataNodeStorage
  time_index
  asset_identifier  = AssetTable.unique_identifier
  time-varying observations only
```

If the extension introduces a new `asset_type`, it should register that type
through `ms-markets` before inserting assets of that type. `Asset.asset_type` is
a logical classification string; static instrument data still belongs in a
detail table, not in `AssetTable`.

Type conflicts must be treated as data errors. If an existing
`AssetTable.unique_identifier` is already registered with a different
`asset_type`, an extension should raise and investigate the identity collision
instead of silently overwriting the type.

Pricing hydration is a separate step. Asset registration should produce the
canonical asset row and static details; pricing details should be written only
after the instrument-specific pricing adapter has built a valid pricing payload.

## ValmerAssetDetailsTable

Static Valmer descriptors do not belong in the time-index table. They live
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

## Public Valmer Detail Query API

Downstream projects should read static Valmer detail fields through:

```python
from valmer_connectors.queries import (
    read_valmer_asset_detail_alias_frame,
    read_valmer_asset_detail_maturity_fields,
    resolve_valmer_detail_identifier_aliases,
)
```

The query API is read-only and uses the active governed `ms-markets` runtime.
It calls `ensure_valmer_asset_detail_runtime(...)`, compiles a projected SQL
statement with `compile_markets_statement(...)`, and executes it with
`execute_markets_operation(...)`.

The lookup accepts either identifier form:

- `ValmerAssetDetailsTable.valmer_unique_identifier`
- `AssetTable.unique_identifier`

`read_valmer_asset_detail_alias_frame(...)` returns alias-expanded rows. The
returned `asset_identifier` column is the lookup alias, so the same Valmer
detail row can be addressed by both its Valmer unique identifier and its
canonical `AssetTable.unique_identifier`.

`read_valmer_asset_detail_maturity_fields(...)` returns the static maturity and
coupon fields used by downstream valuation and analytics code:

```text
asset_uid
asset_table_identifier
asset_identifier
valmer_unique_identifier
valmer_security_type
valmer_issuer
valmer_series
valmer_full_name
valmer_issue_date
valmer_maturity_date
maturity_date
valmer_face_value
valmer_coupon_frequency
valmer_coupon_rate
```

`resolve_valmer_detail_identifier_aliases(...)` returns a dictionary mapping
each accepted lookup alias to its Valmer unique identifier.

Example:

```python
from valmer_connectors.queries import read_valmer_asset_detail_maturity_fields


details = read_valmer_asset_detail_maturity_fields(
    ["M_BONOS_241205", "M_BONOS_241205_VALMER"],
)
```

This helper layer does not change `AssetTable`,
`ValmerAssetDetailsTable`, asset registration, or pricing hydration.

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
- Valmer vector time-series output: `time-index-table-updates.md`
- bond instrument pricing hydration: `pricing.md`
- row-to-instrument mapping rules: `instruments.md`
- dashboard behavior: `dashboards.md`
