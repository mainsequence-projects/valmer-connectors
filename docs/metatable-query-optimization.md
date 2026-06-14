# MetaTable Query Optimization

This document records the current Valmer vector update optimization that avoids
full-row MetaTable reads when the workflow only needs existence checks, row
identifiers, or freshness metadata.

The current problem is not only logging. The backend is doing unnecessary work
because `msm.repositories.crud.search_model(...)` compiles `select(model)`,
which returns every mapped column for that model. The Valmer update path calls
that generic helper on high-cardinality batches where the code usually needs
only two or three fields.

## Current Contract

The implemented contract is:

- asset validation queries return only the columns needed for asset existence,
  UID mapping, and asset type correction;
- Valmer asset detail freshness checks return only
  `asset_uid`, `valmer_unique_identifier`, and `details_asof`;
- current pricing-detail refresh checks return only the fields needed to decide
  whether an instrument must be rebuilt;
- position-building asset lookup returns only `unique_identifier` and `uid`;
- logs show row counts for fetched, skipped, missing, stale, and written rows;
- tests prove that equal or older `details_asof` values skip asset-detail
  upserts and strictly newer values write;
- the vector update does not call full-row `search_model(...)` in the hot
  validation paths listed below;
- pricing details are persisted through
  `msm_pricing.api.add_many_pricing_details(...)`, and incomplete timestamped
  results raise instead of being accepted.

## Current Hot Path Map

```text
+-------------------------------------------------------------+
| valmer-connectors vector update                            |
+-------------------------------------------------------------+
        |
        v
+-------------------------------------------------------------+
| ImportValmer.prepare_for_update()                          |
+-------------------------------------------------------------+
        |
        +-- resolve_valmer_asset_refs(...)
        |      projection: unique_identifier, uid, asset_type
        |
        +-- _get_current_pricing_face_values_by_uid(...)
        |      projection: asset_uid, instrument_dump
        |
        +-- upsert_valmer_asset_details(...)
        |      projection gate: asset_uid, valmer_unique_identifier, details_asof
        |
        +-- build_qll_bond_from_row(...)
        |
        +-- _persist_valmer_pricing_details_batch(...)
               msm_pricing bulk timestamped upsert + strict-date current reconciliation
```

The log line below is from the last phase, not from asset/detail resolution:

```text
Persisting Valmer pricing details in bulk: 1757 items, 1 date groups, batch size 5000.
```

That phase uses `msm_pricing.api.add_many_pricing_details(...)`, so it should
not emit one backend write per asset. Thin projection reads are already used
for the earlier validation phases.

## Implemented Projection Sites

### 1. Asset Resolution

Current file:

```text
src/valmer_connectors/instruments/asset_identity.py
```

Current function:

```text
resolve_valmer_assets(...)
```

Legacy behavior:

```text
search_model(
    model=Asset.__table__,
    in_filters={"unique_identifier": batch},
)
```

Problem:

```text
SELECT *
FROM AssetTable
WHERE unique_identifier IN (...)
```

Most of the vector update only needs:

```text
unique_identifier
uid
asset_type
```

Implemented helper:

```text
resolve_valmer_asset_refs(...)
    SELECT
        AssetTable.unique_identifier,
        AssetTable.uid,
        AssetTable.asset_type
    FROM AssetTable
    WHERE AssetTable.unique_identifier IN (...)
```

Return shape:

```python
@dataclass(frozen=True)
class ValmerAssetRef:
    unique_identifier: str
    uid: uuid.UUID
    asset_type: str | None
```

`resolve_valmer_assets(...)` remains only for callers that need typed
`msm.api.assets.Asset` objects. The vector update hot path uses
`resolve_valmer_asset_refs(...)`.

### 2. Asset UID Only Lookup

Current file:

```text
src/valmer_connectors/instruments/vector_to_asset.py
```

Current call site:

```text
build_valuation_position_from_sheet(...)
```

Legacy behavior:

```text
resolve_valmer_assets(df_out["UID"].to_list())
```

Problem:

This materializes asset objects only to map:

```text
UID -> asset.uid
```

Implemented helper:

```text
resolve_valmer_asset_uids(...)
    SELECT
        AssetTable.unique_identifier,
        AssetTable.uid
    FROM AssetTable
    WHERE AssetTable.unique_identifier IN (...)
```

Return shape:

```python
dict[str, uuid.UUID]
```

### 3. Valmer Asset Detail Freshness

Current file:

```text
src/valmer_connectors/meta_tables/valmer_asset_details.py
```

Current function:

```text
resolve_valmer_asset_details(...)
```

Legacy behavior:

```text
search_model(
    model=ValmerAssetDetailsTable,
    in_filters={"asset_uid": batch},
)
```

Problem:

The strict freshness gate only needs:

```text
asset_uid
valmer_unique_identifier
details_asof
```

It should not fetch static descriptor columns such as issuer, sector, coupon
fields, maturity, or placement terms.

Implemented helper:

```text
resolve_valmer_asset_detail_versions(...)
    SELECT
        ValmerAssetDetailsTable.asset_uid,
        ValmerAssetDetailsTable.valmer_unique_identifier,
        ValmerAssetDetailsTable.details_asof
    FROM ValmerAssetDetailsTable
    WHERE ValmerAssetDetailsTable.asset_uid IN (...)
```

Return shape:

```python
@dataclass(frozen=True)
class ValmerAssetDetailVersion:
    asset_uid: uuid.UUID
    valmer_unique_identifier: str
    details_asof: datetime | None
```

Freshness rule:

```text
if existing row is missing:
    upsert
elif incoming.details_asof > existing.details_asof:
    upsert
else:
    skip
```

Do not compare static fields. Equal dates and older dates must skip.

### 4. Current Pricing Detail Refresh Check

Current file:

```text
src/valmer_connectors/data_nodes/nodes.py
```

Implemented function:

```text
ImportValmer._get_current_pricing_face_values_by_uid(...)
```

Legacy behavior:

```text
search_model(
    model=AssetCurrentPricingDetails.__table__,
    in_filters={"asset_uid": batch},
)
```

Problem:

The refresh decision only checks whether the previous instrument face value
matches the latest Valmer row.

The current code reads only:

```text
asset_uid
instrument_dump
```

Implemented projection:

```text
_get_current_pricing_face_values_by_uid(...)
    SELECT
        AssetCurrentPricingDetailsTable.asset_uid,
        AssetCurrentPricingDetailsTable.instrument_dump
    FROM AssetCurrentPricingDetailsTable
    WHERE AssetCurrentPricingDetailsTable.asset_uid IN (...)
```

Then extract face value locally with the existing
`_pricing_detail_face_value(...)` helper.

Return shape:

```python
dict[str, object]
```

where the key is Valmer `unique_identifier`, not raw `asset_uid`, because
`_get_pricing_refresh_uids(...)` works in Valmer identifier space.

## Implementation Pattern

Do not use `search_model(...)` for the four hot validation paths.

Use compiled SQL projection helpers:

```python
from sqlalchemy import select

from msm.repositories.base import compile_markets_statement
from msm.repositories.base import execute_markets_operation
```

Each helper should:

- accept a batchable list of identifiers or asset UIDs;
- normalize and deduplicate input while preserving order when order matters;
- use the current runtime context;
- query only the required columns;
- return simple dataclasses or dictionaries;
- log batches and summary counts when a logger is provided;
- keep the existing full-object resolver available for non-hot paths.

Implemented helper locations:

```text
src/valmer_connectors/instruments/asset_identity.py
    ValmerAssetRef
    resolve_valmer_asset_refs(...)
    resolve_valmer_asset_uids(...)

src/valmer_connectors/meta_tables/valmer_asset_details.py
    ValmerAssetDetailVersion
    resolve_valmer_asset_detail_versions(...)

src/valmer_connectors/data_nodes/nodes.py
    _get_current_pricing_face_values_by_uid(...)
```

## Updated Vector Sync Flow

```text
ImportValmer._sync_asset_registry_and_pricing(...)
    |
    +-- resolve_valmer_asset_refs(...)
    |      returns uid + asset_type only
    |
    +-- missing_assets = target-bond ids not in refs
    |
    +-- asset_type_conflicts =
    |      existing refs where asset_type != bond
    |
    +-- raise on asset_type_conflicts
    |
    +-- _upsert_asset_table_rows(...)  [private AssetTable write helper]
    |      only for missing assets, with explicit asset_type=bond
    |
    +-- asset_refs.update(upserted refs)
    |
    +-- resolve_valmer_asset_detail_versions(...)
    |      returns only detail freshness fields
    |
    +-- upsert_valmer_asset_details(...)
    |      only where incoming details_asof is strictly newer
    |
    +-- _get_current_pricing_face_values_by_uid(...)
    |      only for target bond asset_uids
    |
    +-- _get_pricing_refresh_uids(...)
    |      compares existing face value to latest Valmer face value
    |
    +-- build_qll_bond_from_row(...)
    |
    +-- _persist_valmer_pricing_details_batch(...)
          -> msm_pricing.api.add_many_pricing_details(...)
```

## Logging Requirements

Logs must show meaningful counts before and after each expensive phase.

Asset refs:

```text
Resolving 28972 Valmer asset refs in 29 batches of up to 1000.
Resolved 28972 Valmer asset refs: 28972 existing, 0 missing, 0 wrong asset_type.
```

Asset details:

```text
Resolving 28972 Valmer asset detail versions in 29 batches of up to 1000.
Prepared Valmer asset detail sync: 28972 candidate rows, 0 newer rows, 28972 skipped because source date is not newer.
No Valmer asset detail upserts required.
```

Pricing refresh:

```text
Resolving current pricing face values for 1757 target assets in 2 batches of up to 1000.
Pricing refresh decision: 1757 target assets, 0 missing details, 0 changed face values, 0 refreshes.
```

Pricing persistence:

```text
Persisting Valmer pricing details in bulk: 1757 items, 1 date groups, batch size 5000.
```

This phase now uses the core `msm_pricing` bulk writer. It should emit one
message per Valmer pricing-date group instead of one backend write per asset.

## Pricing Persistence

Pricing persistence now calls:

```text
valmer_connectors.data_nodes.nodes._persist_valmer_pricing_details_batch(...)
    -> msm_pricing.api.add_many_pricing_details(...)
```

for each explicit Valmer pricing date. The `msm_pricing` batch API always
upserts timestamped pricing-detail rows. It updates current rows only if the
incoming explicit date is strictly newer than the stored current row, or when no
current row exists.

Expected behavior:

- validate each instrument against its asset through `msm_pricing`;
- bulk upsert timestamped pricing-detail rows;
- reconcile current rows through the core strict-date policy;
- raise if the timestamped result is incomplete for the submitted Valmer UIDs;
- log one completion message per date group, not one message per asset.

## Implementation Checklist

- [x] Add `ValmerAssetRef` dataclass.
- [x] Add `resolve_valmer_asset_refs(...)` projection query.
- [x] Add `resolve_valmer_asset_uids(...)` projection query.
- [x] Update `ImportValmer._sync_asset_registry_and_pricing(...)` to use asset
      refs for existence and asset type checks.
- [x] Keep `resolve_valmer_assets(...)` for callers that really need typed
      `Asset` objects.
- [x] Add `ValmerAssetDetailVersion` dataclass.
- [x] Add `resolve_valmer_asset_detail_versions(...)` projection query.
- [x] Update `upsert_valmer_asset_details(...)` to use only detail versions for
      the freshness gate.
- [x] Add `_get_current_pricing_face_values_by_uid(...)`.
- [x] Update `_get_pricing_refresh_uids(...)` to consume face values rather than
      full `AssetCurrentPricingDetails` models.
- [x] Update `build_valuation_position_from_sheet(...)` to use
      `resolve_valmer_asset_uids(...)`.
- [x] Add tests for projection helpers and strict detail freshness behavior.
- [x] Add tests proving full `search_model(...)` is not used in the hot paths.
- [ ] Run the vector update against a local sample and confirm logs show skip
      counts before pricing persistence starts.
- [x] Replace one-row pricing-detail writes with
      `msm_pricing.api.add_many_pricing_details(...)`.

## Validation Commands

Focused tests:

```bash
.venv/bin/python -m unittest tests.test_valmer_asset_identity tests.test_valmer_asset_details
```

Nearby workflow tests:

```bash
.venv/bin/python -m unittest tests.test_valmer_asset_identity tests.test_valmer_asset_details tests.test_valmer_vector_storage tests.test_valmer_instrument_index_uids
```

Runtime smoke check:

```bash
valmer-connectors vector update
```

Expected runtime evidence:

- asset ref resolution logs include existing, missing, and wrong-type counts;
  wrong-type rows raise because they indicate identity collisions;
- detail sync logs show how many rows were skipped because source date was not
  newer;
- pricing refresh logs show target count and refresh count before instrument
  construction;
- pricing persistence logs show the `add_many_pricing_details(...)` batch size,
  date-group count, timestamped-row count, and current-row update count.
