# MetaTable Query Optimization Implementation Plan

This document defines the implementation plan for making the Valmer vector
update path stop using full-row MetaTable reads when the workflow only needs
existence checks, row identifiers, or freshness metadata.

The current problem is not only logging. The backend is doing unnecessary work
because `msm.repositories.crud.search_model(...)` compiles `select(model)`,
which returns every mapped column for that model. The Valmer update path calls
that generic helper on high-cardinality batches where the code usually needs
only two or three fields.

## Success Criteria

The implementation is complete when:

- asset validation queries return only the columns needed for asset existence,
  UID mapping, and asset type correction;
- Valmer asset detail freshness checks return only
  `asset_uid`, `valmer_unique_identifier`, and `details_asof`;
- current pricing-detail refresh checks return only the fields needed to decide
  whether an instrument must be rebuilt;
- position-building asset lookup returns only `unique_identifier` and `uid`;
- logs show row counts for fetched, skipped, missing, stale, and written rows;
- tests prove that equal or older `details_asof` values skip asset detail
  upserts and strictly newer values write;
- the vector update does not call full-row `search_model(...)` in the hot
  validation paths listed below.

## Current Bottleneck Map

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
        +-- resolve_valmer_assets(...)
        |      current: full AssetTable rows
        |      needed: unique_identifier, uid, asset_type
        |
        +-- _get_current_pricing_details_by_uid(...)
        |      current: full AssetCurrentPricingDetails rows
        |      needed: asset_uid, instrument_dump face_value
        |
        +-- upsert_valmer_asset_details(...)
        |      current: full ValmerAssetDetailsTable rows
        |      needed: asset_uid, valmer_unique_identifier, details_asof
        |
        +-- build_qll_bond_from_row(...)
        |
        +-- persist_current_pricing_details(...)
               current: one backend upsert per asset
               needed: separate bulk-write implementation after projection reads
```

The log line below is from the last phase, not from asset/detail resolution:

```text
Persisting current pricing details: starting 1757 items.
```

That phase is still slow because `persist_current_pricing_details(...)` performs
one `AssetCurrentPricingDetails.upsert(...)` per asset. Thin projection reads
will make earlier phases faster, but this final persistence phase needs its own
bulk-write task if the end-to-end update is still too slow.

## Query Sites To Replace

### 1. Asset Resolution

Current file:

```text
src/valmer_connectors/instruments/asset_identity.py
```

Current function:

```text
resolve_valmer_assets(...)
```

Current behavior:

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

Implementation:

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

Keep `resolve_valmer_assets(...)` only for callers that need typed
`msm.api.assets.Asset` objects. The vector update hot path should use
`resolve_valmer_asset_refs(...)`.

### 2. Asset UID Only Lookup

Current file:

```text
src/valmer_connectors/instruments/vector_to_asset.py
```

Current call site:

```text
build_position_from_sheet(...)
```

Current behavior:

```text
resolve_valmer_assets(df_out["UID"].to_list())
```

Problem:

This materializes asset objects only to map:

```text
UID -> asset.uid
```

Implementation:

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

Current behavior:

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

Implementation:

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

Current function:

```text
ImportValmer._get_current_pricing_details_by_uid(...)
```

Current behavior:

```text
search_model(
    model=AssetCurrentPricingDetails.__table__,
    in_filters={"asset_uid": batch},
)
```

Problem:

The refresh decision only checks whether the previous instrument face value
matches the latest Valmer row.

Current code reads a full `AssetCurrentPricingDetails` object, but the decision
only needs:

```text
asset_uid
instrument_dump
```

Better target if supported cleanly by compiled SQL:

```text
asset_uid
instrument_dump["face_value"]
```

Implementation:

```text
resolve_current_pricing_face_values(...)
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

Expected helper locations:

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
    +-- missing_assets = source ids not in refs
    |
    +-- assets_needing_type_update =
    |      existing refs where asset_type != bond
    |
    +-- upsert_valmer_assets(...)
    |      only for missing or wrong-type assets
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
    +-- persist current pricing details
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
Persisting current pricing details: starting 1757 items.
```

If this phase is still slow after the projection work, that is expected. It is
the one-row-at-a-time pricing upsert path and must be addressed separately with
a bulk pricing-details writer.

## Pricing Persistence Follow-Up

The existing pricing persistence path calls:

```text
msm_pricing.api.instruments.persist_current_pricing_details(...)
```

inside a loop. That performs one backend upsert per asset.

After projection reads are implemented, the next performance task should be:

```text
bulk_persist_current_pricing_details(...)
```

Expected behavior:

- build instrument backend payloads locally;
- validate each instrument against its asset;
- bulk upsert `AssetCurrentPricingDetailsTable` rows;
- return or log only the minimal result needed by this project;
- log one completion message per batch, not one message per asset.

This is intentionally separate from the projection-read task because it changes
write semantics and should be validated independently.

## Implementation Checklist

- [ ] Add `ValmerAssetRef` dataclass.
- [ ] Add `resolve_valmer_asset_refs(...)` projection query.
- [ ] Add `resolve_valmer_asset_uids(...)` projection query.
- [ ] Update `ImportValmer._sync_asset_registry_and_pricing(...)` to use asset
      refs for existence and asset type checks.
- [ ] Keep `resolve_valmer_assets(...)` for callers that really need typed
      `Asset` objects.
- [ ] Add `ValmerAssetDetailVersion` dataclass.
- [ ] Add `resolve_valmer_asset_detail_versions(...)` projection query.
- [ ] Update `upsert_valmer_asset_details(...)` to use only detail versions for
      the freshness gate.
- [ ] Add `_get_current_pricing_face_values_by_uid(...)`.
- [ ] Update `_get_pricing_refresh_uids(...)` to consume face values rather than
      full `AssetCurrentPricingDetails` models.
- [ ] Update `build_position_from_sheet(...)` to use
      `resolve_valmer_asset_uids(...)`.
- [ ] Add tests for projection helpers and strict detail freshness behavior.
- [ ] Add tests proving full `search_model(...)` is not used in the hot paths.
- [ ] Run the vector update against a local sample and confirm logs show skip
      counts before pricing persistence starts.
- [ ] Decide whether to implement `bulk_persist_current_pricing_details(...)` as
      the next task.

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
- detail sync logs show how many rows were skipped because source date was not
  newer;
- pricing refresh logs show target count and refresh count before instrument
  construction;
- if the run stalls at `Persisting current pricing details`, the remaining
  bottleneck is the one-row-at-a-time pricing write path.
