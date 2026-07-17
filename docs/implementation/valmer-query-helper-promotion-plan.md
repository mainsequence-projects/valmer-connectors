# Valmer Query Helper Promotion Plan

## Decision

Create a dedicated read/query package:

```text
src/valmer_connectors/queries/
```

The first implementation pass promotes only generic Valmer query behavior:

- published Valmer vector reads;
- latest Valmer quote reads per asset;
- dirty-price and yield-history convenience reads;
- Valmer asset-detail lookup by either Valmer identifier or canonical
  `AssetTable.unique_identifier`;
- shared input and timestamp normalization used by those read helpers.

The first implementation pass creates the query package. The second
implementation pass creates the spread-analysis analytics package on top of the
query API.

## Current Repository Facts

`ValmerVectorPricesStorage` already owns the vector storage contract:

```text
src/valmer_connectors/data_nodes/valmer_vector_storage.py
```

Verified contract:

- class: `ValmerVectorPricesStorage`
- authored storage identifier: `vector_de_precios_valmer`
- runtime MetaTable identifier: `valmer_connectors.vector_de_precios_valmer`
- namespace constant: `VALMER_MARKETS_NAMESPACE = "valmer_connectors"`
- storage app constant: `VALMER_MARKETS_STORAGE_APP = "valmer_connectors"`
- index columns: `time_index`, `asset_identifier`
- `asset_identifier` foreign key:
  `AssetTable.unique_identifier`

`ValmerAssetDetailsTable` already owns static Valmer asset detail rows:

```text
src/valmer_connectors/meta_tables/valmer_asset_details.py
```

Verified contract:

- class: `ValmerAssetDetailsTable`
- runtime MetaTable identifier: `valmer_connectors.ValmerAssetDetails`
- primary key: `asset_uid`
- unique Valmer identifier column: `valmer_unique_identifier`
- existing runtime helper: `ensure_valmer_asset_detail_runtime(...)`
- existing asset-uid resolver: `resolve_valmer_asset_details(asset_uids, ...)`
- existing freshness resolver:
  `resolve_valmer_asset_detail_versions(asset_uids, ...)`

The current package does not expose a public read API for published vector
storage and does not expose a consumer-friendly asset-detail lookup that accepts
both `ValmerAssetDetailsTable.valmer_unique_identifier` and
`AssetTable.unique_identifier`.

`src/valmer_connectors/services/` is an operational service layer for updates,
repairs, and runtime validation. Read-only reusable query helpers do not belong
there.

## Files Created In Phase 1

Phase 1 creates these source files:

```text
src/valmer_connectors/queries/__init__.py
src/valmer_connectors/queries/_normalization.py
src/valmer_connectors/queries/vector_quotes.py
src/valmer_connectors/queries/asset_details.py
```

Phase 1 creates these tests:

```text
tests/test_valmer_vector_queries.py
tests/test_valmer_asset_detail_queries.py
```

No migration file is created. The promotion is a read API over existing
published storage and existing MetaTables.

## Files Modified In Phase 1

Phase 1 modifies these documentation files:

```text
docs/data-nodes.md
docs/markets.md
docs/SUMMARY.md
```

`docs/data-nodes.md` will document the vector query API beside the
`ValmerVectorPricesStorage` publication contract.

`docs/markets.md` will document the asset-detail identifier lookup beside the
existing AssetTable and `ValmerAssetDetailsTable` identity rules.

`docs/SUMMARY.md` contains the entry for this implementation plan. Phase 1
does not create another summary entry because `docs/data-nodes.md` and
`docs/markets.md` already exist in the summary.

Phase 1 does not modify these code files:

```text
src/valmer_connectors/data_nodes/valmer_vector_storage.py
src/valmer_connectors/meta_tables/valmer_asset_details.py
src/valmer_connectors/services/__init__.py
src/valmer_connectors/__init__.py
```

The query modules import the storage class, table class, namespace constants,
and runtime helpers from those existing files. The storage and MetaTable schema
owners remain unchanged.

## New Package Responsibilities

### `src/valmer_connectors/queries/__init__.py`

Public import surface for read helpers.

Exports:

```python
clean_valmer_identifiers
filter_valmer_vector_columns
latest_dirty_price_by_identifier
normalize_valmer_quote_frame
read_valmer_asset_detail_alias_frame
read_valmer_asset_detail_maturity_fields
read_valmer_history
read_valmer_last_observation
read_valmer_yield_history
resolve_valmer_detail_identifier_aliases
valmer_vector_node
valmer_vector_node_identifier
valmer_vector_storage_columns
```

This file will not import operational services or update code.

### `src/valmer_connectors/queries/_normalization.py`

Internal normalization shared by query modules.

Functions:

```python
clean_valmer_identifiers(values) -> list[str]
string_or_none(value) -> str | None
to_utc_datetime(value) -> datetime | None
```

Rules:

- preserve input order;
- drop `None`, empty strings, `"nan"`, and `"none"`;
- deduplicate identifiers;
- normalize timestamp-like values to UTC `datetime`;
- keep this file internal by naming it with a leading underscore.

### `src/valmer_connectors/queries/vector_quotes.py`

Public read API for published `ValmerVectorPricesStorage`.

Constants:

```python
DEFAULT_VALMER_VECTOR_COLUMNS = (
    "dirty_price",
    "clean_price",
    "yield_rate",
    "duration",
    "macaulay_duration",
    "monetary_duration",
    "convexity",
    "spread",
)
VALMER_VECTOR_INDEX_COLUMNS = ("time_index", "asset_identifier")
```

Functions:

```python
valmer_vector_node_identifier() -> str
valmer_vector_node() -> APIDataNode
valmer_vector_storage_columns() -> frozenset[str]
filter_valmer_vector_columns(columns: Sequence[str] | None) -> list[str]
read_valmer_history(asset_identifiers, start_date, end_date=None, columns=None) -> pd.DataFrame
read_valmer_last_observation(asset_identifiers, as_of=None, columns=None, latest_search_start=None) -> pd.DataFrame
latest_dirty_price_by_identifier(asset_identifiers, as_of=None) -> dict[str, float]
read_valmer_yield_history(asset_identifiers, start_date, end_date=None) -> pd.DataFrame
normalize_valmer_quote_frame(frame) -> pd.DataFrame
```

Implementation rules:

- `valmer_vector_node_identifier()` derives the identifier from
  `ValmerVectorPricesStorage.__metatable_identifier__`.
- The code does not read `fundcompetition.config.settings.VECTOR_IDENTIFIER`.
- The code does not define a new setting for the vector identifier.
- `valmer_vector_node()` imports `APIDataNode` inside the function and calls
  `APIDataNode.build_from_meta_table(...)` using the runtime-bound
  `ValmerVectorPricesStorage` MetaTable.
- `filter_valmer_vector_columns(...)` inspects
  `ValmerVectorPricesStorage.__table__.columns`.
- `filter_valmer_vector_columns(...)` always includes `time_index` and
  `asset_identifier`.
- `read_valmer_history(...)` calls `get_df_between_dates(...)` with
  `dimension_filters={"asset_identifier": cleaned_identifiers}`.
- `read_valmer_history(...)` returns an empty `DataFrame` without calling the
  platform when identifier cleanup removes every input value.
- `read_valmer_last_observation(...)` calls `APIDataNode.get_last_observation(...)`
  with one `dimension_range_map` entry per `asset_identifier` and returns one
  backend-selected row per asset.
- `read_valmer_last_observation(...)` exposes `latest_search_start` as an
  optional backend query lower bound; it must not default to a full-history
  pandas fallback.
- `latest_dirty_price_by_identifier(...)` returns a dictionary keyed by
  canonical asset identifier and omits rows where `dirty_price` is missing.
- `read_valmer_yield_history(...)` replaces the donor's
  `yield_history_for_spread(...)` name. Yield history is generic vector data,
  not spread-analysis business logic.
- `normalize_valmer_quote_frame(...)` resets a `time_index` or
  `asset_identifier` index back to columns, normalizes `time_index` to UTC, and
  coerces numeric Valmer quote fields.

### `src/valmer_connectors/queries/asset_details.py`

Public lookup API for static Valmer detail rows.

Functions:

```python
read_valmer_asset_detail_alias_frame(asset_identifiers, timeout=None) -> pd.DataFrame
read_valmer_asset_detail_maturity_fields(asset_identifiers, timeout=None) -> pd.DataFrame
resolve_valmer_detail_identifier_aliases(asset_identifiers, timeout=None) -> dict[str, str]
```

Implementation rules:

- Import `AssetTable` from `msm.models.assets`.
- Import `ValmerAssetDetailsTable` and
  `ensure_valmer_asset_detail_runtime(...)` from
  `valmer_connectors.meta_tables.valmer_asset_details`.
- Use `ensure_valmer_asset_detail_runtime(...)` to get the active governed
  repository context.
- Use `compile_markets_statement(...)` and `execute_markets_operation(...)`
  from `msm.repositories.base`.
- Use `operation_result_rows(...)` from `msm.api.base`.
- Query only projected columns needed by the public helper.
- Join `ValmerAssetDetailsTable.__table__` to `AssetTable.__table__` on
  `ValmerAssetDetailsTable.asset_uid == AssetTable.uid`.
- Accept identifiers matching either
  `ValmerAssetDetailsTable.valmer_unique_identifier` or
  `AssetTable.unique_identifier`.
- Include both table models in the governed operation model list:
  `[ValmerAssetDetailsTable, AssetTable]`.
- Return alias-expanded rows so both the Valmer unique identifier and the
  canonical `AssetTable.unique_identifier` can resolve.
- Keep `string_or_none(...)` internal through `_normalization.py`.

Projection for maturity/static detail lookup:

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

`asset_identifier` in the returned frame is the lookup alias column. It can
contain either the Valmer unique identifier or the canonical
`AssetTable.unique_identifier` depending on the row emitted by alias expansion.

## Donor-To-Target Mapping

### Vector donor

Donor file:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/src/fundcompetition/local_valmer/vector_quotes.py
```

Mapping:

| Donor symbol | Target symbol | Disposition |
| --- | --- | --- |
| `DEFAULT_VALMER_COLUMNS` | `DEFAULT_VALMER_VECTOR_COLUMNS` | Keep, rename for package clarity. |
| `valmer_node()` | `valmer_vector_node()` | Keep, derive identifier from `ValmerVectorPricesStorage`. |
| `valmer_storage_columns()` | `valmer_vector_storage_columns()` | Keep, return `frozenset[str]`. |
| `read_valmer_history(...)` | `read_valmer_history(...)` | Keep, remove project setting dependency. |
| `read_valmer_last_observation(...)` | `read_valmer_last_observation(...)` | Keep, use backend latest-observation lookup with optional `latest_search_start`. |
| `latest_dirty_price_by_identifier(...)` | `latest_dirty_price_by_identifier(...)` | Keep. |
| `yield_history_for_spread(...)` | `read_valmer_yield_history(...)` | Keep behavior, remove spread-specific name. |
| `selected_columns(...)` | `filter_valmer_vector_columns(...)` | Keep behavior, clearer name. |
| `clean_identifiers(...)` | `clean_valmer_identifiers(...)` | Move to `_normalization.py` and export from query package. |
| `normalize_valmer_frame(...)` | `normalize_valmer_quote_frame(...)` | Keep behavior, clearer name. |

### Asset-detail donor

Donor file:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/src/fundcompetition/local_valmer/asset_details.py
```

Mapping:

| Donor symbol | Target symbol | Disposition |
| --- | --- | --- |
| `valmer_asset_detail_maturity_fields(...)` | `read_valmer_asset_detail_maturity_fields(...)` | Keep, replace local executor with governed market operation. |
| `valmer_asset_detail_alias_frame(...)` | `read_valmer_asset_detail_alias_frame(...)` | Keep, make it the public consumer-facing lookup frame. |
| `string_or_none(...)` | `string_or_none(...)` in `_normalization.py` | Keep internal. |
| `execute_select` dependency | none | Drop. Use `compile_markets_statement(...)` and `execute_markets_operation(...)`. |

### Spread-analysis donor

Donor file:

```text
/Users/jose/mainsequence-dev/main-sequence-workbench/projects/mexicofundcompetition-9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16/src/fundcompetition/local_valmer/spread_market_data.py
```

Disposition:

- Implemented in Phase 2.
- No spread-analysis source file is created in Phase 1.
- The donor duplicates identifier cleanup and wraps vector reads. That logic
  must depend on `valmer_connectors.queries.vector_quotes` after Phase 1 exists.

Phase 2 creates exactly these files:

```text
src/valmer_connectors/analytics/__init__.py
src/valmer_connectors/analytics/spread_market_data.py
tests/test_valmer_spread_market_data.py
```

Phase 2 maps the donor symbols as follows:

| Donor symbol | Target symbol | Disposition |
| --- | --- | --- |
| `SPREAD_SNAPSHOT_COLUMNS` | `SPREAD_SNAPSHOT_COLUMNS` | Keep in analytics module. |
| `default_start_date()` | `default_start_date()` | Keep in analytics module. |
| `fetch_yield_history(...)` | `fetch_yield_history(...)` | Keep in analytics module, call `read_valmer_yield_history(...)`. |
| `fetch_market_snapshot(...)` | `fetch_market_snapshot(...)` | Keep in analytics module, call `read_valmer_last_observation(...)`. |
| `clean_identifiers(...)` | none | Drop duplicate; import `clean_valmer_identifiers(...)`. |
| `to_utc_datetime(...)` | none | Drop duplicate; Phase 2 imports timestamp normalization from the Phase 1 query package. |

Phase 2 remains separate because spread analysis is an analytics convenience,
not the core Valmer storage read API.

## Test Coverage

`tests/test_valmer_vector_queries.py` covers:

- vector node identifier derived from
  `ValmerVectorPricesStorage.__metatable_identifier__`;
- no dependency on `fundcompetition.config`;
- storage column filtering against `ValmerVectorPricesStorage.__table__`;
- index columns retained even when the caller requests only quote columns;
- identifier cleanup order and deduplication;
- empty identifier input avoids platform calls;
- `read_valmer_history(...)` calls `APIDataNode.get_df_between_dates(...)` with
  the correct `dimension_filters` and column list;
- normalization resets index columns, coerces `time_index` to UTC, and coerces
  numeric quote columns;
- latest observation calls the backend latest-observation endpoint and returns
  one row per asset;
- dirty-price map omits missing dirty prices;
- yield history requests only yield-related vector columns.

`tests/test_valmer_asset_detail_queries.py` covers:

- empty identifier input avoids governed execution;
- lookup by `ValmerAssetDetailsTable.valmer_unique_identifier`;
- lookup by `AssetTable.unique_identifier`;
- governed SQL compilation with `operation="select"` and `access="read"`;
- model list includes both `ValmerAssetDetailsTable` and `AssetTable`;
- alias expansion emits both Valmer identifier and canonical AssetTable
  identifier rows;
- duplicate aliases collapse deterministically with first row retained;
- maturity/static detail projection includes the exact public columns listed in
  this plan.

`tests/test_valmer_spread_market_data.py` covers:

- default five-year UTC start date behavior;
- empty identifier input avoiding query calls;
- yield-history calls into `read_valmer_yield_history(...)` with cleaned
  identifiers and UTC timestamps;
- wide yield-history pivoting by `time_index` and asset identifier;
- empty yield-history output when `yield_rate` is missing;
- market snapshot calls into `read_valmer_last_observation(...)` with
  `SPREAD_SNAPSHOT_COLUMNS`;
- numeric coercion for spread snapshot fields.

## Implementation Order

1. Create `src/valmer_connectors/queries/_normalization.py`.
2. Create `src/valmer_connectors/queries/vector_quotes.py`.
3. Create `src/valmer_connectors/queries/asset_details.py`.
4. Create `src/valmer_connectors/queries/__init__.py` with the public exports.
5. Add `tests/test_valmer_vector_queries.py`.
6. Add `tests/test_valmer_asset_detail_queries.py`.
7. Update `docs/data-nodes.md`.
8. Update `docs/markets.md`.
9. Confirm `docs/SUMMARY.md` still links this implementation plan.
10. Run the focused query tests.

Phase 2 implementation order:

1. Create `src/valmer_connectors/analytics/__init__.py`.
2. Create `src/valmer_connectors/analytics/spread_market_data.py`.
3. Add `tests/test_valmer_spread_market_data.py`.
4. Update `docs/data-nodes.md` with the public analytics helper surface.
5. Confirm `docs/SUMMARY.md` describes the query and analytics surfaces.
6. Run the focused analytics tests.

## Explicit Non-Goals

- Do not copy donor modules verbatim.
- Do not import from `fundcompetition`.
- Do not add a new setting for the vector identifier.
- Do not place read helpers under `src/valmer_connectors/services/`.
- Do not change `ValmerVectorPricesStorage` schema.
- Do not change `ValmerAssetDetailsTable` schema.
- Do not add Valmer detail fields to `AssetTable`.
- Do not create a migration.
- Do not broaden Valmer asset registration.
- Do not promote fund-competition dashboards, APIs, curve scenarios, or spread
  models as part of Phase 1.

Phase 2 keeps the same non-goals and additionally does not make spread analysis
part of the core vector storage API.

## Success Criteria

Phase 1 is complete when:

- downstream projects can import vector readers from
  `valmer_connectors.queries`;
- vector readers derive the node identifier from
  `ValmerVectorPricesStorage`;
- vector readers filter requested columns against the registered storage schema;
- latest-observation and dirty-price helpers work without project-specific
  settings;
- asset-detail readers resolve both
  `ValmerAssetDetailsTable.valmer_unique_identifier` and
  `AssetTable.unique_identifier`;
- asset-detail readers use governed SQL compilation and execution;
- focused tests cover the promoted behavior without importing the fund
  competition project;
- documentation describes the public query surface and confirms that storage
  schema, MetaTable schema, and registration workflows are unchanged.

Phase 2 is complete when:

- downstream projects can import spread analytics helpers from
  `valmer_connectors.analytics`;
- `fetch_yield_history(...)` calls the public vector query API and returns a
  wide yield-history frame indexed by `time_index`;
- `fetch_market_snapshot(...)` calls the public latest-observation query API and
  returns numeric spread snapshot fields;
- spread analytics helpers reuse Phase 1 identifier and timestamp
  normalization;
- no spread analytics helper defines a DataNode identifier, storage schema, or
  platform read path directly;
- focused tests cover the promoted analytics behavior without importing the
  fund competition project;
- documentation describes the analytics surface as a wrapper over
  `valmer_connectors.queries`, not as part of the core storage API.
