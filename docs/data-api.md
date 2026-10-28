# Valmer Data API

The Valmer data API is how other applications read Valmer data
([ADR 0010](adr/0010-valmer-data-api-consumer-boundary.md)). It is a FastAPI
release of this repository, separate from the control plane: workflow key
`valmer-data-api`, source path `api/valmer_data_api/main.py`. Business logic
lives under `src/valmer_connectors/data_api/`; route handlers stay thin.

The API reads Valmer's own tables by MetaTable identifier, as the control plane
does, and attaches no markets runtime. It reads Markets registries (Asset rows,
for identifier aliases) through the Markets API. Its queries run as its own
workload User, so callers hold no roles on Valmer's tables.

## Finding And Calling The API

A consumer finds the release the way `msm.client.discovery` finds the Markets
API: the FastAPI release of the `valmer-connectors` CodeRepository in the
caller's Environment, then `ResourceRelease.resolve_runtime_access()` for its
URL and a short-lived token. The release name is `valmer_data_api`, from the
source path, as the Markets API is `api` from `api/main.py`. A consumer
resource declares `reader` on the `valmer-connectors` branch in its workflow:

```yaml
access:
  branches:
    - repository: valmer-connectors
      policy: reader
```

Consumers honour these overrides:

| Variable | Effect |
| --- | --- |
| `VALMER_DATA_API_URL` | Call this URL; with `VALMER_DATA_API_TOKEN` as a Bearer token, otherwise with the caller's platform session |
| `VALMER_DATA_API_RELEASE_UID` | Use this release instead of searching |
| `VALMER_DATA_API_RELEASE_NAME` | Release name to search for (default `valmer_data_api`) |
| `VALMER_DATA_API_REPOSITORY_NAME` | Repository name to search for (default `valmer-connectors`) |

The API installs the SDK request identity once and refuses requests without a
platform credential.

## Contract

Request and response models live in `src/valmer_connectors/data_api/models.py`.
Routes change only by addition. Tabular routes answer with the ms-markets
`core.tabular_frame@v1` frame (`msm.api.http.TabularFrameResponse`); its `meta`
carries route context (`valuation_time`, `total_rows`, `source`). Datetimes are
UTC. Paged routes take `limit` (at most 25,000) and `offset`; a page shorter
than `limit` is the last one.

A request that names an unknown column fails with 400. A valuation date without
a vector snapshot fails with 404. Unregistered Valmer tables fail with 503.

| Method and path | Request | Answer |
| --- | --- | --- |
| `GET /health` | | `{"status": "ok", "service": "valmer-data-api"}` |
| `GET /api/v1/datasets/` | | `DatasetsResponse`: the published vector table's identifier, uid, index names and columns |
| `GET /api/v1/vector/valuation-dates/?limit=` | `limit` 1 to 2,000, default 400 | `ValuationDatesResponse`, newest first |
| `POST /api/v1/vector/snapshot/` | `VectorSnapshotRequest` | rows `time_index`, `asset_identifier` and the requested vector columns on one valuation date; `meta.valuation_time` |
| `POST /api/v1/vector/history/` | `VectorHistoryRequest` | rows `time_index`, `asset_identifier` and the requested columns, by `time_index`, `asset_identifier` |
| `POST /api/v1/vector/latest/` | `VectorLatestRequest` | the latest row per identifier at or before `as_of`; `meta.source` is `latest` or `history_fallback` |
| `POST /api/v1/assets/details/` | `AssetDetailsRequest` | rows `asset_identifier` (the Valmer identifier), `asset_uid` and the requested detail columns |
| `POST /api/v1/assets/maturity/` | `AssetMaturityRequest` | static maturity rows keyed by Valmer and Markets identifiers alike: `asset_uid`, `asset_table_identifier`, `asset_identifier`, `valmer_unique_identifier`, `valmer_security_type`, `valmer_issuer`, `valmer_series`, `valmer_full_name`, `valmer_issue_date`, `valmer_maturity_date`, `maturity_date`, `valmer_face_value`, `valmer_coupon_frequency`, `valmer_coupon_rate` |
| `POST /api/v1/bonds/snapshot/` | `BondSnapshotRequest` | rows `asset_uid`, `asset_identifier`, the requested detail and vector columns and `time_index`, for details joined with the vector on one valuation date; `meta.valuation_time` |
| `POST /api/v1/bonds/history/` | `BondHistoryRequest` | the same rows over a time range, by `time_index`, `asset_identifier` |
| `GET /api/v1/bonds/filters/?valuation_date=&security_type=&issuer=` | | `BondFiltersResponse` |
| `POST /api/v1/bonds/search/` | `BondSearchRequest` | one page of rows `asset_uid`, `asset_identifier`, `security_type`, `issuer`, `series`, `full_name`, `hr_rating`, `fitch_rating`, `sp_rating`, `moodys_rating`, `spread`, `yield_rate`, `clean_price`, `dirty_price`, `duration`, `maturity_date`, `liquidity`, `valuation_date`; `meta.total_rows`, `meta.valuation_time` |
| `GET /api/v1/bonds/instrument/?asset_identifier=` | | `BondInstrumentResponse`, found by Valmer or Markets identifier |
| `GET /api/v1/bond-comparison/filters/?valuation_date=&security_type=` | | `BondComparisonFiltersResponse` |
| `POST /api/v1/bond-comparison/universe/` | `BondComparisonUniverseRequest` | at most `max_rows` rows of the universe (details, the vector, the nearest reference M bond, the previous quote and one-year spread statistics), ordered by the axis metric descending; `meta.total_rows`, `meta.valuation_time`. A caller refuses a universe whose `total_rows` exceeds `max_rows` |

Default columns: vector reads use `dirty_price`, `clean_price`, `yield_rate`,
`duration`, `macaulay_duration`, `monetary_duration`, `convexity` and `spread`;
asset-detail reads use `security_type`, `issuer`, `series`, `full_name` and
`maturity_date`.

A valuation date names a UTC calendar date; the API resolves it to the vector's
observation time on that date. Omitting it selects the latest date.

## What Stays With Consumers

The API serves Valmer data filtered, joined and paged. Analytics stay with the
consumer: spreads, relative value, regression and movers. Valmer's curves,
fixings and daily Index values are read through Markets (the pricing
market-data set bindings and the Index dataset routes), not through this API.
