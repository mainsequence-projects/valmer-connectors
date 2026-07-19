# ADR 0009: External Reference-Rate Observations For Mexico-US Spread Analytics

## Status

Implemented; the live migration and namespaced FRED smoke update are verified.
Banxico, bounded backfill, non-namespaced incremental, and scheduled-job
verification remain pending.

## Date

2026-07-18

## Success Criteria

This ADR defines the implementation contract for the external reference-rate
observations required by the Mexico-US spread analytics shell. The future
implementation must:

- keep the data producers in the Valmer Connectors repository while preserving
  source-specific package ownership
- continue using the existing Valmer vector for Mexican government-bond yields
- publish four US Treasury constant-maturity yields from FRED
- publish the Federal Funds target-range upper limit from FRED
- publish the Banco de Mexico policy target from Banxico SIE
- ensure the canonical `interest_rate` `IndexType` exists and register every
  series as an `Index` identity
- publish the six external series through a new index-indexed reference-rate
  storage contract, not through pricing `IndexFixingsStorage`
- define the storage's complete project-owned MetaTable identity, discovery
  metadata, column metadata, foreign key, and stable storage hash component
- include `IndexTable` as migration reference metadata without making it a
  project-owned migration model, and attach the new storage during runtime
  bootstrap before constructing either producer
- use a project-owned `IndexDataNodeConfiguration` subclass whose index scope,
  bootstrap lookback, and bounded backfill dates are explicit hashed fields
- normalize percentage-form source values to decimal rates
- bootstrap only the most recent 90 days by default
- make the five-year history required by the shell an explicit, distinctly
  hashed, and separately verified bounded backfill
- run the first shared-backend DataNode validation with an explicit
  `hash_namespace` before any non-namespaced production run
- keep benchmark selection, common-date alignment, spreads, statistics, and
  trading thresholds outside the source DataNodes
- add migrations, tests, schedules, operational documentation, and live
  platform verification before claiming the feeds are production-ready

## Context

The debt-analysis application needs a Mexico-US spread shell with:

- 2Y, 5Y, 10Y, and 30Y tenor selection
- a Valmer M-Bond benchmark selected near each target tenor
- a matching US Treasury constant-maturity yield
- current and historical Mexico-US yield spreads
- mean, minimum, maximum, and z-score statistics
- Banxico and Federal Reserve policy-rate levels and their differential

The repository already has the Mexican market side of this calculation:

- `ValmerVectorPricesStorage` publishes Valmer vector observations.
- M/BONOS rows expose the yields and maturity information needed to select the
  Mexican benchmark.
- `src/banxico` already contains a Banxico SIE client and TIIE/CETE fixing
  producer.
- TIIE and CETE fixing rates are published through the pricing-specific
  `IndexFixingsStorage` contract.

The missing source observations are US Treasury yields, the Federal Funds
target-range upper limit, and the Banco de Mexico policy target.

The existing Banxico fixing producer establishes the ingestion discipline to
reuse: resolve secrets at runtime, validate source metadata, calculate an
incremental window per index, normalize percentages to decimals, emit UTC
index-stamped frames, and schedule a small launcher. It does not establish that
all rate-shaped data belongs in pricing fixing storage.

Main Sequence DataNodes are storage-first update processes. The registered
`PlatformTimeIndexMetaTable` owns the schema, while the DataNode owns the
incremental update behavior:

```text
https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/data_nodes/
```

ms-markets provides `IndexTimestampedDataNode` for timestamped facts keyed by
`IndexTable.unique_identifier`. That is the correct base for these external
market and policy observations.

## Verified Source Series

The following source mapping is accepted for this implementation:

| Canonical Index Identifier | Source | Source Series | Meaning |
| --- | --- | --- | --- |
| `US_TREASURY_CMT_2Y` | FRED | `DGS2` | 2-year US Treasury constant-maturity yield |
| `US_TREASURY_CMT_5Y` | FRED | `DGS5` | 5-year US Treasury constant-maturity yield |
| `US_TREASURY_CMT_10Y` | FRED | `DGS10` | 10-year US Treasury constant-maturity yield |
| `US_TREASURY_CMT_30Y` | FRED | `DGS30` | 30-year US Treasury constant-maturity yield |
| `FED_FUNDS_TARGET_UPPER` | FRED | `DFEDTARU` | Federal Funds target-range upper limit |
| `BANXICO_POLICY_TARGET` | Banxico SIE | `SF61745` | Banco de Mexico overnight interbank policy target |

The FRED Treasury series are daily, not seasonally adjusted, percentage-form
observations from the Federal Reserve H.15 Selected Interest Rates release:

```text
https://fred.stlouisfed.org/series/DGS2
https://fred.stlouisfed.org/series/DGS5
https://fred.stlouisfed.org/series/DGS10
https://fred.stlouisfed.org/series/DGS30
```

`DFEDTARU` is the daily upper limit of the Federal Funds target range:

```text
https://fred.stlouisfed.org/series/DFEDTARU
```

Banco de Mexico SIE documentation identifies `SF61745` as the policy target
series. Production enablement must still validate its current SIE metadata
through the token-authenticated API before publication:

```text
https://www.anterior.banxico.org.mx/dyn/ley-de-transparencia/consultas-frecuentes/%7B960A6514-B048-02B8-4BF2-920034786674%7D.pdf
```

## Decision

Add a source-neutral project-owned storage contract for externally observed
reference rates and add separate Banxico and FRED producers that write to it.

The target flow is:

```text
Valmer vector M/BONOS yields ------------------------------+
                                                            |
FRED DGS2/DGS5/DGS10/DGS30 -> ReferenceRateObservationsStorage  |
FRED DFEDTARU              -> ReferenceRateObservationsStorage  +--> debt-analysis API
Banxico SF61745             -> ReferenceRateObservationsStorage  |       |
                                                            |       +--> benchmark selection
                                                            |       +--> common-date join
                                                            |       +--> spreads and statistics
                                                            |       +--> policy differential
                                                            +-------+
```

The connector publishes observed source facts. The application API publishes
the requested analytical view.

## Repository Ownership Boundary

The Valmer Connectors repository owns the operational market-data integration,
but source-specific code remains separated:

```text
src/valmer_connectors/data_nodes/reference_rate_observations.py
src/banxico/policy_rates.py
src/fred/client.py
src/fred/settings.py
src/fred/reference_rates.py
scripts/update_banxico_policy_rates.py
scripts/update_fred_reference_rates.py
```

The existing `BanxicoSieClient` and Banxico token resolution should be reused.
The policy-rate producer must not be added to the TIIE/CETE fixing definition
list merely because its value is also a rate.

FRED-specific HTTP requests, metadata validation, missing-value parsing, and
secret handling belong under `src/fred`. Package discovery must include
`fred*` before scheduled jobs are built.

## Canonical Index Registration

Ensure the canonical `interest_rate` `IndexType` exists through the supported
`IndexType.upsert(...)` path, then register all six series as `Index` rows with
`index_type="interest_rate"`. They are canonical market observables, not
tradable `Asset` rows. They do not need pricing-owned
`IndexConventionDetails` unless a later pricing workflow explicitly adopts one
of them as a reconstructable pricing index.

Each `Index.metadata_json` should preserve bounded source metadata:

- `source_series_id`
- `currency`
- `country`
- `source_unit = "percent"`
- `observation_type`
- `tenor_months` for Treasury constant-maturity series
- `source_agency` when the retrieval provider and originating agency differ

Use `provider="FRED"` for the FRED retrieval namespace and retain the Board of
Governors as source-agency metadata. Use `provider="Banco de Mexico"` for the
Banxico policy target.

Canonical identifiers must remain independent of provider series IDs. For
example, application code selects `US_TREASURY_CMT_10Y`, while the connector
metadata maps it to FRED `DGS10`.

## Storage Contract

Create `ReferenceRateObservationsStorage` with this contract:

| Column | Type | Nullability | Meaning |
| --- | --- | --- | --- |
| `time_index` | timezone-aware datetime | non-null | UTC observation date supplied by the source |
| `index_identifier` | `String(255)` foreign key | non-null | Canonical `IndexTable.unique_identifier` |
| `rate` | float | non-null | Observed annualized rate in decimal form |

Required MetaTable configuration:

```python
__metatable_identifier__ = "reference_rate_observations"
__metatable_description__ = (
    "Daily external reference-rate observations keyed by UTC observation date "
    "and canonical Index identifier for cross-market spread, policy-rate, and "
    "diagnostic analytics."
)
__metatable_extra_hash_components__ = {
    "storage_name": "reference_rate_observations",
}
__time_index_name__ = "time_index"
__index_names__ = ["time_index", "index_identifier"]
__cadence__ = "1d"
```

The storage must use `ValmerMarketsTimeIndexMetaTableMixin` and `MarketsBase`
so its authored identifier resolves to the globally scoped
`valmer_connectors.reference_rate_observations` MetaTable identifier and its
physical table name follows the project markets naming helper. Do not hand-build
or override that physical name.

Every `mapped_column(...)` must provide intention-rich
`info={"label": ..., "description": ...}` metadata. At minimum:

- `time_index` describes the source observation date normalized to UTC
- `index_identifier` describes the canonical `IndexTable.unique_identifier`
  relationship
- `rate` states that the source percentage was normalized exactly once to an
  annualized decimal rate

`index_identifier` must use a restrictive SQLAlchemy foreign key to
`IndexTable.unique_identifier`, following the existing ms-markets
`IndexFixingsStorage` FK pattern.

Add the storage class to `src/migrations/registry.py`. Also add `IndexTable` to
`VALMER_REFERENCE_MODELS` in `src/migrations/__init__.py` so
`metadata_for_models(...)` can resolve the foreign-key target. `IndexTable` is
reference metadata only: it must not be added to `VALMER_MIGRATION_MODELS`,
registered as a Valmer-owned model, or emitted by Valmer Alembic DDL.

Generate a new SDK-managed Alembic revision under the project provider and
upgrade `migrations:migration` before executing either producer. Do not modify
the applied baseline revision.

Migration registration does not attach the class in later Python processes.
Add `ReferenceRateObservationsStorage` to the project-owned model list returned
by `_valmer_markets_models(...)`, or provide an equivalent single runtime
bootstrap path that calls `msm.start_engine(models=...)` with the storage class.
Both producer entry points must call the normal runtime bootstrap before node
construction. Runtime bootstrap attaches already-migrated tables; it must not
create schemas or run migrations.

## Why This Is Not IndexFixingsStorage

`IndexFixingsStorage` is a pricing contract for historical fixings consumed by
floating-rate bonds, swaps, and pricing engines. Existing TIIE and CETE rows
belong there because pricing workflows request those fixings.

The new observations have different semantics:

- Treasury constant-maturity yields are analytical benchmark yields, not
  coupon or swap fixings.
- The Banxico and Fed target rates are policy settings, not instrument
  fixings.
- The debt-analysis shell consumes them for comparison and diagnostics, not
  for coupon accrual or fixing resolution.

Putting them in `IndexFixingsStorage` would make them discoverable to pricing
workflows under a false contract. A separate storage preserves the semantic
boundary while retaining the canonical Index identity and timestamped frame
behavior.

`IndexValuesStorage` is also not appropriate. That storage is for calculated
indexes and requires immutable calculation-definition provenance. These six
series are externally supplied observations.

## DataNode Design

Implement two source-specific DataNodes over the shared storage:

```text
FredReferenceRatesNode(IndexTimestampedDataNode)
BanxicoPolicyRatesNode(IndexTimestampedDataNode)
```

Bare `IndexDataNodeConfiguration` cannot accept index scope fields. Define a
project-owned configuration subclass:

```python
from datetime import datetime

from msm.data_nodes.indices import IndexDataNodeConfiguration
from pydantic import Field


class ReferenceRateObservationConfiguration(IndexDataNodeConfiguration):
    index_unique_identifiers: list[str] = Field(
        ...,
        description=(
            "Canonical Index unique identifiers updated by this source-specific "
            "reference-rate producer."
        ),
        examples=[["US_TREASURY_CMT_2Y", "US_TREASURY_CMT_10Y"]],
    )
    bootstrap_lookback_days: int = Field(
        default=90,
        ge=1,
        description=(
            "Fixed calendar-day lookback used only when storage has no progress "
            "for an index and no bounded backfill range is configured."
        ),
        examples=[90],
    )
    backfill_end: datetime | None = Field(
        default=None,
        description="Inclusive UTC end of an explicit bounded backfill.",
        examples=["2026-04-18T00:00:00Z"],
    )
```

The concrete implementation must validate that identifiers are non-empty and
unique. Use the inherited `offset_start` field as the inclusive UTC start of an
explicit bounded backfill. Validate that `offset_start` and `backfill_end` are
either both absent or both present, are timezone-aware, and satisfy
`offset_start <= backfill_end`. `index_unique_identifiers`,
`bootstrap_lookback_days`, inherited `offset_start`, and `backfill_end` all
participate in `update_hash`; do not hash-exclude them.

Normal scheduled configurations leave both backfill dates unset. Compute the
rolling first-run date inside update-window resolution from the fixed
`bootstrap_lookback_days`; do not insert a dynamically calculated
`now - 90 days` datetime into configuration, because that would rotate
`update_hash` on every launcher execution.

Both nodes set `configuration_class` to the project configuration, return the
shared `ReferenceRateObservationsStorage` from `_required_storage_table()`, and
implement the source-specific per-index update loop. Runtime HTTP clients,
secrets, timeouts, retries, and callable builders remain runtime behavior and
must not be Pydantic configuration fields.

Each producer must:

- calculate its update window independently for every index identifier
- request only the required source range
- normalize timestamps to `datetime64[ns, UTC]`
- divide percentage-form values by `100.0`
- set the frame index to `time_index` and `index_identifier`
- reject duplicate keys
- return an empty valid frame when the requested identity is already current
- expose source errors instead of converting them to empty successful updates

FRED API access should use the official series metadata and observations
endpoints:

```text
https://fred.stlouisfed.org/docs/api/fred/series.html
https://fred.stlouisfed.org/docs/api/fred/series_observations.html
```

The FRED client must treat `"."` as a missing observation and omit that row.
It must not forward-fill missing Treasury dates. Metadata validation must
confirm the expected source ID, title terms, daily frequency, percent unit, and
seasonal-adjustment status before production publication.

The Banxico producer must validate `SF61745` metadata through the existing SIE
client and require policy-target title terms. It must reuse the existing
`BANXICO_TOKEN` secret contract.

## Initial Window And Backfill Policy

The default first execution must request only the most recent 90 calendar days.
It must not fetch from 1970, source inception, or an unbounded earliest date.

The normal incremental configuration is stable across executions:

- `bootstrap_lookback_days=90`
- `offset_start=None`
- `backfill_end=None`

For an identity with no persisted progress, resolve the inclusive start date
from the runtime end date and the fixed 90-calendar-day lookback. Once progress
exists, start after the latest persisted observation for that identity.

The staged rollout is:

1. Register indexes and migrate storage.
2. Execute and validate a 90-day source window.
3. Confirm row counts, latest dates, units, and representative values.
4. Run an explicit one-off bounded backfill covering the missing portion of the
   five-year history through the day before the configured 90-day smoke request
   start.
5. Revalidate the complete `1Y`, `3Y`, and `5Y` application windows.
6. Enable normal incremental schedules.

Normal incremental runs start after the last persisted observation for each
identity. The bounded backfill uses explicit `offset_start` and
`backfill_end` values, therefore receives a distinct deterministic
`update_hash`, ignores normal last-progress start selection for that bounded
range, and ends before the configured smoke request window. This makes the
five-year backfill executable after the recent smoke data already exists.

`hash_namespace` isolates update identity, not storage identity. The first
namespaced 90-day live smoke therefore writes the intended migrated storage and
becomes its initial production data. Before enabling the normal non-namespaced
schedule, verify that the normal updater reads persisted storage progress,
starts after the smoke rows, and does not attempt duplicate keys. If a test
requires isolated storage rather than a live smoke, pass a separately
registered test storage class in addition to using an explicit namespace.

Historical source revisions are handled as explicit repairs; the normal append
path must not silently rewrite history. A repair must choose an inclusive
cutoff per affected index, call
`TimeIndexMetaTable.delete_after_date(cutoff, dimension_filters={"index_identifier": [...]})`,
and republish that scoped tail. Do not use raw SQL, compiled SQL, or an unscoped
`delete_after_date(None)` against DataNode storage.

## Missing Dates And As-Of Semantics

Mexican and US market calendars differ. Source DataNodes must preserve actual
observations and must not invent aligned values.

For M-Bond versus Treasury spread history, the application performs an inner
join on common observation dates. A Mexican holiday, US holiday, missing FRED
value, or missing Valmer quote therefore does not produce a spread row.

Policy rates are effective levels rather than traded closing yields. The
current policy card may select each series' latest observation at or before the
application valuation date. Any future daily policy-differential history must
use an explicitly documented effective-until-changed rule; it must not inherit
a generic market-yield forward-fill behavior.

## Scheduling

Add two independent recurring jobs to `scheduled_jobs.yaml`:

- FRED reference-rate refresh
- Banxico policy-target refresh

The jobs remain independent so one provider outage does not block the other
source. FRED H.15 observations are normally available after the US publication
window, so the FRED job should run around `23:30 UTC` on weekdays rather than
with the existing early Banxico/TIIE jobs. A Monday incremental request can
collect any weekend policy-target observations published by `DFEDTARU`.

Recurring schedules belong in repository `scheduled_jobs.yaml`; historical
backfills remain explicit one-off executions:

```text
https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/scheduling_jobs/
```

## Secret Handling

Use these runtime secrets:

| Source | Secret |
| --- | --- |
| Banxico SIE | `BANXICO_TOKEN` |
| FRED | `FRED_API_KEY` |

Rules:

- resolve environment values first and project-readable Main Sequence Secrets
  second, matching the existing Banxico pattern
- never store a secret in DataNode configuration, hashes, logs, metadata, or
  source provenance
- fail explicitly when the required secret is missing or empty
- never log an authenticated request URL containing a secret

## Application Analytics Boundary

The connector must not publish a Mexico-US spread DataNode in the first
implementation. The debt-analysis API owns:

- mapping 2Y, 5Y, 10Y, and 30Y selections to Treasury index identifiers
- selecting the nearest quoted M/BONOS maturity at the valuation date
- returning the selected M-Bond identifier and maturity gap
- keeping that selected bond explicit throughout a requested history response
- joining Valmer and FRED histories on common dates
- calculating `spread_bp = (m_bond_yield - treasury_yield) * 10_000`
- calculating the current policy differential from the latest eligible policy
  observations
- calculating current, mean, minimum, maximum, standard deviation, z-score,
  entry, target, and stop values

If this spread later becomes a reusable published market index, it must use the
ms-markets derived-index contract with a versioned definition and resolved-leg
provenance. That later decision must define whether the M-Bond leg is fixed for
a history window or rolls through benchmark bonds over time.

## Alternatives Rejected

### Put All Rates In IndexFixingsStorage

Rejected because analytical Treasury yields and policy targets are not pricing
fixings. Shared numeric units do not imply shared business semantics.

### Store Treasury Series As Assets

Rejected because constant-maturity Treasury series are indexes, not tradable
Treasury securities. Tradable bonds remain `Asset` rows; reference observables
remain `Index` rows.

### Publish The Final Spread In The Source Connector Immediately

Rejected because the spread depends on an application benchmark-selection and
roll policy that is not yet an immutable methodology. Publishing it now would
hide the selected M-Bond leg and create ambiguous history.

### Fetch Full Source History On First Run

Rejected because it creates unnecessary load and makes unit, date, and series
mapping failures harder to diagnose. The accepted rollout starts with 90 days
and expands through an explicit five-year backfill.

### Forward-Fill Market Yields To Align Calendars

Rejected because it manufactures observations on dates where one market did
not publish. Historical spread statistics use common dates only.

## Implementation Tasks

- [x] Add `fred*` to package discovery and first-party import configuration.
- [x] Add canonical index definitions for the six accepted series.
- [x] Implement `ReferenceRateObservationsStorage`.
- [x] Add the storage model to the project migration registry.
- [x] Add `IndexTable` to migration reference metadata without adding it to the
  Valmer-owned migration model list.
- [x] Generate the SDK-managed MetaTable migration and validate it against a
  local database at revision `0002`.
- [x] Apply the project migration through the authenticated platform provider.
- [x] Attach `ReferenceRateObservationsStorage` through the normal Valmer
  runtime bootstrap before DataNode construction.
- [x] Implement `ReferenceRateObservationConfiguration` with validated hashed
  index scope, fixed bootstrap lookback, and optional bounded backfill dates.
- [x] Implement the FRED client with runtime `FRED_API_KEY` resolution.
- [x] Implement FRED metadata validation and observation parsing.
- [x] Implement `FredReferenceRatesNode` for the four Treasury series and Fed
  target upper limit.
- [x] Implement the Banxico policy definition and metadata validation for
  `SF61745`.
- [x] Implement `BanxicoPolicyRatesNode` by reusing the existing SIE client and
  token resolver.
- [x] Add source-specific scripts or CLI commands for both producers.
- [x] Add independent recurring jobs to `scheduled_jobs.yaml`.
- [x] Add a 90-day smoke execution path that requires an explicit
  `hash_namespace` for the first shared-backend validation.
- [x] Run the namespaced FRED smoke update and verify persisted observations for
  all five configured FRED identities.
- [ ] Run the namespaced Banxico smoke update and verify persisted observations
  for the configured policy-rate identity.
- [x] Add explicit five-year backfill commands and operational documentation.
- [x] Add unit, storage-contract, migration, and integration tests.
- [x] Update the root `README.md` documentation map and remove stale scope text
  that conflicts with the repository's existing Banxico producer.
- [ ] Verify DataNode identity, storage rows, jobs, runs, and logs on the Main
  Sequence platform.

## Validation Plan

The implementation is not complete until all of these checks pass:

- FRED metadata confirms all five accepted FRED series IDs and expected units.
- Banxico metadata confirms `SF61745` as the policy target.
- Percentage source values are divided by `100.0` exactly once.
- FRED `"."` values and Banxico unavailable markers do not become numeric
  observations.
- All non-empty frames are UTC indexed by `time_index` and
  `index_identifier`.
- Duplicate index keys are rejected.
- The storage class has an intention-rich table description, intention-rich
  column metadata, and the stable `reference_rate_observations` extra hash
  component.
- The authored storage identifier resolves to
  `valmer_connectors.reference_rate_observations`.
- The default empty-storage request covers no more than 90 calendar days.
- Incremental requests start after the latest stored date for each identity.
- Repeated normal launcher construction produces the same `update_hash`; no
  runtime-calculated date is inserted into configuration.
- The bounded five-year backfill has explicit inclusive dates, a distinct
  deterministic `update_hash`, ends before the configured 90-day smoke request
  window, and publishes the missing older range after smoke data already
  exists.
- The project migration provider creates and finalizes
  `valmer_connectors.reference_rate_observations` while treating `IndexTable`
  as reference metadata rather than Valmer-owned DDL.
- Runtime bootstrap attaches the migrated storage before either producer is
  constructed.
- A 90-day live smoke run under an explicit `hash_namespace` produces plausible
  rows for all six series before any non-namespaced production run.
- The first non-namespaced incremental run starts after the namespaced smoke
  rows and does not attempt duplicate keys.
- An explicit five-year backfill supports the application's 1Y, 3Y, and 5Y
  windows.
- Any historical repair uses a scoped `delete_after_date(...)` tail reset and
  does not use raw SQL or an unbounded delete.
- No Treasury observation is forward-filled over a missing source date.
- Scheduled FRED and Banxico jobs run independently and their logs show the
  expected source windows and inserted row counts without secrets.
- The application can reproduce each tenor spread using existing Valmer
  M-Bond yields and common-date Treasury observations.
- The policy card can resolve the latest eligible Banxico target and Fed upper
  target without requiring the dates to be identical.

## Consequences

Positive consequences:

- external market and policy observations gain a reusable, canonical storage
  surface
- pricing fixing semantics remain correct
- source integrations stay independently testable and schedulable
- the dashboard no longer needs to call FRED or Banxico directly
- future analytics can reuse the same observations without duplicating source
  extraction

Costs and follow-up obligations:

- the project owns one additional MetaTable and migration lifecycle
- FRED introduces a new runtime secret and source package
- five-year support requires an intentional backfill after the 90-day smoke
  stage
- the dashboard API must implement and test benchmark selection and common-date
  alignment
- a future published spread index requires a separate methodology decision
  covering benchmark rolls and resolved-leg provenance
