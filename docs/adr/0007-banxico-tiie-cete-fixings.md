# ADR 0007: Publish Banxico TIIE And CETE Fixings For Valuation

## Status

Implemented locally; live Banxico metadata validation pending.

## Date

2026-07-01

## Success Criteria

This ADR defines the implementation target for Banxico-backed reference-rate
fixings. The future implementation must:

- publish TIIE and CETE fixings from Banco de Mexico SIE data
- use the current `msm_pricing` `FixingRatesNode` / `IndexFixingsStorage`
  contract
- emit fixing rows with `time_index`, `index_identifier`, and `rate`
- store `rate` as a decimal value, not as Banxico's percentage-form payload
- validate Banxico SIE series metadata before accepting a production series map
- require a project-readable `BANXICO_TOKEN` secret or equivalent runtime secret
- implement Banxico source code under `src/banxico`, because the data source is
  Banxico SIE rather than Valmer
- schedule fixing refresh before valuation and before curve refresh jobs that
  consume the latest overnight anchor
- test Banxico parsing, percent-to-decimal conversion, incremental update
  windows, and the exact `index_identifier` storage contract

The local implementation adds the Banxico source package, fixing builder,
DataNode runner, CLI command, schedule entry, documentation, and unit tests.
Live Banxico metadata validation remains pending until a readable
`BANXICO_TOKEN` secret is available in this project context.

## Context

The Valmer project is already building pricing curves through the current
`msm_pricing` architecture:

- `VALMER_TIIE_OVERNIGHT` is published through `DiscountCurvesNode` from Valmer
  `IRS_MXN_CURVE.csv` OIS market quotes.
- `VALMER_MXN_GOVERNMENT_BOND` is published through `DiscountCurvesNode` from
  Vector Analitico CETES and M Bonos rows.
- TIIE and CETE reference-rate indexes are registered as `Index` rows and used
  as selectors in market-data-set curve bindings.
- The default market-data set is already configured to point the
  `PRICING_CONCEPT_INTEREST_RATE_INDEX_FIXINGS` concept at
  `IndexFixingsStorage`.

The missing piece is the producer that writes actual fixing observations into
`IndexFixingsStorage`. Without those rows, floating-rate valuation can have a
registered index and a curve but still fail when historical fixings are needed.
The Main Sequence pricing runtime documentation explicitly says floating-rate
pricing needs past fixings and that missing fixings are one of the first checks
when floaters fail:

```text
https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/instruments/pricing_runtime/
```

The TIIE OIS curve builder also has an optional overnight front-end anchor. The
first Valmer domestic OIS market quote is a 28-day OIS quote, not a one-day
fixing. A Banxico-backed `TIIE_OVERNIGHT` fixing source is therefore the right
source for the curve's optional overnight deposit/helper anchor when that anchor
is wired in.

## Verified Banxico Facts

The Banco de Mexico SIE REST API documentation was checked on 2026-07-01:

```text
https://www.banxico.org.mx/SieAPIRest/service/v1/
```

Verified from the public Banxico documentation:

- SIE API is the REST replacement for the previous SOAP `DgieWS` service.
- API calls require a query token.
- The token is a 64-character value sent through the `Bmx-Token` header or the
  `token` query parameter.
- The API supports metadata, latest observation, historical observations, and
  date-range observations for up to 20 series in one request.
- Date-range requests use:

```text
GET /SieAPIRest/service/v1/series/:idSerie/datos/:fechaIni/:fechaFin
```

- Date-range request dates are sent as `yyyy-MM-dd`.
- Banxico response observations use Spanish date strings such as `31/01/2023`
  and numeric string values such as `10.8162`.
- The official documentation example identifies `SF43783` as TIIE 28 days and
  shows the title `TIIE a 28 dias Tasa de interes en por ciento anual`.
- The public documentation also lists service-compatibility examples:
  `SF60648` for TIIE 28 days, `SF60649` for TIIE 91 days, and `SF60633` for
  CETES 28 days.

Live metadata verification for all target series was attempted from this
checkout after refreshing the project token. The current project does not have
a readable Main Sequence Secret named `BANXICO_TOKEN`, and `.env` does not
contain `BANXICO_TOKEN`, so token-backed metadata validation could not be
completed from this project.

## Series Validation Targets

The production implementation must treat Banxico SIE metadata as the authority
for series identity. The following target index coverage is required:

| Pricing Index Identifier | Required Banxico Evidence |
| --- | --- |
| `TIIE_OVERNIGHT` | SIE metadata must confirm the overnight TIIE/funding-rate title, unit, and publication frequency. |
| `TIIE_28` | SIE metadata must confirm TIIE 28 days. Banxico public docs validate `SF43783` as one TIIE 28-day series example. |
| `TIIE_91` | SIE metadata must confirm TIIE 91 days. Banxico public docs also list `SF60649` as a TIIE 91-day service example. |
| `TIIE_182` | SIE metadata must confirm TIIE 182 days. |
| `CETE_28` | SIE metadata must confirm CETES 28 days. Banxico public docs also list `SF60633` as a CETES 28-day service example. |
| `CETE_91` | SIE metadata must confirm CETES 91 days. |
| `CETE_182` | SIE metadata must confirm CETES 182 days. |

Before publication, each accepted SIE series id must be verified by a
token-backed metadata request that records the `idSerie`, title,
unit/description when available, and the pricing index identifier it feeds.

## Decision

Add a Banxico-owned fixing producer in a later implementation. The
target should be:

```text
Banxico-backed TIIE/CETE fixing DataNode producing current IndexFixingsStorage
rows, validated against Banxico SIE, scheduled ahead of valuation/curve refresh
jobs.
```

## Source Ownership Boundary

The fixing source is Banco de Mexico SIE, not Valmer. The implementation must
therefore place source-specific client, parser, series-map validation, and
fixing builder code under a Banxico package:

```text
src/banxico/
```

Valmer-specific modules may depend on the resulting `IndexFixingsStorage`
observations and may schedule Banxico refreshes before Valmer curve or
valuation jobs, but they must not own the Banxico SIE extraction logic.

If the repository packaging still discovers only `valmer_connectors*`, update
the packaging configuration so the `banxico*` package under `src/banxico` is
installed and available to jobs/tests.

The producer must use the current `msm_pricing` fixing path:

- `msm_pricing.data_nodes.IndexFixingConfiguration`
- `msm_pricing.data_nodes.FixingRatesNode`
- `msm_pricing.data_nodes.index_fixings.storage.IndexFixingsStorage`

The output frame must use the current installed `msm_pricing` contract:

| Column | Meaning |
| --- | --- |
| `time_index` | UTC observation timestamp for the Banxico fixing date |
| `index_identifier` | `Index.unique_identifier`, for example `TIIE_28` |
| `rate` | decimal rate, for example `0.108162` for a 10.8162 percent source value |

The future implementation must not emit the stale `unique_identifier` fixing
column. The installed `msm_pricing` 4.4.25 source rejects stale fixing builder
columns named `unique_identifier` or `index_uid` for index fixings.

## Documentation Discrepancy

The latest public Main Sequence "Market Data and Registration" page still
describes reference-rate fixing rows with `unique_identifier`:

```text
https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/instruments/market_data_and_registration/
```

The installed `msm_pricing` 4.4.25 code in this workspace requires
`index_identifier`. This ADR follows the installed package contract because
that is what the local runtime and tests will execute. The discrepancy should
be reported or corrected upstream before relying on the public docs for this
specific column name.

## Source Access And Secret Handling

The implementation must use a project-readable secret for Banxico API access.
Use `BANXICO_TOKEN` as the expected Main Sequence Secret name unless platform
operations chooses a different organization-wide naming policy.

Rules:

- do not hardcode the token
- do not put the token in DataNode hashed configuration
- do not log the token
- fail explicitly when the secret is missing or unreadable
- use the `Bmx-Token` header by default
- keep the Banxico base URL and timeout as implementation constants or
  operational knobs, not as dataset identity

## Update Window

The Banxico fixing DataNode should be incremental by default, following the
Main Sequence DataNode guidance:

```text
https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/data_nodes/
```

Target behavior:

- first run starts from an explicit configured historical backfill date
- subsequent runs start from the last persisted fixing date plus one day
- normal daily runs end at yesterday's UTC date, because same-day Banxico data
  may not yet be published or final
- backfills must be explicit, not accidental full-history fetches in every run
- date parsing must normalize Banxico `DD/MM/YYYY` observations to
  `datetime64[ns, UTC]`

## Scheduling

The fixing refresh must run before any valuation workflow that prices TIIE
floaters or swaps and before any curve-refresh workflow that consumes the latest
`TIIE_OVERNIGHT` fixing as an overnight front-end anchor.

Target order:

```text
Banxico TIIE/CETE fixing refresh
    -> Valmer curve refreshes
    -> valuation / runtime validation jobs
```

The current repository has curve update services but no fixing update service,
CLI command, or scheduled fixing job. That is the operational gap this ADR
accepts for later implementation.

## Implementation Tasks

- [x] Add a `src/banxico/` package for Banxico-owned source code.
- [x] Update package discovery so `banxico*` is installed from `src/`.
- [x] Implement a Banxico SIE client under `src/banxico/` for metadata and
  date-range requests.
- [x] Resolve the SIE API token from Main Sequence Secret `BANXICO_TOKEN` at
  runtime and fail explicitly when it is missing or unreadable.
- [ ] Verify and record the accepted SIE series id for each target pricing
  index before enabling publication.
- [x] Implement the TIIE/CETE fixing builder under `src/banxico/` using
  Banxico `fecha` and `dato` payloads.
- [x] Convert Banxico percentage-form rates to decimal rates.
- [x] Publish rows through the current `FixingRatesNode` /
  `IndexFixingsStorage` contract with `time_index`, `index_identifier`, and
  `rate`.
- [x] Add a project CLI or service entry point that delegates to the
  `src/banxico/` package.
- [x] Add a scheduled fixing refresh that runs before Valmer curve refreshes
  and valuation jobs.
- [x] Add unit tests for metadata validation, date parsing, numeric parsing,
  percent-to-decimal conversion, empty responses, and stale-column rejection.
- [x] Add an integration validation path under an explicit `hash_namespace`
  before any shared-backend run.
- [x] Document the Banxico secret requirement and operational verification
  commands in the project docs.

## Validation Plan

The implementation is not complete until these checks pass:

- Banxico metadata is fetched with a valid token for every target series.
- Each accepted series ID has the expected Banxico title and unit.
- Banxico date-range payloads parse `fecha` values as `DD/MM/YYYY`.
- Banxico numeric strings parse to numeric source values.
- Source percentage values are divided by `100.0`.
- Empty or missing `datos` responses return an empty frame without masking API
  errors.
- Builder frames contain `time_index`, `index_identifier`, and `rate`.
- Builder frames do not contain `unique_identifier` or `index_uid`.
- Non-empty frames validate against `IndexFixingsStorage`.
- Unit tests cover TIIE and CETE mapping, parsing, decimal conversion, and
  current storage contract.
- Integration validation runs the fixing node under an explicit
  `hash_namespace` before any shared-backend production run.
- Runtime validation prices or at least loads one TIIE floating instrument or
  swap with `hydrate_fixings=True`.
- Curve validation confirms whether `VALMER_TIIE_OVERNIGHT` used the latest
  Banxico `TIIE_OVERNIGHT` fixing as its front-end anchor or explicitly reports
  that it was published OIS-only.

## Consequences

Positive consequences:

- TIIE floating-rate valuation can rely on real historical fixings instead of
  only a forward/projection curve.
- The TIIE OIS curve can get a real overnight front-end anchor from the same
  reference-rate source used by valuation.
- CETE reference-rate history becomes available for benchmark, reporting, and
  any valuation path that resolves CETE fixings.
- The project keeps curve observations and fixing observations in their
  canonical `msm_pricing` storage tables.

Tradeoffs:

- Banxico SIE token availability becomes an operational prerequisite.
- CETE series identity must be validated carefully before publication because
  more than one Banxico series family may look plausible from public
  documentation.

## Non-Goals

This ADR does not:

- add platform secrets
- claim live Banxico metadata validation for all target series
- synthesize fixings from Valmer OIS or government curves
- change existing curve construction code
