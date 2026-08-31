# MetaTable Source Import Plan

This document plans a third Valmer vector source path backed by one or more
specified Main Sequence MetaTables. It is an implementation plan only. It does
not change runtime behavior yet.

## Goal

Add a source mode where historical Valmer rows can be read from specified
registered MetaTables instead of:

- a local Excel/debug file path
- a Main Sequence Artifact bucket

The MetaTable source must feed the same downstream Valmer workflow:

```text
Valmer source rows
    |
    v
strict source-field mapping
    |
    v
normalized Valmer rows
    |
    +-- asset registration scope selection
    +-- ValmerAssetDetailsTable hydration
    +-- pricing details hydration
    |
    v
ValmerVectorPricesStorage
```

The MetaTable source is only a new source adapter. It must not create a new
updater output table or change `ValmerVectorPricesStorage`.

## Current Sources

`ImportValmer` currently has two source paths:

```text
DEBUG_ARTIFACT_PATH set
    -> local Excel file/folder

DEBUG_ARTIFACT_PATH not set
    -> platform Artifact bucket
```

Both paths produce `self.artifact_data` and then `self.source_data`. Downstream
logic expects normalized Valmer columns plus `unique_identifier`.

The update is asset-indexed. Freshness is determined per asset, not by one
global source window:

```text
ValmerVectorPricesStorage
    -> latest stored time_index per asset_identifier
    -> source reads rows with Fecha newer than that asset's stored value
```

The MetaTable source must participate in the same per-asset cursor model.

## Proposed Source Architecture

Introduce an internal source-provider boundary. The TimeIndexTableUpdater continues to own
the update process, but source acquisition moves behind provider objects.

```text
ImportValmer
    |
    +-- ValmerSourceProvider
            |
            +-- LocalExcelValmerSource
            +-- ArtifactBucketValmerSource
            +-- MetaTableValmerSourceSet
                    |
                    +-- [MetaTableValmerSource, ...]
```

Provider responsibilities:

- read rows newer than the stored cursor for each requested asset
- expose cheap source-date metadata for diagnostics and empty-run checks
- read rows needed for the run
- return a pandas DataFrame with normalized Valmer source names
- never register assets, hydrate pricing details, or publish TimeIndexTableUpdater rows

`ImportValmer` remains responsible for:

- asset-indexed freshness orchestration
- target-bond selection
- asset/detail/pricing orchestration
- updater frame construction
- update-statistics filtering

## MetaTable Source Contract

The MetaTable source reads from one or more specified Main Sequence MetaTables.
Each MetaTable may be platform-managed or external-registered; this adapter
consumes registered MetaTable contracts and does not manage the external
physical database connection directly.

`MetaTableValmerSource` is a single source specification. MetaTable ingestion
uses a list of these specifications and loops through that list explicitly. A
single update can therefore read from several MetaTables, normalize each source
frame, and then combine the frames according to the configured partition mode.

The expected source row grain is:

```text
one row per Valmer instrument per valuation date
```

The underlying physical object may be an external SQL Server table/view, but
that is hidden behind the MetaTable. The production adapter must not hard-code
SQL Server credentials, connection strings, or physical object names. SQL
Server-specific exploration scripts under `data/` are not part of the
production adapter.

Required MetaTable source configuration:

```text
source_kind = "metatable"
source_metatables = [
  {
    source_name = <stable label for diagnostics>
    metatable_identifier = <stable MetaTable identifier>
    column_map = <strict MetaTable-column to normalized-Valmer-field map>
  },
  ...
]
```

Optional MetaTable source configuration:

```text
valuation_date_filter_column = <MetaTable column mapped to fecha>
max_rows_per_read = <safety limit for exploratory/test runs>
partition_mode = "row_union" | "column_join"
join_keys = ["fecha", "tipovalor", "emisora", "serie"]  # only for column_join
```

Configuration fields that select source identity or row scope must participate
in the updater update hash.

## Multi-MetaTable Partition Modes

The source can be split across multiple MetaTables in two different ways. The
implementation must not guess which one applies.

### Row-Partitioned Sources

In the first supported mode, each source MetaTable has enough columns to produce
valid normalized Valmer rows. Different MetaTables contain different instrument
families, date ranges, vendors, or operational partitions. A valid normalized
row means the required identity/date fields are present and all optional target
fields are either mapped from the source or materialized as null columns.

```text
MetaTable A rows -> normalized Valmer rows
MetaTable B rows -> normalized Valmer rows
MetaTable C rows -> normalized Valmer rows
                  -> concat
                  -> de-duplicate by (fecha, unique_identifier)
```

This should be the first implementation target because it preserves the current
source model: a provider returns normalized rows.

Required collision rule:

- if two MetaTables produce the same `(fecha, unique_identifier)`, fail unless
  a deterministic source-priority list is configured.

### Column-Partitioned Sources

If a vector is split by fields across MetaTables, the adapter must join sources
before producing normalized rows.

```text
MetaTable A identity + prices
MetaTable B static descriptors
MetaTable C analytics
          |
          v
join on configured keys
          |
          v
one normalized Valmer row
```

This mode requires explicit `join_keys`, source priorities for overlapping
columns, and validation that the join does not multiply rows. It should not be
silently enabled by providing multiple MetaTables.

## Strict Field Mapping

MetaTable mode must not rely on fuzzy header normalization. It must use an
explicit mapping from registered MetaTable columns to canonical normalized
Valmer source fields.

```text
MetaTable column               -> normalized Valmer source field
----------------------------------------------------------------
<MetaTable date column>        -> fecha
<MetaTable tipo valor column>  -> tipovalor
<MetaTable emisora column>     -> emisora
<MetaTable serie column>       -> serie
...
```

The adapter should validate each source mapping before reading a full dataset:

- every required target field appears exactly once per row-producing source, or
  across the joined source set when `partition_mode = "column_join"`
- no two source columns map to the same target field unless a source-priority
  rule is configured for `column_join`
- every mapped source column exists in its specified MetaTable contract
- mapped columns can be coerced by the existing Valmer transforms
- row identity can be constructed as `tipovalor_emisora_serie`
- `fecha` can be parsed into a UTC nanosecond timestamp

## Required And Optional Target Fields

The MetaTable source contract is layered. Only the row identity and valuation
date are globally required. Vector values, static descriptors, analytics,
ratings, and pricing-detail inputs are optional source capabilities.

Required normalized target fields:

```text
fecha
tipovalor
emisora
serie
```

Required external-schema fields for the known SQL Server vector shape:

```text
Fecha   -> fecha
TV      -> tipovalor
Emisora -> emisora
Serie   -> serie
```

All other fields are optional for source ingestion. Missing optional fields must
not fail source reads. The adapter should materialize missing optional target
columns as nulls before handing rows to the existing Valmer transformation path.
This keeps the updater frame shape stable while preserving the real source
semantics.

Optional time-series Valmer fields currently understood by this project:

```text
preciolimpio
preciosucio
interesesacumulados
cuponactual
sobretasa
montoencirculacion
diastransccpn
cuponesxcobrar
hechodemkt
fechauh
precioteorico
postcompra
postventa
yieldcompra
yieldventa
spreadcompra
spreadventa
mdys
sp
bursatilidad
liquidez
cambiodiario
cambiosemanal
preciomax12m
preciomin12m
suspension
volatilidad
volatilidad2
duracion
duracionmonet
convexidad
var
desviacionstand
valornominalactualizado
calificacionfitch
fechapreciomaximo
fechapreciominimo
sensibilidad
duracionmacaulay
tasaderendimiento
hrratings
```

Optional static Valmer detail fields currently understood by this project:

```text
nombrecompleto
sector
montoemitido
fechaemision
plazoemision
fechavcto
valornominal
monedaemision
subyacente
rendcolocacion
stcolocacion
freccpn
tasacupon
reglacupon
cuponesemision
```

Optional fields from the known SQL Server vector shape that can be mapped when
present:

```text
TipoMercado
PrecioSucio
PrecioLimpio
InteresesDevengados
DiasPorVencer
TasaDescuento
PrecioSucio24Hrs
PrecioLimpio24Hrs
InteresesDevengados24Hrs
DiasPorVencer24Hrs
TasaDescuento24Hrs
Plazo
Sobretasa
ClaveProveedor
TipoEnvio
Duracion
Convexidad
Rendimiento
Instrumento
FechaInicioCupon
FechaFinCupon
TasaCuponVigente
TasaCuponVigente24hrs
Moneda
Isin
```

Some optional source fields do not yet have a canonical Valmer target field in
this project. The first implementation should reject unmapped source columns in
the mapping file, but it should not require every available source column to be
mapped. Adding new target columns to `ValmerVectorPricesStorage` is out of scope
for this plan.

Pricing and static-detail hydration have stricter practical requirements than
time-index table publication. For example, target-bond pricing currently needs enough
descriptor fields to classify supported bonds and build instruments. A MetaTable
source with only `Fecha`, `TV`, `Emisora`, and `Serie` can publish sparse vector
rows, but it cannot hydrate full Valmer asset details or pricing details unless
the needed optional fields are also supplied by the same source or by a future
`column_join` source set.

## Asset-Indexed Freshness For MetaTable Mode

The vector table is keyed by `(time_index, asset_identifier)`. MetaTable mode
must therefore use a per-asset cursor, not a single global date window.

Before reading heavy source rows, the update should resolve the current stored
cursor:

```text
asset_identifier -> latest stored time_index
```

The source identity is built from the required source fields:

```text
asset_identifier = tipovalor_emisora_serie
```

The source read should then return only rows where the mapped `Fecha` is newer
than that asset's stored cursor:

```text
source asset_identifier = TV_Emisora_Serie
source time_index = Fecha normalized to Valmer vector close timestamp

read row when source time_index > stored_latest_by_asset[source asset_identifier]
```

This preserves the normal TimeIndexTableUpdater idea: each asset updates from its own last
stored value. It does not invent a separate `latest` or `range` source mode.

## Incremental Read Strategy

Each `MetaTableValmerSource` should push the per-asset cursor into the source
query whenever the MetaTable API supports it safely.

Preferred read:

```text
for each configured source MetaTable:
  build source asset_identifier from mapped TV, Emisora, Serie
  read rows where source time_index > stored_latest_by_asset[source asset_identifier]
```

If the MetaTable operation cannot express a per-asset cursor efficiently, the
fallback is not to change update semantics. The fallback is an implementation
detail:

```text
read the smallest source slice that can still be filtered by the normal
asset-indexed latest-value machinery
```

For example, a fallback may read rows after the minimum stored cursor across the
requested asset scope and then rely on the TimeIndexTableUpdater/latest-value filter to drop
already-current asset rows. That fallback must be logged as less efficient, and
large-source deployments should implement the preferred per-asset pushdown.

## Query Safety

Do not build arbitrary SQL from user input.

Safe options:

- every source MetaTable resolved to a registered `MetaTable.uid`
- source columns validated against each registered MetaTable contract before
  query execution
- values passed through parameters or governed MetaTable operations
- no hardcoded physical table names in runtime SQL
- compiled SQL operations declare the full source MetaTable scope

Custom SQL should be a later feature and must be explicitly reviewed. The first
MetaTable source should support one or more specified MetaTables plus strict
column maps.

## Output Contract

MetaTable mode must produce one combined normalized DataFrame with the same
shape as the existing source modes. Required fields come from the source
mapping. Optional fields that are absent from a specific MetaTable source are
added as null columns before downstream processing:

```text
normalized Valmer columns
unique_identifier = tipovalor_emisora_serie
```

No new output storage table is needed. The same updater output table contract
continues:

```text
ValmerVectorPricesStorage
    index: (time_index, asset_identifier)
```

No Alembic migration should be needed unless the existing storage contract is
changed. This plan does not propose a storage change.

## Implementation Tasks

- [ ] Define a source-provider interface for Valmer vector rows.
- [ ] Move local Excel source reading behind `LocalExcelValmerSource`.
- [ ] Move Artifact bucket source reading behind `ArtifactBucketValmerSource`.
- [ ] Add `MetaTableValmerSource` for one configured MetaTable.
- [ ] Add `MetaTableValmerSourceSet` to loop through the configured
  `source_metatables[]` list.
- [ ] Add a typed TimeIndexTableUpdater configuration field for source mode.
- [ ] Add typed MetaTable source configuration:
  `source_metatables[]`, per-source `metatable_identifier`, and per-source
  `column_map`.
- [ ] Implement `partition_mode = "row_union"` first.
- [ ] Document `partition_mode = "column_join"` as unsupported until join keys,
  row-multiplication checks, and source-priority rules are implemented.
- [ ] Implement strict mapping validation before row reads.
- [ ] Require only `fecha`, `tipovalor`, `emisora`, and `serie` in
  row-producing sources.
- [ ] Materialize missing optional target fields as null columns before
  downstream Valmer transforms.
- [ ] Implement MetaTable contract inspection for mapped source columns across
  all configured source MetaTables.
- [ ] Resolve latest stored `time_index` per `asset_identifier` from
  `ValmerVectorPricesStorage` before reading heavy source rows.
- [ ] Implement incremental MetaTable reads from each asset's stored cursor.
- [ ] Add a documented fallback for MetaTable APIs that cannot push down the
  per-asset cursor efficiently, without changing asset-indexed semantics.
- [ ] Concatenate row-partitioned source frames.
- [ ] Fail on duplicate `(fecha, unique_identifier)` across source MetaTables
  unless a deterministic priority rule is configured.
- [ ] Reuse existing `add_valmer_unique_identifier(...)`.
- [ ] Reuse existing `_coerce_valmer_series(...)` transforms.
- [ ] Ensure combined MetaTable rows still feed `_prepare_latest_inputs(...)`.
- [ ] Ensure target-bond selection remains unchanged when the optional pricing
  and descriptor fields required by that path are present.
- [ ] Add clear diagnostics when sparse MetaTable rows can publish vector data
  but cannot hydrate asset details or pricing details because optional
  descriptor fields are absent.
- [ ] Add CLI options for MetaTable mode without breaking existing defaults.
- [ ] Add tests for missing required mapped columns.
- [ ] Add tests that missing optional columns are materialized as nulls and do
  not fail ingestion.
- [ ] Add tests for sparse required-only rows.
- [ ] Add tests for duplicate target mappings.
- [ ] Add tests for multiple row-partitioned MetaTables.
- [ ] Add tests for duplicate `(fecha, unique_identifier)` across sources.
- [ ] Add tests for bad date coercion.
- [ ] Add tests that MetaTable mode preserves `EMISORA = "NA"` as text.
- [ ] Add tests that pricing/detail hydration is skipped or diagnosed when the
  optional pricing fields are absent.
- [x] Add tests that each MetaTable source is filtered from the target vector
  cursor per `asset_identifier` before concatenation.
- [ ] Add tests that TimeIndexTableUpdater output still validates against
  `ValmerVectorPricesStorage`.
- [ ] Update `docs/source-import.md` after implementation.
- [ ] Update `docs/time-index-table-updates.md` after implementation.
- [ ] Update CLI docs after implementation.

## CLI Shape

Keep current behavior unchanged:

```bash
valmer-connectors vector update
valmer-connectors vector update --debug-artifact-path /path/to/file.xls
```

Add a MetaTable source mode:

```bash
valmer-connectors vector update \
  --source metatable \
  --source-metatables-config-path configs/valmer_metatable_sources.json
```

The source config file should be explicit and reviewable. It should not be
guessed from MetaTable names.

Example source config file:

```json
{
  "partition_mode": "row_union",
  "sources": [
    {
      "source_name": "government_vector",
      "metatable_identifier": "external.valmer_government_vector",
      "column_map": {
        "fecha_valmer": "fecha",
        "tipo_valor": "tipovalor",
        "emisora": "emisora",
        "serie": "serie",
        "precio_limpio": "preciolimpio",
        "precio_sucio": "preciosucio",
        "intereses_acumulados": "interesesacumulados",
        "moneda": "monedaemision"
      }
    },
    {
      "source_name": "corporate_vector",
      "metatable_identifier": "external.valmer_corporate_vector",
      "column_map": {
        "fecha_valmer": "fecha",
        "tipo_valor": "tipovalor",
        "emisora": "emisora",
        "serie": "serie",
        "precio_limpio": "preciolimpio",
        "precio_sucio": "preciosucio"
      }
    }
  ]
}
```

The real mappings must include the required target set above, either per
row-producing source in `row_union` mode or across the joined source set in a
future `column_join` mode. Optional fields should be mapped only when the source
actually provides them and the downstream workflow needs them.

## Validation Plan

Validation must prove the source mode changed without changing downstream
semantics:

- every source MetaTable contract reports all mapped source columns
- every row-producing source maps `fecha`, `tipovalor`, `emisora`, and `serie`
- missing optional columns become null target columns instead of ingestion
  failures
- the update resolves the latest stored `time_index` per `asset_identifier`
- source reads return only rows newer than each asset's stored cursor when
  per-asset pushdown is available
- normalized combined MetaTable rows match normalized Excel/Artifact rows for the same
  Valmer valuation date
- `unique_identifier` values match `tipovalor_emisora_serie`
- repeated same-day rows are filtered by the asset-indexed latest-value
  contract
- duplicate `(fecha, unique_identifier)` rows across source MetaTables fail
  unless configured priority resolves them deterministically
- target-bond count matches the same source date from Excel/Artifact when data
  overlaps
- `ValmerAssetDetailsTable` rows are upserted only when incoming details are
  strictly newer and the optional descriptor fields needed by that table are
  present
- pricing details are persisted through the batch API, not one-by-one, only
  when the optional pricing fields needed to build instruments are present
- `ValmerVectorPricesStorage` rows pass the updater runtime validation

## Open Questions

- What is the canonical Main Sequence SDK API for reading a specified MetaTable
  through governed operations into a DataFrame in this project runtime?
- Do all source MetaTables expose one date column equivalent to Valmer
  `fecha`, or is the date derived from another field?
- Are the source MetaTables row-partitioned, column-partitioned, or mixed?
- Are the source MetaTables full historical data or only latest snapshot data?
- Can the governed MetaTable read operation express
  `source_time_index > stored_latest_by_asset[source_asset_identifier]`, or is
  a less efficient fallback required?
- Should the source MetaTables be queried only through predefined operations,
  or should reviewed custom compiled SQL operations be allowed later?
- Are the provided MetaTables already normalized to Valmer semantics, or do they
  use business-friendly aliases that require larger mapping files?
- Which optional SQL Server fields should map to existing Valmer target fields,
  and which should remain unmapped until a storage-contract change is approved?
- Should fields such as `Rendimiento`, `TasaCuponVigente`, 24-hour prices, and
  `Isin` be mapped to existing target fields or kept outside the first
  implementation?

## Non-Goals

- Do not change `ValmerVectorPricesStorage`.
- Do not create new MetaTables.
- Do not move Valmer source parsing into `ms-markets` or `msm_pricing`.
- Do not treat all rows in MetaTable sources as supported assets.
- Do not make the SQL Server exploration script under `data/` a production
  source adapter.
- Do not add free-form SQL ingestion in the first implementation.
- Do not silently join column-partitioned MetaTables without explicit keys and
  row-multiplication validation.
