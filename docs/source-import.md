# Source Import

This page documents where Valmer source rows come from. It is separate from
time-index table publication, AssetTable registration, and pricing hydration.

## Constructor Versus Import

`build_import_valmer()` in
`src/valmer_connectors/services/vector_update.py` only constructs the updater:

```text
ImportValmer(
    config=ImportValmerConfig(bucket_name=BUCKET_NAME_HISTORICAL_VECTORS)
)
```

It does not read files.

Actual source import happens later:

```text
prepare_for_update()
    -> prepare_source_data()
        -> _set_artifact_data() or _set_metatable_source_data()
```

After source rows are loaded and normalized, `prepare_source_data()` filters
rows against the target vector storage cursor per asset:

```text
asset_identifier = tipovalor_emisora_serie
source time_index = Fecha + 1 day - 1 second

keep row when source time_index > latest stored vector time_index for asset_identifier
keep row when asset_identifier has no stored vector observation
```

This is the same asset-indexed update rule regardless of source type. There is
no global latest-date source skip.

Repair runs can bypass this row-level cursor so the normal updater path still
revisits rows whose vector observations already exist:

```bash
valmer-connectors vector update --bypass-vector-cursor-filter
```

The equivalent environment control is:

```text
VALMER_VECTOR_BYPASS_CURSOR_FILTER=1
```

Use this only for repair workflows such as patching pricing details or
rebuilding static asset metadata. It keeps source rows available for
pre-update hydration; vector publication still writes through the normal
updater path.

## Source Selection

`prepare_source_data()` has three source paths. The CLI exposes the platform
bucket, OneDrive Graph, local-folder-as-bucket, debug artifact, and MetaTable
source choices.

```text
artifact source
    -> Main Sequence Artifact bucket import

onedrive-graph source
    -> Microsoft Graph download into local cache, then artifact reader

local bucket path
    -> local folder import using the artifact reader

source_kind = "metatable"
    -> configured MetaTable source list

DEBUG_ARTIFACT_PATH set
    -> low-level local file/folder override
```

## Hydration Path Examples

The examples in this section intentionally use fictional names. Replace bucket
names, secret names, folder paths, MetaTable identifiers, and UIDs with the
values owned by the consuming project or deployment.

Every source path hydrates the same normalized Valmer row shape before the
downstream workflow continues:

```text
source-specific reader
    |
    v
normalized Valmer rows
    |
    v
per-asset vector cursor filter
    |
    v
AssetTable registration for supported rows
    |
    v
ValmerAssetDetailsTable hydration
    |
    v
AssetSnapshot publication
    |
    v
optional pricing-details hydration for supported pricing targets
    |
    v
Valmer vector time-index table publication
```

Supported source hydration paths:

| Path | Use When | Example |
| --- | --- | --- |
| Platform Artifact bucket | Files are already uploaded to Main Sequence Artifacts. | `valmer-connectors vector update --bucket-name "example-vector-archive"` |
| Local folder bucket | A developer has a local folder with many vector files. | `valmer-connectors vector update --local-bucket-path /mnt/example/vector-drop` |
| Local folder from env | A local path should stay out of launch configs and commits. | `valmer-connectors vector update --local-bucket-path-env-var EXAMPLE_VECTOR_FOLDER` |
| OneDrive Graph | A Linux/Kubernetes job must read OneDrive without a mounted filesystem. | `valmer-connectors vector update --source onedrive-graph --onedrive-drive-id "<drive-id>" --onedrive-folder-path "shared/vector-files"` |
| MetaTable source list | Vector rows already exist in one or more registered MetaTables. | `valmer-connectors vector update --source metatable --source-metatables-config-path configs/example_sources.json` |
| Debug artifact | A one-off file or folder needs direct local debugging. | `valmer-connectors vector update --debug-artifact-path /tmp/example-vector.xls` |

## Platform Artifact Bucket Path

The platform source is selected explicitly with `--bucket-name` or through the
environment:

```text
VALMER_VECTOR_BUCKET_NAME=<artifact bucket name>
```

If neither is provided, the importer keeps a legacy bucket fallback for
backwards compatibility. Do not rely on that fallback for new operational
configurations.

Example:

```bash
valmer-connectors vector update \
  --bucket-name "example-vector-archive"
```

The platform path calls:

```text
Artifact.filter(bucket__name=bucket_name)
```

Supported artifact file types:

- `.xls`
- `.csv`

For each artifact, the importer:

1. reads the artifact content
2. normalizes all column headers with `normalize_column_name(...)`
3. requires `tipovalor`, `emisora`, and `serie`
4. derives `unique_identifier` through `add_valmer_unique_identifier(...)`
5. concatenates all accepted frames into `self.artifact_data`

Artifact rows are concatenated first and then filtered by the per-asset vector
cursor described above.

When `VALMER_VECTOR_BYPASS_CURSOR_FILTER=1` or
`--bypass-vector-cursor-filter` is set, artifact rows skip that row-level
cursor before asset and pricing hydration.

## Local Folder Bucket Path

For local multi-file upload testing, use a folder path as the source:

```bash
valmer-connectors vector update \
  --local-bucket-path /mnt/example/vector-drop
```

For VS Code debug runs, keep the user-specific path in `.env` and pass the
environment variable name:

```bash
EXAMPLE_VECTOR_FOLDER=/mnt/example/vector-drop

valmer-connectors vector update \
  --local-bucket-path-env-var EXAMPLE_VECTOR_FOLDER
```

This path is intentionally separate from `DEBUG_ARTIFACT_PATH`. The local
bucket option is the semantic multi-file folder source. Internally it discovers
Excel files recursively:

```text
Path(local_bucket_path).rglob("*.xls*")
```

Before any Excel file is opened, the service parses the Valmer valuation date
from filenames such as `VectorAnalitico24h_2024-12-03.xls` and compares it
with the latest persisted vector `time_index`. Files whose date is not newer
than storage are not staged or read. Files without a parseable date remain
selected and are still checked by the row-level TimeIndexTableUpdater cursor filter.

For repair runs, `--bypass-vector-cursor-filter` also bypasses this local
filename-date prefilter so older files can be staged and passed to
`prepare_for_update()`.

The local folder path also preflights each selected batch for cloud-provider
placeholders before reading that batch. If a selected OneDrive/FileProvider file
exists but has not been downloaded/materialized locally, the command opens the
file to request materialization and retries before reading the batch. If
OneDrive still does not provide the file, the command fails with the exact file
paths. The importer does not silently skip unreadable local files.

Selected source rows then follow the same normalization, unique-identifier
construction, and per-asset vector cursor filtering as platform bucket rows.

## OneDrive Graph Source

For Linux/Kubernetes jobs, use Microsoft Graph instead of a mounted OneDrive
folder:

```bash
valmer-connectors vector update --source onedrive-graph
```

The source adapter:

1. reads Microsoft Graph credentials from Main Sequence `Secret`s
2. queries the registered Valmer vector storage table for `MAX(time_index)`
3. lists files from the configured OneDrive drive/folder
4. selects only `VectorAnalitico24h_*.xls` files newer than storage
5. sorts selected files by their valuation date, oldest first
6. downloads selected files into a local cache directory

The oldest-first ordering is part of the restart contract, not a presentation
detail. Each successfully persisted batch can advance `MAX(time_index)`. If a
later file were processed before an older selected file, a restart could then
exclude that older file even though it had never been published. Files without
a parseable valuation date sort after dated files and retain filename ordering.
7. runs the existing local-file Valmer parser on the cached files

With `--bypass-vector-cursor-filter`, the OneDrive adapter does not send the
latest vector time index to file selection, so historical files can be
downloaded for a repair pass.

Credential values are resolved by key. The default keys are:

```text
VALMER_ONEDRIVE_TENANT_ID
VALMER_ONEDRIVE_CLIENT_ID
VALMER_ONEDRIVE_CLIENT_SECRET
```

For each key, the importer first checks an environment variable with that exact
name. If it is not present, it reads a Main Sequence `Secret` with that name. If
neither exists, the update fails before calling Microsoft Graph.

Production jobs should normally use Main Sequence `Secret`s:

```bash
mainsequence secrets create VALMER_ONEDRIVE_TENANT_ID "<azure-tenant-id>"
mainsequence secrets create VALMER_ONEDRIVE_CLIENT_ID "<azure-app-client-id>"
mainsequence secrets create VALMER_ONEDRIVE_CLIENT_SECRET "<azure-app-client-secret>"
```

Local runs may provide the same keys through the process environment. Do not
commit those values:

```bash
export VALMER_ONEDRIVE_TENANT_ID="<azure-tenant-id>"
export VALMER_ONEDRIVE_CLIENT_ID="<azure-app-client-id>"
export VALMER_ONEDRIVE_CLIENT_SECRET="<azure-app-client-secret>"
```

The drive id is not sensitive and can be supplied by CLI, environment, or a
Main Sequence `Constant`. The default key is:

```text
VALMER_ONEDRIVE_DRIVE_ID
```

```bash
mainsequence constants create VALMER_ONEDRIVE_DRIVE_ID "<microsoft-graph-drive-id>"
```

Override non-secret routing when needed:

```bash
valmer-connectors vector update \
  --source onedrive-graph \
  --onedrive-drive-id "<drive-id>" \
  --onedrive-folder-path "shared/vector-files" \
  --onedrive-cache-path /tmp/example-vector-cache
```

Secret names are passed by name, never by value:

```bash
valmer-connectors vector update \
  --source onedrive-graph \
  --onedrive-tenant-id-secret-name EXAMPLE_GRAPH_TENANT_ID \
  --onedrive-client-id-secret-name EXAMPLE_GRAPH_CLIENT_ID \
  --onedrive-client-secret-secret-name EXAMPLE_GRAPH_CLIENT_SECRET \
  --onedrive-drive-id "<drive-id>" \
  --onedrive-folder-path "shared/vector-files"
```

When custom keys are passed, the same order applies for each key: environment
variable first, then Main Sequence `Secret`.

## MetaTable Source Path

Run:

```bash
valmer-connectors vector update \
  --source metatable \
  --source-metatables-config-path configs/example_metatable_sources.json
```

The config file contains one or more `MetaTableValmerSource` entries:

```json
{
  "sources": [
    {
      "source_name": "example_government_slice",
      "metatable_identifier": "example.vendor_government_vector",
      "column_map": {
        "Fecha": "fecha",
        "TV": "tipovalor",
        "Emisora": "emisora",
        "Serie": "serie",
        "PrecioSucio": "preciosucio",
        "PrecioLimpio": "preciolimpio"
      }
    }
  ]
}
```

Every source must have a stable logical MetaTable identifier. Register or import
the external table and assign that identifier before configuring the Job. The
Job resolves the identifier through Main Sequence and never accepts a physical
database connection or a MetaTable UID as a source reference.

Multiple sources are allowed. Each source is read independently and then
normalized into the same Valmer semantic column names:

```json
{
  "sources": [
    {
      "source_name": "example_government_slice",
      "metatable_identifier": "example.vendor_government_vector",
      "column_map": {
        "Fecha": "fecha",
        "TV": "tipovalor",
        "Emisora": "emisora",
        "Serie": "serie",
        "PrecioSucio": "preciosucio",
        "PrecioLimpio": "preciolimpio"
      }
    },
    {
      "source_name": "example_bank_slice",
      "metatable_identifier": "example.vendor_bank_vector",
      "column_map": {
        "valuationDate": "fecha",
        "securityType": "tipovalor",
        "issuerCode": "emisora",
        "seriesCode": "serie",
        "dirtyPrice": "preciosucio",
        "cleanPrice": "preciolimpio"
      }
    }
  ]
}
```

Each source is read and normalized independently, filtered against the vector
cursor per `asset_identifier`, and only then concatenated with the other source
frames. Duplicate `(time_index, asset_identifier)` rows across MetaTable
sources fail unless a source-priority rule is added in a later implementation.

### MetaTable-only database access

The production reader accepts only `metatable_identifier`. Main Sequence owns
the registered DataSource binding and governed query execution; the Job and API
do not open database connections or load database access values.

When a vector cursor exists, the reader pushes the minimum stored per-asset
cursor into the SQL query as a lower `Fecha` bound, then applies the normal
per-asset cursor filter in pandas. Repair runs using
`--bypass-vector-cursor-filter` intentionally omit that SQL bound. An initial
load with no stored cursor reads the full configured table.

The read uses the schema and table stored on the resolved MetaTable, including
cursor pushdown against the configured valuation-date column. Query failures
stop the run instead of bypassing MetaTable governance.

## Low-Level Debug Artifact Path

Set:

```bash
DEBUG_ARTIFACT_PATH=/tmp/example-vector.xls
```

or pass the CLI option:

```bash
valmer-connectors vector update --debug-artifact-path /tmp/example-vector.xls
```

`DEBUG_ARTIFACT_PATH` is the low-level override kept for direct one-off
debugging. If the path is a file, that single file is read. If the path is a
directory, Excel files are read recursively:

```text
Path(DEBUG_ARTIFACT_PATH).rglob("*.xls*")
```

Unlike `--local-bucket-path`, this low-level override is not prefiltered by
filename date in the service layer. It is intended for explicit debugging of a
known file or folder.

The local path re-reads the `EMISORA` column with `keep_default_na=False` so
issuer code `NA` is preserved as text instead of being converted to missing
data.

Local rows then pass through the same normalization and unique-identifier
construction as platform artifacts.

## Pricing Detail Repair Controls

Pricing details are patched through the same `valmer-connectors vector update`
execution path as normal source imports. The repair controls are:

| Control | Effect |
| --- | --- |
| `--force-pricing-details-patch` / `VALMER_FORCE_PRICING_DETAILS_PATCH=1` | Rebuild pricing details for every selected supported target bond, even when a current row already exists. |
| `--bypass-vector-cursor-filter` / `VALMER_VECTOR_BYPASS_CURSOR_FILTER=1` | Keep source rows available before hydration even when vector observations already exist. |

Example:

```bash
VALMER_FORCE_PRICING_DETAILS_PATCH=1 \
VALMER_VECTOR_BYPASS_CURSOR_FILTER=1 \
valmer-connectors vector update
```

The force flag only controls pricing-detail hydration during a source import.
Use it when new source rows should rebuild pricing details while the vector
TimeIndexTableUpdater is already running.

For already-persisted bad pricing-detail payloads, use the targeted repair
script instead of replaying vector files:

```bash
PYTHONPATH=src .venv/bin/python scripts/patch_valmer_asset_pricing_details.py
PYTHONPATH=src .venv/bin/python scripts/patch_valmer_asset_pricing_details.py --apply
```

The script queries `AssetCurrentPricingDetails` and `AssetPricingDetailsTS` for
Valmer rows whose serialized `instrument_dump` contains known stale signatures,
patches those JSON payloads in memory, and bulk upserts the same persisted keys
back to the pricing-detail tables. It does not import or loop through Valmer
vectors. Normal Valmer source imports must continue to persist pricing details
through `msm_pricing.api.add_many_pricing_details(...)`, which owns instrument
serialization through the ms-markets instrument model.

Calendar repair targets persisted instrument calendar JSON only. Invalid
calendar tokens such as `Mexico-BMV` and `Mexico/BMV`, plus the stale class-name
object `{"name": "Mexico"}`, are rewritten to the QuantLib display-name payload
`{"name": "Mexican stock exchange"}` for instrument calendar objects. Scalar
`*_calendar_code` fields remain `Mexico`.

## Output Of Source Import

The source import step produces:

- `self.artifact_data`
- `self.source_data`

Those frames are the input to:

- asset/detail/pricing preparation in `prepare_for_update()`
- vector time-index table publication in `update()`

## What Source Import Does Not Do

Source import does not:

- register AssetTable rows
- upsert ValmerAssetDetailsTable
- build bond pricing instruments
- publish TimeIndexTableUpdater rows
- publish curves

Those happen in later explicit steps.
