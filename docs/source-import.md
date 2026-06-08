# Source Import

This page documents where Valmer source rows come from. It is separate from
DataNode publication, AssetTable registration, and pricing hydration.

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
        -> _set_artifact_data()
```

## Source Selection

`_set_artifact_data()` has two source paths.

```text
DEBUG_ARTIFACT_PATH set
    -> local filesystem import

DEBUG_ARTIFACT_PATH not set
    -> Main Sequence Artifact bucket import
```

## Platform Artifact Bucket Path

The platform source is selected explicitly with `--bucket-name` or through the
environment:

```text
VALMER_VECTOR_BUCKET_NAME=<artifact bucket name>
```

If neither is provided, the importer keeps the legacy bucket fallback
`Hitorical Valmer Vector Analytico` for backwards compatibility. Do not rely on
that fallback for new operational configurations.

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

When update statistics already exist, artifacts are filtered to source dates
newer than the latest stored update timestamp.

## Local Debug Path

Set:

```bash
DEBUG_ARTIFACT_PATH=/path/to/local/valmer/file-or-folder
```

or pass the CLI option:

```bash
valmer-connectors vector update --debug-artifact-path /path/to/local/valmer/file-or-folder
```

When this environment variable is present, `_set_artifact_data()` reads local
source files and bypasses the platform bucket. If the path is a file, that
single file is read. If the path is a directory, Excel files are read
recursively:

```text
Path(DEBUG_ARTIFACT_PATH).rglob("*.xls*")
```

The local path re-reads the `EMISORA` column with `keep_default_na=False` so
issuer code `NA` is preserved as text instead of being converted to missing
data.

Local rows then pass through the same normalization and unique-identifier
construction as platform artifacts.

## Output Of Source Import

The source import step produces:

- `self.artifact_data`
- `self.source_data`

Those frames are the input to:

- asset/detail/pricing preparation in `prepare_for_update()`
- vector DataNode publication in `update()`

## What Source Import Does Not Do

Source import does not:

- register AssetTable rows
- upsert ValmerAssetDetailsTable
- build bond pricing instruments
- publish DataNode rows
- publish curves

Those happen in later explicit steps.
