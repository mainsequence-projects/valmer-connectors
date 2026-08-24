---
name: valmer-connectors-registering-assets
description: Use this skill when implementing, reviewing, or documenting Valmer asset registration in this project, especially `register_valmer_assets_from_rows`, Valmer row classification, AssetTable writes, ValmerAssetDetailsTable hydration, AssetSnapshot publication, and optional pricing-details persistence.
---

# Valmer Asset Registration

Use this skill for Valmer row-to-asset registration work in `valmer-connectors`.

## Public API

Use the public service API:

```python
from valmer_connectors.assets import register_valmer_assets_from_rows
```

Do not expose or call `_upsert_asset_table_rows(...)` as a public Valmer API.
That helper is a private minimal `AssetTable` writer and does not classify rows,
write Valmer details, publish snapshots, or persist pricing details.

## Required Workflow

The full Valmer registration workflow is:

```text
Valmer source rows
    |
    v
normalize source column names
    |
    v
validate identity: tipovalor, emisora, serie
    |
    v
build unique_identifier = tipovalor_emisora_serie
    |
    v
classify explicit asset_type
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
publish AssetSnapshot rows when requested
    |
    v
build and persist pricing details when requested and supported
```

## Asset Type Rule

Asset type must be explicit before writing `AssetTable`.

Default classification supports this project's Valmer bond families and returns
`msm.constants.ASSET_TYPE_BOND`. If a caller imports a different vector or a
broader Valmer universe, require a caller-owned classifier:

```python
def classify_asset_type(row):
    if (row["tipovalor"], row["emisora"]) in {("M", "BONOS"), ("BI", "CETES")}:
        return ASSET_TYPE_BOND
    return None
```

Unsupported rows should be skipped and reported unless the caller requests
strict unsupported-row failure. Never silently coerce unknown instruments to
`bond`.

## Validation Rules

Validate by stage:

- identity requires `tipovalor`, `emisora`, and `serie`
- details require `fecha` for `details_asof`
- snapshots require `unique_identifier`, `fecha`, and `nombrecompleto`
- pricing uses the instrument adapter as validator because required inputs vary
  by Valmer instrument family

Asset type conflicts are hard failures. If an existing `AssetTable` row has a
different `asset_type` than the classifier output, raise and investigate the
identity collision.

## Batch Machinery

Reuse the existing batch machinery:

- Asset rows: private `_upsert_asset_table_rows(...)`
- Valmer details: `upsert_valmer_asset_details(...)`
- snapshots: `AssetSnapshot.set_snapshots(...).run(...)`
- pricing details: `_persist_valmer_pricing_details_batch(...)`, which calls
  `msm_pricing.api.add_many_pricing_details(...)`

Do not reintroduce per-row writes.

## Boundaries

- Keep `AssetTable` small: only canonical `unique_identifier` and explicit
  `asset_type` belong there.
- Keep static Valmer vendor descriptors in `ValmerAssetDetailsTable`.
- Keep time-varying vector values in the Valmer vector storage DataNode.
- Keep pricing payload construction in the pricing adapter path.
- Keep extension-specific classifiers in the extension service layer.

## Validation Commands

After changing this workflow, run at minimum:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_valmer_asset_registration
PYTHONPATH=src .venv/bin/python -m unittest tests.test_valmer_asset_identity tests.test_valmer_vector_storage
```
