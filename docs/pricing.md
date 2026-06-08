# Pricing

This page documents pricing hydration and curve/index runtime behavior. It is
separate from source import, DataNode publication, and static asset details.

## Runtime Bootstrap

The single project bootstrap entry point is:

```text
valmer_connectors.instruments.bootstrap.bootstrap_runtime()
```

It attaches already-migrated runtime tables and seeds static pricing reference
rows. It does not create schemas at runtime.

```text
bootstrap_runtime()
    |
    v
msm.start_engine(...)
    |
    +-- AssetType
    +-- Asset
    +-- IndexType
    +-- Index
    +-- ValmerAssetDetailsTable
    +-- ValmerVectorPricesStorage
    |
    v
msm_pricing.bootstrap.create_pricing_schemas(...)
    |
    v
seed static pricing rows
```

## Static Pricing Rows

`src/valmer_connectors/instruments/curve_bootstrap.py` seeds the Mexican
reference-rate and Valmer curve rows.

Reference indexes:

- `TIIE_OVERNIGHT`
- `TIIE_28`
- `TIIE_91`
- `TIIE_182`
- `CETE_28`
- `CETE_91`
- `CETE_182`

Pricing convention details:

- stored in `IndexConventionDetails`
- keyed by `index_uid`
- source: `mexico`

Curve identity:

- `Curve.unique_identifier = "VALMER_TIIE_28"`
- `curve_type = "discount"`
- `index_unique_identifier = "TIIE_28"`
- `source = "valmer"`

Relationship:

```text
+-----------------------------+
| IndexTable                  |
|-----------------------------|
| uid                         |
| unique_identifier           |
| index_type = interest_rate  |
+-----------------------------+
              |
              | index_uid
              v
+-----------------------------+
| IndexConventionDetails      |
+-----------------------------+
              |
              | index_uid
              v
+-----------------------------+
| Curve                       |
|-----------------------------|
| unique_identifier           |
| VALMER_TIIE_28              |
+-----------------------------+
```

## Bond Pricing Hydration

Bond pricing hydration runs during `ImportValmer.prepare_for_update()`, before
the DataNode run.

```text
prepare_for_update()
    |
    v
_prepare_latest_inputs(...)
    |
    +-- latest Valmer row per unique_identifier
    +-- target-bond subset
    |
    v
_sync_asset_registry_and_pricing(...)
    |
    +-- resolve/upsert AssetTable rows
    +-- upsert ValmerAssetDetailsTable rows
    +-- decide which target bonds need pricing refresh
    +-- build_qll_bond_from_row(...)
    +-- persist_current_pricing_details(...)
```

Pricing hydration writes through:

```text
msm_pricing.api.instruments.persist_current_pricing_details(...)
```

It links the current instrument/pricing payload to the canonical `AssetTable`
row through the asset object.

## Target Bond Selection

Not every Valmer vector row receives pricing details.

Target bond selection is implemented in:

- `ImportValmer._get_target_bonds(...)`

The source vector table is broader than the supported pricing surface. A row
can be published as source data without being hydrated into an instrument.

## Instrument Construction

The instrument adapter is documented in `instruments.md`.

Pricing calls:

```text
build_qll_bond_from_row(...)
```

That adapter converts one normalized Valmer source row into an `msm_pricing`
instrument.

## Curve Publication

The active curve publication path is:

```text
valmer-connectors curves update-tiie-zero
    |
    v
bootstrap_runtime()
    |
    v
configure_valmer_discount_curves_cadence()
    |
    v
DiscountCurvesNode(
    CurveConfig(curve_unique_identifier="VALMER_TIIE_28")
)
    |
    v
set_curve_builder(build_tiie_valmer)
    |
    v
run(force_update=True)
```

The curve writes to the canonical `msm_pricing.data_nodes.DiscountCurvesNode`
storage. The old standalone Valmer TIIE curve DataNode is not an active
publication path.

## What Pricing Does Not Own

Pricing does not own:

- Valmer source file loading
- raw vector table schema
- static Valmer asset descriptors
- AssetTable identity shape
- dashboard rendering

Those are separate project boundaries.
