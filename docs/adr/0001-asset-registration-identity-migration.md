# ADR 0001: Migrate Valmer Asset Registration Identity

## Status

Proposed

## Date

2026-05-27

## Success Criteria

This ADR is successful when it gives the next implementation agent enough
precision to migrate asset identity without guessing.

The implementation that follows this ADR must:

- preserve the existing Valmer asset key
- replace legacy asset registration calls with the new `ms-markets` asset
  identity API
- keep asset registration separate from pricing hydration
- leave pricing hydration as its own explicitly scoped migration unless it is
  implemented in the same follow-up change
- include tests and live validation that prove the new asset identity rows exist
  and are reusable by the Valmer DataNode, dashboard, and pricing workflow

## Context

The project currently mixes three responsibilities inside the Valmer DataNode:

- deriving the Valmer asset identity from source rows
- registering or resolving platform assets
- hydrating fixed-income pricing details for target bonds

Those responsibilities happen in and around `ImportValmer` in
`src/data_nodes/nodes.py`. That makes the upcoming `mainsequence` and
`ms-markets` migration risky because asset identity and pricing details are not
the same platform object in the new model.

ADR 0000 defines the separate pricing-library boundary and proposed migration
for generic bond, curve, convention, and pricing-definition machinery. This ADR
only covers asset identity and registration. Pricing hydration should not be
treated as complete until the ADR 0000 core-pricing service exists or an
explicit transition path is implemented.

The current project is also still written against the legacy
`mainsequence.client.Asset` surface. In the locally installed
`mainsequence==4.0.11`, `mainsequence.client.Asset` is not available. The
installed `ms-markets==0.0.7` package exposes a typed markets asset identity
surface through `msm.api.assets.Asset`.

The relevant domain rule from the Main Sequence assets documentation still
applies: `unique_identifier` is the stable key that connects asset-based
DataNodes, pricing details, dashboards, portfolios, and downstream analytics.
The instruments documentation separately states that price history does not make
an asset priceable; pricing details store the instrument terms needed for
valuation.

References:

- Main Sequence assets documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/markets/assets/>
- Main Sequence instruments asset/pricing details documentation:
  <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/instruments/assets_and_pricing_details/>
- `ms-markets==0.0.7` local typed asset API:
  `.venv/lib/python3.11/site-packages/msm/api/assets.py`
- `ms-markets==0.0.7` local asset table model:
  `.venv/lib/python3.11/site-packages/msm/models/assets/core.py`
- `ms-markets==0.0.7` local asset-indexed DataNode helpers:
  `.venv/lib/python3.11/site-packages/msm/asset_indexed_data_node.py`

## Current Asset Registration Audit

### Asset Identity Derivation

Current location:

- `src/data_nodes/nodes.py:1075-1087`

Current behavior:

```python
df["unique_identifier"] = (
    df["tipovalor"]
    .astype("string")
    .str.cat(df["emisora"].astype("string"), sep="_")
    .str.cat(df["serie"].astype("string"), sep="_")
)
```

This is not registration. It is identity derivation from vendor source fields.

The migration must move this rule into a project-owned identity helper so every
asset entry point uses the same construction and validation logic.

### Asset Registration And Lookup

Current primary registration location:

- `src/data_nodes/nodes.py:1124-1224`

Inside that method, registration-specific behavior is currently:

- lookup existing assets:
  `src/data_nodes/nodes.py:1135-1140`
- compute missing assets:
  `src/data_nodes/nodes.py:1142-1144`
- build legacy registration payloads:
  `src/data_nodes/nodes.py:1151-1160`
- call legacy batch registration:
  `src/data_nodes/nodes.py:1165-1168`
- return registered plus existing assets:
  `src/data_nodes/nodes.py:1224`

Current legacy API calls:

```python
existing_assets_list = msc.Asset.query(
    unique_identifier__in=unique_identifiers,
    per_page=per_page_assets,
)
```

```python
assets = msc.Asset.batch_get_or_register_custom_assets(assets_payload)
```

This is the actual asset registration path that must be migrated.

### Pricing-Hydration Decision Logic

Current location:

- `src/data_nodes/nodes.py:950-1025`

Current behavior:

- `_get_uids_to_update(...)` returns two lists:
  - `missing_assets`
  - `pricing_updates`
- missing assets are identified from the full `unique_identifiers` input
- pricing updates are only considered for assets in `all_target_bonds`
- existing assets are marked for pricing refresh when:
  - `force_update=True`
  - `current_pricing_detail` is missing
  - `current_pricing_detail.instrument_dump` is missing
  - latest source face value differs from stored instrument face value

This function currently mixes asset-registration planning and pricing-hydration
planning. The migration should split it into two concepts:

- identity registration plan
- pricing hydration plan

### Pricing Hydration Execution

Current location:

- `src/data_nodes/nodes.py:1173-1222`

Current behavior:

1. Build `uids_needing_pricing` from pricing updates and newly registered target
   assets.
2. For each target UID, use the latest Valmer row to build a pricing instrument:
   - `get_instrument_conventions(row)`
   - `build_qll_bond_from_row(...)`
3. Attach the built instrument to the asset with:

```python
asset.add_instrument_pricing_details_from_ms_instrument(
    **instrument_pricing_detail_map[uid]
)
```

This is not asset registration. It is pricing-detail hydration. The asset
identity migration must not silently claim this path is migrated just because
asset lookup/registration is migrated.

### Other Asset Lookup Touchpoints

`src/data_nodes/nodes.py:719-722`

- `MexDerTIIE28Zero.get_asset_list()` calls
  `msc.Asset.get(unique_identifier="TIIE_28")`
- this is an asset lookup for a reference-rate or curve-related asset, not
  Valmer bond registration
- it must be reviewed separately because it may belong to pricing-runtime
  constant resolution rather than Valmer custom-asset registration

`src/instruments/vector_to_asset.py:1030-1033`

- `build_position_from_sheet(...)` calls
  `msc.Asset.filter(unique_identifier__in=df_out["UID"].to_list())`
- this maps local pricing-check output back to platform asset IDs
- this is lookup-only, but it depends on the same stable Valmer identity

`dashboards/valmer_monitor/valmer_dashboard.py:303-335`

- `_query_assets(...)` calls
  `msc.Asset.query(unique_identifier__in=batch, per_page=batch_size)`
- `load_pricing_health(...)` reads `asset.current_pricing_detail`
- this dashboard path uses both asset lookup and pricing-detail inspection
- lookup can migrate with asset registration, but pricing health remains tied
  to the pricing-detail migration

## Decision

The Valmer project will migrate asset registration and lookup to the typed
`ms-markets` asset identity model.

The canonical Valmer asset identity contract is:

- source fields:
  `tipovalor`, `emisora`, `serie`
- canonical `unique_identifier`:
  `tipovalor_emisora_serie`
- canonical asset API:
  `msm.api.assets.Asset`
- canonical asset type:
  `mexican_fixed_income`

The project will introduce one identity module:

- `src/instruments/asset_identity.py`

That module will own all Valmer asset identity construction, normalization,
lookup, and registration helpers. No DataNode, dashboard, validation script, or
instrument helper should hand-build Valmer asset registration payloads after
this migration.

## New Registration Boundary

Asset registration means:

- derive or receive a stable `unique_identifier`
- ensure a row exists in the markets `AssetTable`
- set the correct `asset_type`
- return an asset identity object or asset scope keyed by `unique_identifier`

Asset registration does not mean:

- publishing Valmer price bars
- selecting target bonds for pricing
- building QuantLib or `mainsequence.instruments` objects
- writing `instrument_dump`
- checking `current_pricing_detail`
- making an asset priceable

Pricing hydration means:

- choose the subset of Valmer assets that the project can currently price
- build the instrument object from source terms
- persist pricing details with the correct as-of timestamp
- update those details only when missing or intentionally stale

Pricing hydration must remain a separate implementation task and validation
surface.

## New Implementation Shape

### `src/instruments/asset_identity.py`

The new module should expose at least:

```python
VALMER_FIXED_INCOME_ASSET_TYPE = "mexican_fixed_income"

def build_valmer_unique_identifier(row: Mapping[str, object]) -> str:
    ...

def add_valmer_unique_identifier(df: pd.DataFrame) -> pd.DataFrame:
    ...

def resolve_valmer_assets(unique_identifiers: Sequence[str]) -> dict[str, Asset]:
    ...

def upsert_missing_valmer_assets(unique_identifiers: Sequence[str]) -> dict[str, Asset]:
    ...
```

The implementation may choose slightly different function names, but it must
keep this ownership boundary:

- identity construction is centralized
- lookup and upsert are centralized
- call sites do not know registration payload shape

### Lookup Strategy

Legacy `msc.Asset.query(unique_identifier__in=..., per_page=...)` is a large
payload lookup.

The typed `msm.api.assets.Asset.filter(...)` helper in the installed
`ms-markets==0.0.7` code does not support the same `unique_identifier__in`
filter shape. The installed repository layer does support `in_filters` through
`msm.repositories.crud.search_model(...)`.

The implementation should therefore add one project helper for batched lookup by
`unique_identifier` instead of scattering loops or single-row lookups across the
codebase.

### Schema Bootstrap

Because `msm.api.assets.Asset` is backed by markets MetaTables, the migration
must ensure the required markets schemas are created or attached once before
registration and lookup.

This must not happen inside a per-row loop.

## Implementation Tasks

- [ ] Create `src/instruments/asset_identity.py`.
  - Owner skill: `mainsequence-assets-and-translation`.
  - Scope: define the Valmer asset type constant, identifier construction,
    identifier validation, batched asset lookup, and idempotent asset upsert.
  - Validation: unit tests cover normal rows, missing fields, null fields,
    duplicate identifiers, and whitespace/string normalization.

- [ ] Move Valmer `unique_identifier` construction out of
  `ImportValmer._concatenate_artifacts_content(...)`.
  - Current code: `src/data_nodes/nodes.py:1075-1087`.
  - Target: call `add_valmer_unique_identifier(...)` from
    `src/instruments/asset_identity.py`.
  - Validation: output identifiers match current
    `tipovalor_emisora_serie` behavior for the sample workbook.

- [ ] Split `_get_uids_to_update(...)` into registration planning and pricing
  planning.
  - Current code: `src/data_nodes/nodes.py:950-1025`.
  - Target: one function determines missing asset identities; another function
    determines pricing-detail refresh candidates.
  - Validation: tests prove non-target Valmer assets can be registered without
    being marked for pricing hydration.

- [ ] Replace asset lookup in `_register_and_update_pricing(...)`.
  - Current code: `src/data_nodes/nodes.py:1135-1140`.
  - Target: call the new batched resolver from `asset_identity.py`.
  - Validation: lookup returns a complete `dict[str, Asset]` for a mixed list of
    existing and missing Valmer identifiers.

- [ ] Replace legacy custom asset registration.
  - Current code: `src/data_nodes/nodes.py:1151-1168`.
  - Target: call `msm.api.assets.Asset.upsert(...)` through the new project
    helper with `asset_type="mexican_fixed_income"`.
  - Validation: repeated calls do not create duplicate assets and preserve the
    same `unique_identifier` keys.

- [ ] Keep pricing hydration in `_register_and_update_pricing(...)` isolated
  after identity migration.
  - Current code: `src/data_nodes/nodes.py:1173-1222`.
  - Target: pricing hydration consumes resolved asset identities but remains a
    separate block with explicit TODO or implementation for the new pricing
    detail API.
  - Validation: implementation either migrates pricing details with a verified
    current API or leaves a failing/open task that clearly states pricing
    hydration is not migrated.

- [ ] Replace `build_position_from_sheet(...)` asset lookup.
  - Current code: `src/instruments/vector_to_asset.py:1030-1033`.
  - Target: use the new asset identity resolver.
  - Validation: generated pricing-check output still maps each `UID` to the
    correct platform asset UID.

- [ ] Replace dashboard asset lookup.
  - Current code: `dashboards/valmer_monitor/valmer_dashboard.py:303-335`.
  - Target: use the new asset identity resolver for lookup and a separate
    pricing-detail reader for pricing health.
  - Validation: dashboard still reports missing pricing for target bonds without
    performing asset registration from dashboard code.

- [ ] Review `MexDerTIIE28Zero.get_asset_list()`.
  - Current code: `src/data_nodes/nodes.py:719-722`.
  - Target: decide whether `TIIE_28` should be resolved as a markets `Asset`,
    a pricing-runtime constant, or an index/curve-specific object.
  - Validation: zero-curve updates no longer depend on legacy
    `mainsequence.client.Asset.get(...)`.

- [ ] Add focused tests for the migration.
  - Suggested files:
    `tests/test_valmer_asset_identity.py`,
    `tests/test_import_valmer_asset_registration.py`.
  - Validation: tests run locally without live platform credentials by mocking
    the markets asset API boundary.

- [ ] Add live validation procedure after implementation.
  - Suggested docs target: `docs/deployment.md` or a follow-up ADR section.
  - Required checks:
    - markets asset schemas exist
    - a sample Valmer vector universe resolves to assets
    - sample assets have `asset_type == "mexican_fixed_income"`
    - `ImportValmer.get_asset_list()` returns the expected asset scope
    - pricing hydration is either verified or explicitly open

## Non-Goals

This ADR does not decide to:

- change Valmer asset identity away from `tipovalor_emisora_serie`
- map Valmer bonds to FIGI, ISIN, or another public master identifier
- create an `AssetCategory` for the Valmer universe
- create an `AssetTranslationTable`
- migrate portfolio construction
- change which Valmer rows are target bonds
- claim pricing details are migrated

Those may be valid future changes, but each one changes a different boundary.

## Consequences

Positive:

- every Valmer asset registration path has one owner
- registration can be migrated without accidentally changing pricing behavior
- DataNode and dashboard asset lookup can share the same resolver
- the project aligns with the `ms-markets` typed asset model
- test coverage can target the identity boundary directly

Negative:

- this is not a mechanical API rename
- pricing hydration remains broken or incomplete until its own migration is done
- display snapshots are no longer part of the identity registration payload
- large lookups need a project helper because the new typed API does not match
  the old `query(unique_identifier__in=...)` surface

## Rejected Alternatives

### Keep Legacy `mainsequence.client.Asset`

Rejected. The local `mainsequence==4.0.11` package does not expose
`mainsequence.client.Asset`, and continuing to code against it blocks the
project upgrade.

### Register Only Target Bonds

Rejected for this ADR. Current behavior registers missing assets from the
broader Valmer vector universe while pricing details are attached only to target
bonds. Changing registration scope would be a behavior change and should be
decided separately.

### Migrate Pricing Details As Part Of Asset Registration

Rejected as a design boundary. Asset identity and pricing details are separate
concepts. They may be implemented in the same pull request, but the code must
keep them as separate phases with separate validation.

### Put Asset Helpers In `src/data_nodes/nodes.py`

Rejected. That is where the coupling exists today. The registration contract
must be reusable by scripts, dashboards, and validation tools without importing
the Valmer DataNode.

## Verification Plan

Before marking the implementation complete:

- run unit tests for `asset_identity.py`
- run focused tests for `ImportValmer` identifier construction and registration
  planning
- inspect that no project code calls:
  - `msc.Asset.query`
  - `msc.Asset.filter`
  - `msc.Asset.get`
  - `msc.Asset.batch_get_or_register_custom_assets`
- run `ImportValmer.get_asset_list()` in an authenticated platform context
- verify sample Valmer assets exist in the markets `AssetTable`
- verify sample Valmer assets have `asset_type == "mexican_fixed_income"`
- verify dashboard pricing-health lookup still resolves target-bond assets
- verify pricing-detail hydration separately, or keep it marked open with the
  exact failing or missing API call
