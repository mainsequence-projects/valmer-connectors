# ADR 0004: Valmer Connectors CLI Design

## Status

Accepted / Implemented

## Date

2026-06-04

## Context

The project previously exposed operational behavior through loose scripts under
`scripts/`:

- `scripts/update_vector_valmer.py`
- `scripts/update_tiie_zero_curve.py`
- `scripts/validate_runtime.py`

Those scripts are useful, but they are not a stable package interface. They also
make it harder to discover what operational commands exist after installation or
inside a deployed project image.

The project package is now `valmer_connectors`, so any project-owned CLI should
live inside that package and be exposed through Python packaging metadata.

The CLI must not reimplement Main Sequence migration behavior. Main Sequence
migrations remain owned by the canonical `mainsequence migrations ...` CLI and
the migration providers:

- `msm.migrations:migration`
- `migrations:migration`

## Decision

Add a project CLI entry point named `valmer-connectors` backed by package code
under `src/valmer_connectors`.

Current package shape:

```text
src/valmer_connectors/
  cli/
    __init__.py
    main.py
  services/
    __init__.py
    curve_update.py
    migrations.py
    runtime_validation.py
    vector_update.py
```

Packaging entry point:

```toml
[project.scripts]
valmer-connectors = "valmer_connectors.cli.main:main"
```

The CLI uses Python stdlib `argparse`. The project does not depend on `click`
or `typer`.

`cli/main.py` only parses arguments and dispatches. Operational behavior lives
in `valmer_connectors.services.*` so scripts, tests, jobs, and APIs can reuse
the same functions.

## Command Surface

### `valmer-connectors version`

Print installed versions for:

- `valmer-connectors`
- `mainsequence`
- `ms-markets`

This command is offline and should not require platform credentials.

### `valmer-connectors migrations commands`

Print the canonical migration sequence for this project:

```bash
mainsequence migrations current --provider msm.migrations:migration
mainsequence migrations upgrade --provider msm.migrations:migration head
mainsequence migrations current --provider migrations:migration
mainsequence migrations upgrade --provider migrations:migration head
```

This command must not run migrations. It is a discovery/help command only.

### `valmer-connectors runtime validate`

Run `bootstrap_runtime(override=True)` and print a JSON summary of the runtime
bootstrap result:

- index type
- indexes
- index conventions
- curves

This command requires platform credentials and already migrated MetaTables.

### `valmer-connectors vector update`

Own the behavior previously exposed by `scripts/update_vector_valmer.py`.

Default behavior:

- call `bootstrap_runtime()`;
- build `ImportValmer` with `ImportValmerConfig`;
- call `ImportValmer.prepare_for_update()` to import source rows, sync
  AssetTable rows, sync `ValmerAssetDetailsTable`, and hydrate supported
  current pricing details;
- run the Valmer vector DataNode update with `run(force_update=True)`.

Options:

```text
--bucket-name TEXT
--debug-artifact-path PATH
--first-loop-count INT
```

If `--bucket-name` is omitted, resolve the platform source bucket from
`VALMER_VECTOR_BUCKET_NAME`. The legacy bucket name constant is only a
backwards-compatible fallback.

Do not expose `--force` / `--no-force`.
`force_update=True` is the current script behavior and the intended default.
Define what "new work" means for Valmer inputs before adding any non-forced
update mode.

Do not expose full-source asset registration until the project has a real
Valmer asset-type classifier. The Valmer vector contains multiple instrument
types; the current asset registration path only registers rows selected by the
target-bond pricing filter.

### `valmer-connectors curves update-mxn-government`

Run the Valmer MXN government bond discount-curve update.

Default behavior:

- call `bootstrap_runtime()`;
- import Valmer Vector Analitico rows without running asset registration,
  asset-detail sync, vector publication, or bond pricing hydration;
- select CETES and M Bonos MXN government bootstrap rows;
- build the curve frame for `VALMER_MXN_GOVERNMENT_BOND`;
- publish through `msm_pricing.data_nodes.DiscountCurvesNode` with
  `run(force_update=True)`.

Options:

```text
--curve-identifier TEXT
--bucket-name TEXT
--debug-artifact-path PATH
```

### `valmer-connectors curves update-tiie-zero`

Own the behavior previously exposed by `scripts/update_tiie_zero_curve.py`.

Default behavior:

- call `bootstrap_runtime()`;
- build `DiscountCurvesNode` with the Valmer TIIE 28 curve identifier;
- attach `build_tiie_valmer`;
- run the curve DataNode update with the current script behavior,
  `run(force_update=True)`.

Options:

```text
--curve-identifier TEXT
```

The default curve identifier is
`VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER`.

Do not expose `--force` / `--no-force`.
`force_update=True` is the current script behavior and the intended default.

## Script Compatibility

Keep existing operational `scripts/*.py` files as thin compatibility wrappers.

Example:

```python
from valmer_connectors.services.vector_update import run_vector_update


if __name__ == "__main__":
    run_vector_update()
```

`scripts/test_script.py` is not a durable operational command and should not be
promoted into the CLI.

## Non-Goals

This ADR does not:

- reimplement `mainsequence migrations`;
- add Alembic table-creation migration files;
- run live platform updates during installation;
- introduce a new CLI framework dependency;
- change DataNode, curve, or bootstrap semantics;
- rename package modules outside the CLI/operations boundary.

## Implementation Tasks

- [x] Add `src/valmer_connectors/services/`.
- [x] Add reusable service functions for runtime validation, vector update,
      TIIE zero curve update, and migration command rendering.
- [x] Add reusable service function for the MXN government bond curve update.
- [x] Add `src/valmer_connectors/cli/main.py` using `argparse`.
- [x] Add `[project.scripts] valmer-connectors = "valmer_connectors.cli.main:main"`.
- [x] Convert `scripts/update_vector_valmer.py` into a thin wrapper.
- [x] Convert `scripts/update_tiie_zero_curve.py` into a thin wrapper.
- [x] Convert `scripts/validate_runtime.py` into a thin wrapper.
- [x] Add `valmer-connectors curves update-mxn-government`.
- [x] Document CLI usage in project docs after implementation.

## Validation

Offline validation:

```bash
valmer-connectors version
valmer-connectors migrations commands
TDAG_ROOT_PATH=/private/tmp/tdag-plan .venv/bin/python -m unittest discover -s tests
```

Live validation after credentials and migrations:

```bash
valmer-connectors runtime validate
valmer-connectors vector update
valmer-connectors curves update-tiie-zero
valmer-connectors curves update-mxn-government
```

## Consequences

The project gains a stable installed command surface while preserving the
canonical Main Sequence migration boundary.

Operational code becomes reusable outside ad hoc scripts. Scripts remain
available during migration but stop owning business behavior.
