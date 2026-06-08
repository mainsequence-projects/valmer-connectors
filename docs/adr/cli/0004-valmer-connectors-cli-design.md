# ADR 0004: Valmer Connectors CLI Design

## Status

Proposed

## Date

2026-06-04

## Context

The project currently exposes operational behavior through loose scripts under
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

Initial package shape:

```text
src/valmer_connectors/
  cli.py
  operations/
    __init__.py
    curves.py
    metadata.py
    migrations.py
    runtime.py
    vector.py
```

Packaging entry point:

```toml
[project.scripts]
valmer-connectors = "valmer_connectors.cli:main"
```

Use Python stdlib `argparse` for the first implementation. Do not add `click` or
`typer` until the command surface is large enough to justify a new dependency.

`cli.py` should only parse arguments and dispatch. Operational behavior belongs
in `valmer_connectors.operations.*` so scripts, tests, jobs, and future APIs can
reuse the same functions.

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

### `valmer-connectors metadata inspect`

Run offline metadata checks for the Valmer project models:

- `migrations:migration` imports successfully.
- `VALMER_MIGRATION_MODELS` contains only project-owned models.
- `ValmerVectorPricesStorage.__metatable_identifier__` is
  `vector_de_precios_valmer`.
- Valmer storage and detail foreign keys point to `AssetTable`.
- default PostgreSQL schema is authored as `None`, not `"public"`.
- Valmer Alembic version table is project-prefixed.

This command should return non-zero on failed checks.

### `valmer-connectors runtime validate`

Run `bootstrap_runtime(override=True)` and print a JSON summary of the runtime
bootstrap result:

- index type
- indexes
- index conventions
- curves

This command requires platform credentials and already migrated MetaTables.

### `valmer-connectors vector update`

Replace the behavior currently in `scripts/update_vector_valmer.py`.

Default behavior:

- call `bootstrap_runtime()`;
- build `ImportValmer` with `ImportValmerConfig`;
- run the Valmer vector DataNode update with the current script behavior,
  `run(force_update=True)`.

Initial options:

```text
--bucket-name TEXT
--first-loop-count INT
```

The default bucket should remain `BUCKET_NAME_HISTORICAL_VECTORS`.

Do not expose `--force` / `--no-force` in the first CLI version.
`force_update=True` is the current script behavior and the intended default.
If non-forced update behavior becomes useful later, define what "new work"
means for Valmer inputs before adding a separate option.

### `valmer-connectors curves update-tiie-zero`

Replace the behavior currently in `scripts/update_tiie_zero_curve.py`.

Default behavior:

- call `bootstrap_runtime()`;
- build `DiscountCurvesNode` with the Valmer TIIE 28 curve identifier;
- attach `build_tiie_valmer`;
- run the curve DataNode update with the current script behavior,
  `run(force_update=True)`.

Initial options:

```text
--curve-identifier TEXT
```

The default curve identifier should remain
`VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER`.

Do not expose `--force` / `--no-force` in the first CLI version.
`force_update=True` is the current script behavior and the intended default.

## Script Compatibility

Keep existing `scripts/*.py` files temporarily as thin compatibility wrappers.

Example:

```python
from valmer_connectors.operations.vector import run_vector_update


if __name__ == "__main__":
    run_vector_update()
```

After the CLI is validated in local and deployed environments, the wrappers may
either be deleted or kept as minimal examples.

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

- [ ] Add `src/valmer_connectors/operations/`.
- [ ] Add reusable operation functions for runtime validation, vector update,
      TIIE zero curve update, metadata inspection, and migration command
      rendering.
- [ ] Add `src/valmer_connectors/cli.py` using `argparse`.
- [ ] Add `[project.scripts] valmer-connectors = "valmer_connectors.cli:main"`.
- [ ] Convert `scripts/update_vector_valmer.py` into a thin wrapper.
- [ ] Convert `scripts/update_tiie_zero_curve.py` into a thin wrapper.
- [ ] Convert `scripts/validate_runtime.py` into a thin wrapper.
- [ ] Add CLI unit tests for offline commands.
- [ ] Add tests that the script wrappers call the package operations.
- [ ] Document CLI usage in project docs after implementation.

## Validation

Offline validation:

```bash
valmer-connectors version
valmer-connectors metadata inspect
valmer-connectors migrations commands
TDAG_ROOT_PATH=/private/tmp/tdag-plan .venv/bin/python -m unittest discover -s tests
```

Live validation after credentials and migrations:

```bash
valmer-connectors runtime validate
valmer-connectors vector update
valmer-connectors curves update-tiie-zero
```

## Consequences

The project gains a stable installed command surface while preserving the
canonical Main Sequence migration boundary.

Operational code becomes reusable outside ad hoc scripts. Scripts remain
available during migration but stop owning business behavior.
