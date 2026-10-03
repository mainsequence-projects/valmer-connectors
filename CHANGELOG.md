# Changelog

## 0.1.47 (unreleased)

### Packaging

- The wheel ships only the `valmer_connectors` import package. The Valmer
  migration provider moved from the top-level `migrations` package to
  `valmer_connectors.migrations` (provider reference
  `valmer_connectors.migrations:migration`, no `PYTHONPATH`), and `banxico` and
  `fred` moved under `valmer_connectors`. Earlier wheels overwrote the
  top-level `migrations` package that ms-markets 2.0.x installed. The provider
  package, migration namespace, version table and revision `0001` are
  unchanged, so no database migration is needed.
- `BanxicoFixingsNode`, `BanxicoPolicyRatesNode` and `FredReferenceRatesNode`
  get new update hashes because their class import paths changed. Their output
  tables are unchanged, so they continue incrementally.

### Dependencies

- Requires `mainsequence>=9.0.5,<10`, `mainsequence-metatable>=0.1.14,<0.2`
  and `ms-markets>=2.1.1,<3`. The lock selects mainsequence 9.0.5,
  mainsequence-metatable 0.1.14 and ms-markets 2.1.1. Nothing here imports the
  `cli` or `command_center` packages that ms-markets 2.1 renamed.

### Deployment and migrations

- `.mainsequence/workflows/valmer-control-plane-api.yaml` moves to workflow API
  2.3.0. The API is declared as `kind: fastapi` by `source_path` under the same
  key and file, so the backend keeps the existing release. A new
  `migrate-valmer` Job runs `scripts/migrate_valmer.py`, which applies
  `valmer_connectors.migrations:migration` with
  `metatables.upgrade_application`, and the API deploys only after it
  succeeds: `image` -> `migrate` -> `deploy_api`, all from the same image. The
  Jobs workflow is unchanged and stays outside the graph.
- Hosted environments are migrated only by that Job. The
  `valmer-connectors migrations commands` output, the documentation and the
  VS Code launchers use `metatables --local ...` for local development, and
  nothing here applies the ms-markets provider any more: the ms-markets
  deployment migrates its own schema.
- The control plane lists the migration Job but refuses to launch it by hand.
- Documentation describes publishing with `git push`:
  `mainsequence code-repository sync` only refreshes dependency files since
  SDK 9.0.5.

### Agent skills

- Refreshed the SDK 9.0.5, MetaTables 0.1.14 and ms-markets 2.1.1 skills and
  the managed `AGENTS.md` block.

### Known issues

- MetaTables 0.1.14 refuses to finalize revision `0001`
  (`cascading_foreign_key_unsupported`) because its
  `valmer_connectors__valmerassetdetails.asset_uid` foreign key uses
  `ON DELETE CASCADE`. Until a new revision replaces it, the `migrate-valmer`
  Job is expected to fail and block the API deployment. See `.agents/tasks.md`.
