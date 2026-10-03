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

- Requires `mainsequence>=9.0.5,<10`, `mainsequence-metatable>=0.1.18,<0.2`
  and `ms-markets>=2.2.2,<3`. The lock selects mainsequence 9.0.5,
  mainsequence-metatable 0.1.18 and ms-markets 2.2.2.
- MetaTables registers tables with cascading foreign keys since 0.1.17, so
  revision `0001` finalizes with its asset-details key to
  `ms_markets__asset.uid` (`ON DELETE CASCADE`): deleting an Asset deletes its
  Valmer details row. The floor is 0.1.18 because it makes write on a table
  depend only on that table's own grants; under 0.1.17, writing
  `ms_markets__asset` also required write access to
  `valmer_connectors__valmerassetdetails`.
- ms-markets 2.2.2 requires MetaTables 0.1.17 and names the long
  portfolio-ledger index explicitly, so its provider applies to a new
  environment. Nothing here imports the `cli` or `command_center` packages that
  ms-markets 2.1 renamed, or what 2.2.0 removed: `msm_command_center`, the
  asset-monitor builders, pricing `response_mappings` and the `apps.v1`
  re-export modules.

### Deployment and migrations

- `.mainsequence/workflows/valmer-control-plane-api.yaml` moves to workflow API
  2.3.0. The API is declared as `kind: fastapi` by `source_path` under the same
  key and file, so the backend keeps the existing release. A new
  `migrate-valmer` Job runs `scripts/migrate_valmer.py`, which applies
  `valmer_connectors.migrations:migration` with
  `metatables.upgrade_application`, and the API deploys only after it
  succeeds: `image` -> `migrate` -> `deploy_api`, all from the same image.
- That file is now the repository's only workflow. The sixteen pipeline Jobs
  moved into it from `.mainsequence/workflows/valmer-control-plane-jobs.yaml`
  (workflow API 2.1.0), which is deleted. Keys, names, descriptions, execution
  paths, compute, runtimes, the `0 13 * * 1-5` standard-pipeline schedule and
  every-push redeployment are unchanged. No execution step references them, so
  each one keeps redeploying on its own. The backend identifies a workflow Job
  by file and key, so the push creates sixteen new Jobs; the old ones are
  deleted by a backend with ADR-077 or must be deleted by hand. See "Cleanup
  After The Push" in `docs/deployment.md`.
- Hosted environments are migrated only by the `migrate-valmer` Job. The
  `valmer-connectors migrations commands` output, the documentation and the
  VS Code launchers use `metatables --local ...` for local development, and
  nothing here applies the ms-markets provider any more: the ms-markets
  deployment migrates its own schema. The Valmer foreign keys reference
  ms-markets tables, so the Job succeeds only in a DataSource where the
  ms-markets deployment has migrated that schema.
- The control plane lists the migration Job but refuses to launch it by hand.
- `.gitignore` ignores `.local/`, where the MetaTables local launcher keeps its
  private client token.
- Documentation describes publishing with `git push`:
  `mainsequence code-repository sync` only refreshes dependency files since
  SDK 9.0.5.

### Agent skills

- Refreshed the SDK 9.0.5, MetaTables 0.1.18 and ms-markets 2.2.2 skills and
  the managed `AGENTS.md` block. The ms-markets refresh drops the asset-monitor
  skills that 2.2.0 removed.
