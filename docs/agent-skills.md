# Agent Skills

`valmer-connectors` ships project-specific Codex skills for downstream
repositories that import Valmer data or extend Valmer asset registration.

Install or update the skills in a host project with:

```bash
valmer-connectors copy-valmer-skills --path /path/to/host-project
```

From inside the host project, this is usually enough:

```bash
valmer-connectors copy-valmer-skills --path .
```

The command copies packaged skills into:

```text
<host-project>/.agents/skills/valmer-connectors
```

It only replaces the `valmer-connectors` skill namespace. It does not modify
`mainsequence`, `ms_markets`, or other project skill namespaces.

## Current Skills

```text
.agents/skills/valmer-connectors/
  registering_assets/
    SKILL.md
```

`registering_assets` explains the public Valmer asset registration API:

```python
from valmer_connectors.assets import register_valmer_assets_from_rows
```

Use it when implementing or reviewing source-row registration that needs the
full Valmer workflow:

```text
Valmer rows
    |
    +-- AssetTable rows
    +-- ValmerAssetDetailsTable rows
    +-- AssetSnapshot rows
    +-- optional pricing details
```

## Safety Guard

The copy command refuses to run when `--path` points at the
`valmer-connectors` source checkout. This prevents the command from deleting or
replacing this repository's own source skill bundle while trying to refresh a
host project's skills.

Use the command only from a separate host project that depends on
`valmer-connectors`.

## Validation

To preview changes without writing files:

```bash
valmer-connectors copy-valmer-skills --path /path/to/host-project --dry-run
```

For automation or CI checks:

```bash
valmer-connectors copy-valmer-skills --path /path/to/host-project --dry-run --json
```
