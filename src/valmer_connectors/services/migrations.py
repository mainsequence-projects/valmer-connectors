from __future__ import annotations

# The Valmer provider lives under the project namespace, so its reference is
# unambiguous next to the ms-markets core provider and needs no ``PYTHONPATH``.
VALMER_MIGRATION_PROVIDER = "valmer_connectors.migrations:migration"

CANONICAL_MIGRATION_COMMANDS = (
    "metatables migrations current --provider msm.migrations:migration",
    "metatables migrations upgrade --provider msm.migrations:migration head",
    f"metatables migrations current --provider {VALMER_MIGRATION_PROVIDER}",
    f"metatables migrations upgrade --provider {VALMER_MIGRATION_PROVIDER} head",
)

REVISION_COMMAND_NOTE = "Use this only after changing Valmer SQLAlchemy table contracts:"
REVISION_COMMAND = f"metatables migrations revision --provider {VALMER_MIGRATION_PROVIDER}"


def migration_command_lines() -> list[str]:
    return [
        *CANONICAL_MIGRATION_COMMANDS,
        "",
        REVISION_COMMAND_NOTE,
        REVISION_COMMAND,
    ]
