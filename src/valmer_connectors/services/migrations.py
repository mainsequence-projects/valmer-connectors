from __future__ import annotations

CANONICAL_MIGRATION_COMMANDS = (
    "mainsequence migrations current --provider msm.migrations:migration",
    "mainsequence migrations upgrade --provider msm.migrations:migration head",
    "mainsequence migrations current --provider migrations:migration",
    "mainsequence migrations upgrade --provider migrations:migration head",
)

REVISION_COMMAND_NOTE = "Use this only after changing Valmer SQLAlchemy table contracts:"
REVISION_COMMAND = "mainsequence migrations revision --provider migrations:migration"


def migration_command_lines() -> list[str]:
    return [
        *CANONICAL_MIGRATION_COMMANDS,
        "",
        REVISION_COMMAND_NOTE,
        REVISION_COMMAND,
    ]
