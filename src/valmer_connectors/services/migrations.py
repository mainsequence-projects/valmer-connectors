from __future__ import annotations

# ms-markets also installs a top-level ``migrations`` package (its core provider,
# re-exported as ``msm.migrations``). ``metatables migrations`` only prepends the
# current directory to ``sys.path``, so the Valmer provider under ``src/migrations``
# is selected with ``PYTHONPATH=src`` and the ms-markets provider runs without it.
CANONICAL_MIGRATION_COMMANDS = (
    "metatables migrations current --provider msm.migrations:migration",
    "metatables migrations upgrade --provider msm.migrations:migration head",
    "PYTHONPATH=src metatables migrations current --provider migrations:migration",
    "PYTHONPATH=src metatables migrations upgrade --provider migrations:migration head",
)

REVISION_COMMAND_NOTE = "Use this only after changing Valmer SQLAlchemy table contracts:"
REVISION_COMMAND = "PYTHONPATH=src metatables migrations revision --provider migrations:migration"


def migration_command_lines() -> list[str]:
    return [
        *CANONICAL_MIGRATION_COMMANDS,
        "",
        REVISION_COMMAND_NOTE,
        REVISION_COMMAND,
    ]
