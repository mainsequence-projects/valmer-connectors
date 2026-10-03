from __future__ import annotations

# The Valmer provider lives under the project namespace, so its reference is
# unambiguous next to the ms-markets provider and needs no ``PYTHONPATH``.
VALMER_MIGRATION_PROVIDER = "valmer_connectors.migrations:migration"

# Every provider this repository owns, in dependency order. The deployment
# migration Job applies exactly these. The ms-markets schema that the Valmer
# tables reference is migrated by the ms-markets deployment, never from here.
MIGRATION_JOB_PROVIDERS = (VALMER_MIGRATION_PROVIDER,)

MIGRATION_JOB_EXECUTION_PATH = "scripts/migrate_valmer.py"
MIGRATION_WORKFLOW_PATH = ".mainsequence/workflows/valmer-control-plane-api.yaml"

LOCAL_MIGRATION_NOTE = "Local development, against the running `metatables serve --local` runtime:"
LOCAL_MIGRATION_COMMANDS = (
    f"metatables --local migrations upgrade --provider {VALMER_MIGRATION_PROVIDER} head",
    f"metatables --local migrations current --provider {VALMER_MIGRATION_PROVIDER}",
)

HOSTED_MIGRATION_NOTE = (
    "Hosted environments are migrated only by the deployment workflow's migration "
    f"Job ({MIGRATION_JOB_EXECUTION_PATH} in {MIGRATION_WORKFLOW_PATH}), before "
    "the control-plane API rolls out. Do not run migrations upgrade or downgrade "
    "against a hosted runtime."
)

REVISION_COMMAND_NOTE = "Use this only after changing Valmer SQLAlchemy table contracts:"
REVISION_COMMAND = f"metatables migrations revision --provider {VALMER_MIGRATION_PROVIDER}"


def migration_command_lines() -> list[str]:
    return [
        LOCAL_MIGRATION_NOTE,
        *LOCAL_MIGRATION_COMMANDS,
        "",
        HOSTED_MIGRATION_NOTE,
        "",
        REVISION_COMMAND_NOTE,
        REVISION_COMMAND,
    ]
