"""Main Sequence Job: apply the Valmer migrations before the control-plane API rolls out.

`.mainsequence/workflows/valmer-control-plane-api.yaml` runs this Job from the
candidate image, so the revisions it applies are the ones that image ships. A
failure stops the rollout and the previous release keeps serving.

The Job applies only the providers this repository owns. The ms-markets tables
that the Valmer tables reference are migrated by the ms-markets deployment.
"""

from __future__ import annotations

from metatables import upgrade_application

from valmer_connectors.services.migrations import MIGRATION_JOB_PROVIDERS


def main() -> int:
    for provider in MIGRATION_JOB_PROVIDERS:
        result = upgrade_application(provider)
        state = "migrated" if result["migrated"] else "already current"
        print(
            f"{provider}: {result['revision']} ({state}) on {result['data_source_uid']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
