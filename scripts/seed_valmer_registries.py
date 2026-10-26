"""Main Sequence Job: write Valmer's static Markets registry rows.

``bootstrap_runtime()`` no longer seeds by default; this Job (the first stage of
the standard pipeline) is the one process that writes the Index, convention,
curve and market-data binding rows Valmer's producers reference, through the
Markets API (ms-markets ADR 0047).
"""

from __future__ import annotations

import json

from valmer_connectors.runtime import use_markets_api
from valmer_connectors.services.registry_seed import (
    seed_valmer_registries,
    summarize_seeded_registries,
)


def main() -> int:
    result = seed_valmer_registries()
    print(json.dumps(summarize_seeded_registries(result), indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    use_markets_api()
    raise SystemExit(main())
