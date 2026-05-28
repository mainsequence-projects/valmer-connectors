from __future__ import annotations

import json

from src.instruments.curve_bootstrap import bootstrap_valmer_curve_pricing


def main() -> None:
    result = bootstrap_valmer_curve_pricing()
    payload = {
        "index_type": result["index_type"].index_type,
        "indexes": sorted(result["indexes"]),
        "index_conventions": sorted(result["index_conventions"]),
        "curves": sorted(result["curves"]),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
