from __future__ import annotations

import json

from valmer_connectors.services.runtime_validation import validate_runtime


def main() -> None:
    print(json.dumps(validate_runtime(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
