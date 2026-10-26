from __future__ import annotations

import argparse
from collections.abc import Sequence

from valmer_connectors.banxico.fixings import run_banxico_fixings_update
from valmer_connectors.runtime import use_markets_api


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh all supported Banxico fixings.")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args(argv)
    run_banxico_fixings_update(end_date=args.end_date)


if __name__ == "__main__":
    use_markets_api()
    main()
