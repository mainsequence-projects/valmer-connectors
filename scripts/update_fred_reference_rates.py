from __future__ import annotations

import argparse
from collections.abc import Sequence

from fred.reference_rates import run_fred_reference_rates_update


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh configured FRED reference rates.")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args(argv)
    run_fred_reference_rates_update(runtime_end=args.end_date)


if __name__ == "__main__":
    main()
