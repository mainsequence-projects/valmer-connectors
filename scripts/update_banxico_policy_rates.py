from __future__ import annotations

import argparse
from collections.abc import Sequence

from banxico.policy_rates import run_banxico_policy_rates_update


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh the Banxico policy target rate.")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args(argv)
    run_banxico_policy_rates_update(runtime_end=args.end_date)


if __name__ == "__main__":
    main()
