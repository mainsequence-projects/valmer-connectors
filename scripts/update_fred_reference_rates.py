from __future__ import annotations

from fred.reference_rates import run_fred_reference_rates_update


def main() -> None:
    run_fred_reference_rates_update()


if __name__ == "__main__":
    main()
