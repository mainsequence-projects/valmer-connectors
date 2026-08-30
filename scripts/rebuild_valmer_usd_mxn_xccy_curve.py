"""Rebuild the current USD/MXN XCCY curve even when update statistics are current."""

from valmer_connectors.services.curve_update import run_usd_mxn_xccy_curve_update


def main() -> None:
    run_usd_mxn_xccy_curve_update(rebuild_current=True)


if __name__ == "__main__":
    main()
