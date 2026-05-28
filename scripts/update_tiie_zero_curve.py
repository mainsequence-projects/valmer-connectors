from msm_pricing.data_nodes import CurveConfig, DiscountCurvesNode

from src.instruments.curve_bootstrap import (
    VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
    bootstrap_valmer_curve_pricing,
)
from src.instruments.rates_curves import build_tiie_valmer


def main() -> None:
    bootstrap_valmer_curve_pricing()
    cfg = CurveConfig(curve_unique_identifier=VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER)
    node = DiscountCurvesNode(curve_config=cfg).set_curve_builder(build_tiie_valmer)
    node.run(force_update=True)


if __name__ == "__main__":
    main()
