from mainsequence.instruments.interest_rates.etl.nodes import CurveConfig, DiscountCurvesNode
from src.instruments.bootstrap import register_all


def main() -> None:
    register_all()
    configs = [
        CurveConfig(
            curve_const="ZERO_CURVE__VALMER_TIIE_28",
            name="Discount Curve TIIE 28 Mexder Valmer",
        ),
    ]

    for cfg in configs:
        node = DiscountCurvesNode(curve_config=cfg)
        node.run(force_update=True)


if __name__ == "__main__":
    main()
