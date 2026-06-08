from __future__ import annotations

from msm_pricing.data_nodes import CurveConfig, DiscountCurvesNode

from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
    configure_valmer_discount_curves_cadence,
)
from valmer_connectors.instruments.rates_curves import build_tiie_valmer


def run_tiie_zero_curve_update(
    *,
    curve_identifier: str = VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
) -> None:
    """Publish the Valmer TIIE 28 curve through the canonical DiscountCurvesNode."""

    bootstrap_runtime()
    configure_valmer_discount_curves_cadence()
    config = CurveConfig(curve_unique_identifier=curve_identifier)
    node = DiscountCurvesNode(curve_config=config).set_curve_builder(build_tiie_valmer)
    node.run(force_update=True)
