from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
import structlog
from msm_pricing.data_nodes import CurveConfig, DiscountCurvesNode

from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
    configure_valmer_discount_curves_cadence,
)
from valmer_connectors.instruments.mexican_government_bond_curve import (
    build_mxn_government_curve_from_vector,
    select_mxn_government_bootstrap_instruments,
)
from valmer_connectors.instruments.rates_curves import build_tiie_valmer

CurveBuilder = Callable[..., pd.DataFrame]
LOGGER = structlog.get_logger(__name__)


def _run_valmer_discount_curve_update(
    *,
    curve_identifier: str,
    curve_builder: CurveBuilder,
    logger=None,
) -> None:
    """Run a Valmer discount curve builder through the canonical pricing node."""

    bootstrap_runtime()
    configure_valmer_discount_curves_cadence()
    node = DiscountCurvesNode(
        curve_config=CurveConfig(curve_unique_identifier=curve_identifier)
    ).set_curve_builder(
        _with_curve_summary_logging(
            curve_builder,
            logger=logger or LOGGER,
        )
    )
    node.run(force_update=True)


def _with_curve_summary_logging(
    curve_builder: CurveBuilder,
    *,
    logger,
) -> CurveBuilder:
    def build_curve(**kwargs: Any) -> pd.DataFrame:
        frame = curve_builder(**kwargs)
        _log_curve_frame_summary(frame, logger=logger)
        return frame

    return build_curve


def _log_curve_frame_summary(frame: pd.DataFrame, *, logger) -> None:
    if frame.empty:
        logger.warning("Valmer curve builder returned an empty frame.")
        return

    for row in frame.reset_index().itertuples(index=False):
        curve_points = getattr(row, "curve")
        logger.info(
            "Built Valmer discount curve "
            f"{getattr(row, 'curve_identifier')} for {getattr(row, 'time_index')} "
            f"with {len(curve_points)} pillars."
        )


def run_tiie_zero_curve_update(
    *,
    curve_identifier: str = VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
) -> None:
    """Publish the Valmer TIIE 28 curve through the canonical DiscountCurvesNode."""

    _run_valmer_discount_curve_update(
        curve_identifier=curve_identifier,
        curve_builder=build_tiie_valmer,
    )


def run_mxn_government_curve_update(
    *,
    curve_identifier: str = VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    bucket_name: str | None = None,
    debug_artifact_path: str | None = None,
) -> None:
    """Publish the Valmer MXN government bond curve through DiscountCurvesNode."""

    from valmer_connectors.data_nodes.nodes import ImportValmer, ImportValmerConfig
    from valmer_connectors.settings import resolve_valmer_vector_bucket_name
    from valmer_connectors.services.vector_update import _debug_artifact_path

    with _debug_artifact_path(debug_artifact_path):
        source_loader = ImportValmer(
            config=ImportValmerConfig(
                bucket_name=resolve_valmer_vector_bucket_name(bucket_name),
            )
        )
        source_data = source_loader.prepare_source_data()
        selected = select_mxn_government_bootstrap_instruments(source_data)
        selected_counts = (
            selected.groupby(["tipovalor", "emisora"], dropna=False)
            .size()
            .to_dict()
        )
        source_loader.logger.info(
            "Selected "
            f"{len(selected)} Valmer MXN government curve rows from "
            f"{len(source_data)} vector rows; skipped {len(source_data) - len(selected)} rows. "
            f"Selected families: {selected_counts}."
        )

        def build_curve(**kwargs):
            return build_mxn_government_curve_from_vector(
                **kwargs,
                source_df=source_data,
            )

        _run_valmer_discount_curve_update(
            curve_identifier=curve_identifier,
            curve_builder=build_curve,
            logger=source_loader.logger,
        )
