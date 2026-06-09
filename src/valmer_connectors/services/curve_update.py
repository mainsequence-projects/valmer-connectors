from __future__ import annotations

from msm_pricing.data_nodes import CurveConfig, DiscountCurvesNode

from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.instruments.curve_bootstrap import (
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_28_CURVE_UNIQUE_IDENTIFIER,
    configure_valmer_discount_curves_cadence,
)
from valmer_connectors.instruments.mexican_government_bond_curve import (
    build_mxn_government_curve_frame,
    select_mxn_government_bootstrap_instruments,
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

    bootstrap_runtime()
    configure_valmer_discount_curves_cadence()

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
            frame = build_mxn_government_curve_frame(
                source_data,
                curve_identifier=kwargs["curve_identifier"],
            )
            row = frame.reset_index().iloc[0]
            source_loader.logger.info(
                "Built Valmer MXN government bond curve "
                f"{row['curve_identifier']} for {row['time_index']} with "
                f"{len(row['curve'])} pillars."
            )
            return frame

        config = CurveConfig(curve_unique_identifier=curve_identifier)
        node = DiscountCurvesNode(curve_config=config).set_curve_builder(build_curve)
        node.run(force_update=True)
