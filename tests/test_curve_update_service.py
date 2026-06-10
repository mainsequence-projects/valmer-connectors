import unittest
from unittest.mock import Mock, patch

import pandas as pd

from valmer_connectors.services import curve_update


class ValmerCurveUpdateServiceTests(unittest.TestCase):
    def test_shared_runner_builds_discount_curves_node_once(self):
        frame = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-08-30 23:59:59", tz="UTC"),
                    "curve_identifier": "TEST_CURVE",
                    "curve": {28: 0.1, 91: 0.102},
                }
            ]
        ).set_index(["time_index", "curve_identifier"])
        builder = Mock(return_value=frame)
        logger = Mock()

        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime") as bootstrap,
            patch(
                "valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"
            ) as configure_cadence,
            patch("valmer_connectors.services.curve_update.CurveConfig") as curve_config,
            patch("valmer_connectors.services.curve_update.DiscountCurvesNode") as node_class,
        ):
            node = Mock()
            node.set_curve_builder.return_value = node
            node_class.return_value = node

            curve_update._run_valmer_discount_curve_update(
                curve_identifier="TEST_CURVE",
                curve_builder=builder,
                logger=logger,
            )

        bootstrap.assert_called_once_with()
        configure_cadence.assert_called_once_with()
        curve_config.assert_called_once_with(curve_unique_identifier="TEST_CURVE")
        node_class.assert_called_once_with(curve_config=curve_config.return_value)
        node.run.assert_called_once_with(force_update=True)

        wrapped_builder = node.set_curve_builder.call_args.args[0]
        result = wrapped_builder(
            update_statistics=object(),
            curve_identifier="TEST_CURVE",
            base_node_curve_points=None,
        )

        self.assertIs(result, frame)
        builder.assert_called_once()
        logger.info.assert_called_once()

    def test_tiie_update_uses_shared_runner(self):
        with patch(
            "valmer_connectors.services.curve_update._run_valmer_discount_curve_update"
        ) as run_curve:
            curve_update.run_tiie_zero_curve_update(curve_identifier="VALMER_TIIE_28")

        run_curve.assert_called_once_with(
            curve_identifier="VALMER_TIIE_28",
            curve_builder=curve_update.build_tiie_valmer,
        )

    def test_mxn_government_update_uses_shared_runner_with_vector_builder(self):
        source_data = pd.DataFrame(
            [
                {
                    "fecha": "20240830",
                    "tipovalor": "BI",
                    "emisora": "CETES",
                    "serie": "240926",
                    "sector": "GUBERNAMENTAL",
                    "monedaemision": "MPS",
                    "fechavcto": "2024-09-26",
                    "preciosucio": 9.9,
                }
            ]
        )
        source_loader = Mock()
        source_loader.prepare_source_data.return_value = source_data

        with (
            patch("valmer_connectors.data_nodes.nodes.ImportValmer", return_value=source_loader),
            patch("valmer_connectors.data_nodes.nodes.ImportValmerConfig") as config_class,
            patch(
                "valmer_connectors.settings.resolve_valmer_vector_bucket_name",
                return_value="Vector Bucket",
            ),
            patch("valmer_connectors.services.vector_update._debug_artifact_path") as debug_path,
            patch(
                "valmer_connectors.services.curve_update.select_mxn_government_bootstrap_instruments",
                return_value=source_data,
            ),
            patch(
                "valmer_connectors.services.curve_update._run_valmer_discount_curve_update"
            ) as run_curve,
        ):
            debug_path.return_value.__enter__.return_value = None
            debug_path.return_value.__exit__.return_value = False

            curve_update.run_mxn_government_curve_update(
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
                bucket_name="bucket",
                debug_artifact_path="sample.xls",
            )

        config_class.assert_called_once_with(bucket_name="Vector Bucket")
        run_curve.assert_called_once()
        self.assertEqual(
            run_curve.call_args.kwargs["curve_identifier"],
            "VALMER_MXN_GOVERNMENT_BOND",
        )

        builder = run_curve.call_args.kwargs["curve_builder"]
        with patch(
            "valmer_connectors.services.curve_update.build_mxn_government_curve_from_vector",
            return_value="curve-frame",
        ) as build_frame:
            result = builder(
                update_statistics=object(),
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
                base_node_curve_points=None,
            )

        self.assertEqual(result, "curve-frame")
        self.assertIs(build_frame.call_args.kwargs["source_df"], source_data)


if __name__ == "__main__":
    unittest.main()
