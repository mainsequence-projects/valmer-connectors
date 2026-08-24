import datetime as dt
import unittest
from unittest.mock import Mock, patch

import pandas as pd

import valmer_connectors.services as services
from valmer_connectors.instruments.curve_key_nodes import (
    validate_mxn_government_key_nodes,
)
from valmer_connectors.services import curve_update


class ValmerCurveUpdateServiceTests(unittest.TestCase):
    def test_shared_runner_builds_discount_curves_node_once(self):
        frame = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-08-30 23:59:59", tz="UTC"),
                    "curve_identifier": "TEST_CURVE",
                    "curve": {28: 0.1, 91: 0.102},
                    "key_nodes": [{"maturity_date": "2024-09-27", "quote": 0.1}],
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

        self.assertIsNot(result, frame)
        self.assertNotIn("metadata_json", result.reset_index().columns)
        builder.assert_called_once()
        logger.info.assert_called_once()

    def test_shared_runner_attaches_key_nodes_validator(self):
        validator = Mock()
        builder = Mock(
            return_value=pd.DataFrame(
                [
                    {
                        "time_index": pd.Timestamp("2024-08-30 23:59:59", tz="UTC"),
                        "curve_identifier": "TEST_CURVE",
                        "curve": {28: 0.1},
                        "key_nodes": [{"maturity_date": "2024-09-27", "quote": 0.1}],
                    }
                ]
            ).set_index(["time_index", "curve_identifier"])
        )

        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime"),
            patch("valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"),
            patch("valmer_connectors.services.curve_update.CurveConfig"),
            patch("valmer_connectors.services.curve_update.DiscountCurvesNode") as node_class,
        ):
            node = Mock()
            node.set_curve_builder.return_value = node
            node.set_key_nodes_validator.return_value = node
            node_class.return_value = node

            curve_update._run_valmer_discount_curve_update(
                curve_identifier="TEST_CURVE",
                curve_builder=builder,
                key_nodes_validator=validator,
            )

        node.set_key_nodes_validator.assert_called_once_with(validator)
        node.run.assert_called_once_with(force_update=True)

    def test_shared_runner_can_rebuild_current_curve_date(self):
        builder = Mock(
            return_value=pd.DataFrame(
                [
                    {
                        "time_index": pd.Timestamp("2024-08-30 23:59:59", tz="UTC"),
                        "curve_identifier": "TEST_CURVE",
                        "curve": {28: 0.1},
                        "key_nodes": [{"maturity_date": "2024-09-27", "quote": 0.1}],
                    }
                ]
            ).set_index(["time_index", "curve_identifier"])
        )

        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime"),
            patch("valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"),
            patch("valmer_connectors.services.curve_update.CurveConfig") as curve_config,
            patch("valmer_connectors.services.curve_update.DiscountCurvesNode") as node_class,
            patch("valmer_connectors.services.curve_update.UpdateStatistics") as update_stats,
        ):
            node = Mock()
            node.set_curve_builder.return_value = node
            node_class.return_value = node
            empty_stats = object()
            update_stats.return_empty.return_value = empty_stats

            curve_update._run_valmer_discount_curve_update(
                curve_identifier="TEST_CURVE",
                curve_builder=builder,
                hash_namespace="pytest-xccy",
                rebuild_current=True,
            )

        node_class.assert_called_once_with(
            curve_config=curve_config.return_value,
            hash_namespace="pytest-xccy",
        )
        update_stats.return_empty.assert_called_once_with()
        node.run.assert_called_once_with(
            force_update=True,
            override_update_stats=empty_stats,
        )

    def test_shared_runner_rejects_curve_frame_without_key_nodes(self):
        frame = pd.DataFrame(
            [
                {
                    "time_index": pd.Timestamp("2024-08-30 23:59:59", tz="UTC"),
                    "curve_identifier": "TEST_CURVE",
                    "curve": {28: 0.1},
                }
            ]
        ).set_index(["time_index", "curve_identifier"])
        wrapped_builder = curve_update._with_curve_summary_logging(
            Mock(return_value=frame),
            logger=Mock(),
        )

        with self.assertRaisesRegex(
            ValueError,
            "key_nodes",
        ):
            wrapped_builder(
                update_statistics=object(),
                curve_identifier="TEST_CURVE",
                base_node_curve_points=None,
            )

    def test_tiie_irs_mxn_update_uses_dependency_backed_node(self):
        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime"),
            patch(
                "valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"
            ),
            patch(
                "valmer_connectors.services.curve_update.ValmerTiieDiscountCurveNode"
            ) as node_class,
        ):
            curve_update.run_tiie_irs_mxn_curve_update()

        config = node_class.call_args.kwargs["curve_config"]
        self.assertEqual(config.curve_unique_identifier, "VALMER_TIIE_OVERNIGHT")
        self.assertEqual(config.source_families, ("tiie_ois",))
        node_class.return_value.run.assert_called_once_with(force_update=True)

    def test_usd_sofr_update_uses_dependency_backed_node(self):
        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime"),
            patch(
                "valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"
            ),
            patch(
                "valmer_connectors.services.curve_update.ValmerUsdSofrDiscountCurveNode"
            ) as node_class,
        ):
            curve_update.run_usd_sofr_curve_update()

        config = node_class.call_args.kwargs["curve_config"]
        self.assertEqual(config.curve_unique_identifier, "VALMER_USD_SOFR_OVERNIGHT")
        self.assertEqual(config.source_families, ("sofr_future", "sofr_ois"))
        node_class.return_value.run.assert_called_once_with(force_update=True)

    def test_usd_mxn_xccy_update_uses_dependency_backed_node(self):
        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime"),
            patch(
                "valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"
            ),
            patch(
                "valmer_connectors.services.curve_update.ValmerUsdMxnCollateralDiscountCurveNode"
            ) as node_class,
        ):
            curve_update.run_usd_mxn_xccy_curve_update()

        config = node_class.call_args.kwargs["curve_config"]
        self.assertEqual(
            config.curve_unique_identifier,
            "VALMER_MXN_USD_COLLATERAL_DISCOUNT",
        )
        self.assertEqual(
            config.source_families,
            ("fx_spot", "fx_forward", "tiie_sofr_xccy_basis"),
        )
        node_class.return_value.run.assert_called_once_with(force_update=True)

    def test_usd_mxn_xccy_update_forwards_rebuild_controls(self):
        with (
            patch("valmer_connectors.services.curve_update.bootstrap_runtime"),
            patch(
                "valmer_connectors.services.curve_update.configure_valmer_discount_curves_cadence"
            ),
            patch(
                "valmer_connectors.services.curve_update.ValmerUsdMxnCollateralDiscountCurveNode"
            ) as node_class,
        ):
            curve_update.run_usd_mxn_xccy_curve_update(
                hash_namespace="pytest-xccy",
                rebuild_current=True,
            )

        self.assertEqual(
            node_class.call_args.kwargs["hash_namespace"],
            "pytest-xccy",
        )
        node_class.return_value.run.assert_called_once()

    def test_services_package_exports_all_curve_updates(self):
        self.assertIs(
            services.run_tiie_irs_mxn_curve_update,
            curve_update.run_tiie_irs_mxn_curve_update,
        )
        self.assertIs(
            services.run_usd_sofr_curve_update,
            curve_update.run_usd_sofr_curve_update,
        )
        self.assertIs(
            services.run_usd_mxn_xccy_curve_update,
            curve_update.run_usd_mxn_xccy_curve_update,
        )
        self.assertIs(
            services.run_mxn_government_curve_update,
            curve_update.run_mxn_government_curve_update,
        )

    def test_mxn_government_update_uses_shared_runner_with_vector_builder(self):
        with (
            patch(
                "valmer_connectors.services.curve_update._run_valmer_discount_curve_update"
            ) as run_curve,
        ):
            curve_update.run_mxn_government_curve_update(
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
                bucket_name="bucket",
                debug_artifact_path="sample.xls",
            )

        run_curve.assert_called_once_with(
            curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
            curve_builder=run_curve.call_args.kwargs["curve_builder"],
            logger=curve_update.LOGGER,
            node_class=curve_update.ValmerMxnGovernmentBondDiscountCurvesNode,
            key_nodes_validator=validate_mxn_government_key_nodes,
        )

    def test_mxn_government_curve_node_uses_first_vector_snapshot(self):
        self.assertTrue(
            issubclass(
                curve_update.ValmerMxnGovernmentBondDiscountCurvesNode,
                curve_update.DiscountCurvesNode,
            )
        )
        self.assertEqual(
            curve_update.ValmerMxnGovernmentBondDiscountCurvesNode.OFFSET_START,
            dt.datetime(2024, 8, 30, 23, 59, 59, tzinfo=dt.UTC),
        )

    def test_vector_storage_builder_queries_from_node_offset_before_first_update(self):
        source_data = _government_source_frame(
            [
                ("2026-06-01 23:59:59+00:00", "BI", "CETES", "260625"),
                ("2026-06-02 23:59:59+00:00", "BI", "CETES", "260702"),
            ]
        )
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = None

        with patch(
            "valmer_connectors.services.curve_update.load_mxn_government_curve_source_from_vector_storage",
            return_value=source_data,
        ) as load_source, patch(
            "valmer_connectors.services.curve_update.build_mxn_government_curve_frame",
            side_effect=_fake_government_curve_frame,
        ) as build_frame:
            result = curve_update.build_mxn_government_curve_from_vector_storage(
                update_statistics=update_statistics,
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
                base_node_curve_points=None,
                logger=Mock(),
        )

        load_source.assert_called_once_with(
            start_time_index=(
                curve_update.ValmerMxnGovernmentBondDiscountCurvesNode.OFFSET_START
            ),
            logger=load_source.call_args.kwargs["logger"],
        )
        self.assertEqual(build_frame.call_count, 2)
        self.assertEqual(len(result), 2)

    def test_vector_storage_builder_queries_after_existing_curve_update(self):
        source_data = _government_source_frame(
            [("2026-06-03 23:59:59+00:00", "BI", "CETES", "260709")]
        )
        update_statistics = Mock()
        update_statistics.get_last_update_for_identity.return_value = pd.Timestamp(
            "2026-06-02 23:59:59",
            tz="UTC",
        )

        with patch(
            "valmer_connectors.services.curve_update.load_mxn_government_curve_source_from_vector_storage",
            return_value=source_data,
        ) as load_source, patch(
            "valmer_connectors.services.curve_update.build_mxn_government_curve_frame",
            side_effect=_fake_government_curve_frame,
        ):
            curve_update.build_mxn_government_curve_from_vector_storage(
                update_statistics=update_statistics,
                curve_identifier="VALMER_MXN_GOVERNMENT_BOND",
                base_node_curve_points=None,
            )

        load_source.assert_called_once_with(
            after_time_index=dt.datetime(2026, 6, 2, 23, 59, 59, tzinfo=dt.UTC),
            logger=None,
        )

    def test_vector_storage_rows_fill_curve_source_shape(self):
        time_index = pd.Timestamp("2026-06-01 23:59:59", tz="UTC")
        frame = curve_update._vector_storage_rows_to_curve_source_frame(
            [
                {
                    "time_index": time_index,
                    "unique_identifier": "BI_CETES_260625",
                    "fecha": None,
                    "tipovalor": "BI",
                    "emisora": "CETES",
                }
            ],
            time_index=time_index,
        )

        self.assertEqual(frame.loc[0, "fecha"], time_index)
        self.assertIn("preciosucio", frame.columns)
        self.assertIn("fechavcto", frame.columns)

    def test_vector_storage_rows_can_return_empty_range_frame(self):
        frame = curve_update._vector_storage_rows_to_curve_source_frame(
            [],
            time_index=None,
            allow_empty=True,
        )

        self.assertTrue(frame.empty)
        self.assertIn("time_index", frame.columns)
        self.assertIn("tipovalor", frame.columns)

    def test_vector_storage_loader_rejects_truncated_governed_result(self):
        runtime_context = Mock(
            data_source_uid="data-source",
            timeout=30,
            namespace="mainsequence.markets",
            reserved_policy="reject",
        )

        with (
            patch(
                "valmer_connectors.meta_tables.valmer_asset_details."
                "ensure_valmer_asset_detail_runtime",
                return_value=runtime_context,
            ),
            patch(
                "msm.repositories.base.execute_markets_operation",
                return_value={"rows": [], "truncated": True},
            ),
            patch(
                "msm.repositories.base.compile_markets_statement",
                return_value=object(),
            ),
        ):
            with self.assertRaisesRegex(
                curve_update.MexicanGovernmentBondCurveError,
                "refusing to bootstrap from a truncated history",
            ):
                curve_update.load_mxn_government_curve_source_from_vector_storage(
                    start_time_index="2024-08-30T23:59:59Z",
                )

    def test_empty_curve_frame_matches_discount_curve_storage_contract(self):
        frame = curve_update._empty_curve_frame()

        columns = frame.reset_index().columns.tolist()
        self.assertEqual(
            columns,
            [
                "time_index",
                "curve_identifier",
                "curve",
                "key_nodes",
                "metadata_json",
            ],
        )


def _government_source_frame(rows):
    records = []
    for time_index, security_type, issuer, series in rows:
        timestamp = pd.Timestamp(time_index)
        records.append(
            {
                "time_index": timestamp,
                "unique_identifier": f"{security_type}_{issuer}_{series}",
                "fecha": timestamp.normalize(),
                "tipovalor": security_type,
                "emisora": issuer,
                "serie": series,
                "sector": "GUBERNAMENTAL",
                "monedaemision": "MPS",
                "fechavcto": timestamp + pd.Timedelta(days=28),
                "preciosucio": 9.9,
            }
        )
    return pd.DataFrame(records)


def _fake_government_curve_frame(source_df, *, curve_identifier):
    time_index = pd.Timestamp(source_df["time_index"].iloc[0])
    return pd.DataFrame(
        [
            {
                "time_index": time_index,
                "curve_identifier": curve_identifier,
                "curve": {28: 0.1},
                "key_nodes": [
                    {
                        "maturity_date": (
                            time_index + pd.Timedelta(days=28)
                        ).date().isoformat(),
                        "quote": 9.9,
                        "source_reference": {
                            "type": "asset",
                            "identifier": str(
                                source_df["unique_identifier"].iloc[0]
                            ),
                        },
                    }
                ],
            }
        ]
    ).set_index(["time_index", "curve_identifier"])


if __name__ == "__main__":
    unittest.main()
