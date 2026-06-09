import unittest
from unittest.mock import Mock, patch

from msm.constants import INDEX_TYPE_INTEREST_RATE

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.instruments.bootstrap import seed_static_defaults
from valmer_connectors.instruments.curve_bootstrap import (
    MEXICAN_INDEX_CONVENTION_DEFINITIONS,
    MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    VALMER_DISCOUNT_CURVES_CADENCE,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION,
    VALMER_TIIE_28_CURVE_DEFINITION,
    attach_valmer_curve_pricing_runtime,
    configure_valmer_discount_curves_cadence,
    create_valmer_curve_pricing_schemas,
    mexican_reference_index_payloads,
)
from valmer_connectors.meta_tables.valmer_asset_details import ValmerAssetDetailsTable


class ValmerCurveBootstrapTests(unittest.TestCase):
    def test_reference_index_definitions_cover_required_tiie_and_cete_indexes(self):
        identifiers = {
            definition.unique_identifier for definition in MEXICAN_REFERENCE_INDEX_DEFINITIONS
        }

        self.assertEqual(
            identifiers,
            {
                "TIIE_OVERNIGHT",
                "TIIE_28",
                "TIIE_91",
                "TIIE_182",
                "CETE_28",
                "CETE_91",
                "CETE_182",
                "MXN_GOVERNMENT_BOND",
            },
        )

    def test_reference_index_payloads_target_core_index_api(self):
        payload_by_identifier = {
            payload["unique_identifier"]: payload for payload in mexican_reference_index_payloads()
        }

        tiie_28 = payload_by_identifier["TIIE_28"]
        self.assertEqual(tiie_28["index_type"], INDEX_TYPE_INTEREST_RATE)
        self.assertIsNone(tiie_28["provider"])
        self.assertIsNone(tiie_28["metadata_json"])

    def test_convention_payload_keeps_pricing_terms_off_index_payload(self):
        definition = next(
            item
            for item in MEXICAN_INDEX_CONVENTION_DEFINITIONS
            if item.index_unique_identifier == TIIE_28_INDEX_UNIQUE_IDENTIFIER
        )

        payload = definition.to_convention_payload(index_uid="fake-index-uid")

        self.assertEqual(payload["index_family"], "TIIE")
        self.assertEqual(payload["convention_dump"]["period"], "28D")
        self.assertEqual(payload["convention_dump"]["day_counter_code"], "Actual360")
        self.assertEqual(
            payload["convention_dump"]["business_day_convention"],
            "ModifiedFollowing",
        )

    def test_mxn_government_convention_payload_follows_ms_markets_curve_contract(self):
        definition = next(
            item
            for item in MEXICAN_INDEX_CONVENTION_DEFINITIONS
            if item.index_unique_identifier == MXN_GOVERNMENT_BOND_INDEX_UNIQUE_IDENTIFIER
        )

        payload = definition.to_convention_payload(index_uid="fake-index-uid")
        convention_dump = payload["convention_dump"]

        self.assertEqual(payload["index_family"], "MXN_GOVERNMENT_BOND")
        self.assertEqual(convention_dump["currency_code"], "MXN")
        self.assertEqual(convention_dump["fixing_calendar_code"], "Mexico-BMV")
        self.assertEqual(convention_dump["calendar_code"], "Mexico/BMV")
        self.assertEqual(convention_dump["day_counter_code"], "Actual360")
        self.assertEqual(convention_dump["settlement_days"], 0)
        self.assertEqual(convention_dump["coupon_period_days"], 182)
        self.assertEqual(convention_dump["date_generation_rule"], "Backward")
        self.assertFalse(convention_dump["end_of_month"])

    def test_valmer_curve_payload_links_to_tiie_index(self):
        payload = VALMER_TIIE_28_CURVE_DEFINITION.to_curve_payload(index_uid="fake-index-uid")

        self.assertEqual(payload["unique_identifier"], "VALMER_TIIE_28")
        self.assertEqual(payload["curve_type"], "discount")
        self.assertEqual(payload["source"], "valmer")
        self.assertEqual(payload["index_uid"], "fake-index-uid")

    def test_valmer_mxn_government_curve_payload_links_to_benchmark_index(self):
        payload = VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION.to_curve_payload(
            index_uid="fake-index-uid"
        )

        self.assertEqual(payload["unique_identifier"], "VALMER_MXN_GOVERNMENT_BOND")
        self.assertEqual(payload["curve_type"], "discount")
        self.assertEqual(payload["source"], "valmer")
        self.assertEqual(payload["index_uid"], "fake-index-uid")

    def test_curve_runtime_attach_includes_valmer_details_by_default(self):
        with (
            patch("msm.start_engine") as start_engine,
            patch("msm_pricing.bootstrap.create_pricing_schemas", return_value="pricing-runtime")
            as pricing_bootstrap,
        ):
            result = attach_valmer_curve_pricing_runtime(timeout=15)

        models = start_engine.call_args.kwargs["models"]
        self.assertIn(ValmerAssetDetailsTable, models)
        pricing_bootstrap.assert_called_once_with(timeout=15)
        self.assertEqual(result, "pricing-runtime")

    def test_curve_runtime_sets_discount_curve_storage_cadence(self):
        from msm_pricing.data_nodes import DiscountCurvesNode

        storage_table = DiscountCurvesNode._required_storage_table()
        original_cadence = getattr(storage_table, "__cadence__", None)
        try:
            storage_table.__cadence__ = None

            configured_storage = configure_valmer_discount_curves_cadence()

            self.assertIs(configured_storage, storage_table)
            self.assertEqual(storage_table.__cadence__, VALMER_DISCOUNT_CURVES_CADENCE)
            self.assertEqual(storage_table.__cadence__, "1d")
        finally:
            storage_table.__cadence__ = original_cadence

    def test_old_curve_schema_helper_forwards_to_runtime_attach(self):
        with patch(
            "valmer_connectors.instruments.curve_bootstrap.attach_valmer_curve_pricing_runtime",
            return_value="pricing-runtime",
        ) as attach_runtime:
            result = create_valmer_curve_pricing_schemas(timeout=15)

        attach_runtime.assert_called_once_with(markets_models=None, timeout=15)
        self.assertEqual(result, "pricing-runtime")

    def test_project_bootstrap_attaches_valmer_extension_tables(self):
        with (
            patch(
                "valmer_connectors.instruments.bootstrap.bootstrap_valmer_curve_pricing",
                return_value={"curves": {}},
            ) as core_bootstrap,
            patch(
                "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
                return_value="details-context",
            ) as details_bootstrap,
        ):
            result = seed_static_defaults(timeout=15, attach_runtime=True)

        core_bootstrap.assert_called_once_with(
            markets_models=[ValmerAssetDetailsTable, ValmerVectorPricesStorage],
            timeout=15,
            attach_runtime=True,
        )
        details_bootstrap.assert_called_once_with(timeout=15)
        self.assertEqual(result["valmer_asset_details_context"], "details-context")

    def test_project_bootstrap_does_not_directly_register_extension_tables(self):
        with (
            patch(
                "valmer_connectors.instruments.bootstrap.bootstrap_valmer_curve_pricing",
                return_value={"curves": {}},
            ),
            patch(
                "valmer_connectors.meta_tables.valmer_asset_details.ensure_valmer_asset_detail_runtime",
                return_value="details-context",
            ),
            patch.object(ValmerAssetDetailsTable, "register", Mock(side_effect=AssertionError)),
            patch.object(ValmerVectorPricesStorage, "register", Mock(side_effect=AssertionError)),
        ):
            result = seed_static_defaults(timeout=15, attach_runtime=True)

        self.assertEqual(result["valmer_asset_details_context"], "details-context")


if __name__ == "__main__":
    unittest.main()
