import unittest
from unittest.mock import Mock, patch

from msm.constants import INDEX_TYPE_INTEREST_RATE
from msm_pricing.instruments.json_codec import calendar_from_json

from valmer_connectors.data_nodes.valmer_vector_storage import ValmerVectorPricesStorage
from valmer_connectors.instruments.bootstrap import seed_static_defaults
from valmer_connectors.instruments.curve_bootstrap import (
    BANCO_DE_MEXICO_PROVIDER,
    CETE_28_INDEX_UNIQUE_IDENTIFIER,
    CETE_91_INDEX_UNIQUE_IDENTIFIER,
    CETE_182_INDEX_UNIQUE_IDENTIFIER,
    FEDERAL_RESERVE_BANK_OF_NEW_YORK_PROVIDER,
    MEXICAN_INDEX_CONVENTION_DEFINITIONS,
    MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    TIIE_91_INDEX_UNIQUE_IDENTIFIER,
    TIIE_182_INDEX_UNIQUE_IDENTIFIER,
    TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    USD_INDEX_CONVENTION_DEFINITIONS,
    USD_REFERENCE_INDEX_DEFINITIONS,
    USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
    VALMER_CURVE_BUILDING_DETAILS_DEFINITIONS,
    VALMER_CURVE_QUOTE_SIDE,
    VALMER_DISCOUNT_CURVES_CADENCE,
    VALMER_INDEX_CURVE_BINDING_DEFINITIONS,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION,
    VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
    VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION,
    VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION,
    VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
    attach_valmer_curve_pricing_runtime,
    configure_valmer_discount_curves_cadence,
    create_valmer_curve_pricing_schemas,
    mexican_reference_index_payloads,
    usd_reference_index_payloads,
    valmer_pricing_runtime_models,
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
            },
        )

    def test_usd_reference_index_definitions_cover_sofr(self):
        identifiers = {
            definition.unique_identifier for definition in USD_REFERENCE_INDEX_DEFINITIONS
        }

        self.assertEqual(identifiers, {"USD_SOFR_OVERNIGHT"})

    def test_reference_index_payloads_target_core_index_api(self):
        payload_by_identifier = {
            payload["unique_identifier"]: payload for payload in mexican_reference_index_payloads()
        }

        tiie_28 = payload_by_identifier["TIIE_28"]
        self.assertEqual(tiie_28["index_type"], INDEX_TYPE_INTEREST_RATE)
        self.assertEqual(tiie_28["provider"], BANCO_DE_MEXICO_PROVIDER)
        self.assertIsNone(tiie_28["metadata_json"])

        for identifier in (
            "TIIE_OVERNIGHT",
            "TIIE_28",
            "TIIE_91",
            "TIIE_182",
            "CETE_28",
            "CETE_91",
            "CETE_182",
        ):
            self.assertEqual(
                payload_by_identifier[identifier]["provider"],
                BANCO_DE_MEXICO_PROVIDER,
            )

    def test_usd_reference_index_payload_targets_core_index_api(self):
        payload_by_identifier = {
            payload["unique_identifier"]: payload for payload in usd_reference_index_payloads()
        }

        sofr = payload_by_identifier["USD_SOFR_OVERNIGHT"]
        self.assertEqual(sofr["index_type"], INDEX_TYPE_INTEREST_RATE)
        self.assertEqual(sofr["provider"], FEDERAL_RESERVE_BANK_OF_NEW_YORK_PROVIDER)
        self.assertEqual(sofr["metadata_json"], {"market": "US"})

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
            payload["convention_dump"]["fixing_calendar_code"],
            {"name": "Mexico"},
        )
        self.assertEqual(
            calendar_from_json(payload["convention_dump"]["fixing_calendar_code"]).name(),
            "Mexican stock exchange",
        )
        self.assertEqual(
            payload["convention_dump"]["business_day_convention"],
            "ModifiedFollowing",
        )

    def test_no_mxn_government_bond_index_convention_is_seeded(self):
        identifiers = {
            item.index_unique_identifier for item in MEXICAN_INDEX_CONVENTION_DEFINITIONS
        }

        self.assertNotIn("MXN_GOVERNMENT_BOND", identifiers)

    def test_usd_sofr_index_convention_payload_records_fixing_selector(self):
        definition = next(
            item
            for item in USD_INDEX_CONVENTION_DEFINITIONS
            if item.index_unique_identifier == USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER
        )

        payload = definition.to_convention_payload(index_uid="fake-sofr-index-uid")

        self.assertEqual(payload["index_family"], "SOFR")
        self.assertEqual(payload["source"], "united_states")
        self.assertEqual(payload["convention_dump"]["currency_code"], "USD")
        self.assertEqual(payload["convention_dump"]["period"], "1D")
        self.assertEqual(payload["convention_dump"]["settlement_days"], 0)
        self.assertEqual(payload["convention_dump"]["day_counter_code"], "Actual360")
        self.assertEqual(
            payload["convention_dump"]["business_day_convention"],
            "ModifiedFollowing",
        )
        self.assertEqual(
            payload["convention_dump"]["fixings_unique_identifier"],
            "USD_SOFR_OVERNIGHT",
        )

    def test_valmer_curve_payload_is_curve_identity_without_index_uid(self):
        payload = VALMER_TIIE_OVERNIGHT_CURVE_DEFINITION.to_curve_payload()

        self.assertEqual(payload["unique_identifier"], "VALMER_TIIE_OVERNIGHT")
        self.assertEqual(payload["display_name"], "Valmer TIIE overnight OIS curve")
        self.assertEqual(payload["curve_type"], "projection")
        self.assertEqual(payload["currency_code"], "MXN")
        self.assertEqual(payload["quote_side"], VALMER_CURVE_QUOTE_SIDE)
        self.assertEqual(payload["source"], "valmer")
        self.assertEqual(payload["metadata_json"]["source_file"], "IRS_MXN_CURVE.csv")
        self.assertEqual(
            payload["metadata_json"]["source_url"],
            "https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_MXN_CURVE.csv",
        )
        self.assertEqual(
            payload["metadata_json"]["included_source_family"],
            "Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO",
        )
        self.assertNotIn("index_uid", payload)

    def test_valmer_mxn_government_curve_payload_is_curve_identity_without_index_uid(self):
        payload = VALMER_MXN_GOVERNMENT_BOND_CURVE_DEFINITION.to_curve_payload()

        self.assertEqual(payload["unique_identifier"], "VALMER_MXN_GOVERNMENT_BOND")
        self.assertEqual(payload["curve_type"], "discount")
        self.assertEqual(payload["currency_code"], "MXN")
        self.assertEqual(payload["quote_side"], VALMER_CURVE_QUOTE_SIDE)
        self.assertEqual(payload["source"], "valmer")
        self.assertNotIn("index_uid", payload)

    def test_valmer_usd_sofr_curve_payload_is_curve_identity_without_index_uid(self):
        payload = VALMER_USD_SOFR_OVERNIGHT_CURVE_DEFINITION.to_curve_payload()

        self.assertEqual(payload["unique_identifier"], "VALMER_USD_SOFR_OVERNIGHT")
        self.assertEqual(payload["display_name"], "Valmer USD SOFR overnight OIS curve")
        self.assertEqual(payload["curve_type"], "projection")
        self.assertEqual(payload["currency_code"], "USD")
        self.assertEqual(payload["quote_side"], VALMER_CURVE_QUOTE_SIDE)
        self.assertEqual(payload["source"], "valmer")
        self.assertEqual(payload["metadata_json"]["source_file"], "IRS_USD_CURVE.csv")
        self.assertEqual(
            payload["metadata_json"]["source_url"],
            "https://www.valmer.com.mx/VAL/Web_Benchmarks/IRS_USD_CURVE.csv",
        )
        self.assertIn(
            "Future.USD.CME.CME SR3 IMM.<MMM>.<YY>",
            payload["metadata_json"]["included_source_families"],
        )
        self.assertIn(
            "Swap.<tenor>.USD.FEDFUNDS.1D/1Y.FEDFUNDS1",
            payload["metadata_json"]["excluded_source_families"],
        )
        self.assertNotIn("index_uid", payload)

    def test_curve_building_details_cover_all_valmer_curves(self):
        definitions = {
            definition.curve_unique_identifier: definition
            for definition in VALMER_CURVE_BUILDING_DETAILS_DEFINITIONS
        }

        self.assertEqual(
            set(definitions),
            {
                VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
                VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
                VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
            },
        )
        tiie_payload = definitions[
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER
        ].to_building_details_payload(curve_uid="fake-tiie-curve-uid")
        self.assertEqual(tiie_payload["curve_uid"], "fake-tiie-curve-uid")
        self.assertEqual(tiie_payload["builder_type"], "rate_helper_curve")
        self.assertEqual(tiie_payload["quote_convention"], "helper_quote")
        self.assertEqual(tiie_payload["rate_unit"], "helper_unit")
        self.assertEqual(
            tiie_payload["bootstrap_method"],
            "piecewise_log_linear_discount",
        )
        self.assertEqual(tiie_payload["builder_payload"]["helper_schema"], "rate_helpers@v1")
        self.assertEqual(
            tiie_payload["builder_payload"]["output_quote_convention"],
            "zero_rate",
        )
        self.assertEqual(tiie_payload["builder_payload"]["output_rate_unit"], "decimal")
        self.assertEqual(
            tiie_payload["builder_payload"]["source_row_pattern"],
            "Swap.<tenor>.MXN.FTIIE.1D/28D.BANXICO",
        )
        self.assertEqual(tiie_payload["builder_payload"]["implied_front_zero_days"], [1])
        self.assertEqual(
            tiie_payload["builder_payload"]["instrument_rules"]["FTIIE_OIS"][
                "helper_type"
            ],
            "ois_rate_helper",
        )
        self.assertEqual(
            tiie_payload["builder_payload"]["instrument_rules"]["FTIIE_OIS"][
                "settlement_days"
            ],
            1,
        )

        sofr_payload = definitions[
            VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER
        ].to_building_details_payload(curve_uid="fake-sofr-curve-uid")
        self.assertEqual(sofr_payload["curve_uid"], "fake-sofr-curve-uid")
        self.assertEqual(sofr_payload["builder_type"], "rate_helper_curve")
        self.assertEqual(sofr_payload["quote_convention"], "helper_quote")
        self.assertEqual(sofr_payload["rate_unit"], "helper_unit")
        self.assertEqual(sofr_payload["calendar_code"], "UnitedStates")
        self.assertEqual(
            sofr_payload["bootstrap_method"],
            "piecewise_log_linear_discount",
        )
        self.assertEqual(sofr_payload["builder_payload"]["helper_schema"], "rate_helpers@v1")
        self.assertEqual(
            sofr_payload["builder_payload"]["output_quote_convention"],
            "zero_rate",
        )
        self.assertEqual(sofr_payload["builder_payload"]["output_rate_unit"], "decimal")
        self.assertIn(
            "Future.USD.CME.CME SR1 EOM.<MMM>.<YY>",
            sofr_payload["builder_payload"]["source_row_patterns"],
        )
        self.assertIn(
            "Swap.USD.<tenor>.FEDFUNDS.1D/SOFR.1D.SOFR",
            sofr_payload["builder_payload"]["excluded_source_row_patterns"],
        )
        self.assertEqual(
            sofr_payload["builder_payload"]["active_future_policy"],
            "exclude_futures_before_valuation_date",
        )
        self.assertEqual(
            sofr_payload["builder_payload"]["instrument_rules"]["SOFR_FUTURE"][
                "helper_type"
            ],
            "sofr_future_rate_helper",
        )
        self.assertEqual(
            sofr_payload["builder_payload"]["instrument_rules"]["SOFR_FUTURE"][
                "future_family"
            ],
            "sofr",
        )
        self.assertEqual(
            sofr_payload["builder_payload"]["instrument_rules"]["SOFR_OIS"][
                "floating_index"
            ],
            USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            sofr_payload["builder_payload"]["instrument_rules"]["SOFR_OIS"][
                "settlement_days"
            ],
            2,
        )

        payload = definitions[
            VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER
        ].to_building_details_payload(curve_uid="fake-curve-uid")
        self.assertEqual(payload["curve_uid"], "fake-curve-uid")
        self.assertEqual(payload["builder_type"], "bond_helper_bootstrap")
        self.assertEqual(payload["quote_convention"], "key_node_quote")
        self.assertEqual(payload["rate_unit"], "key_node_unit")
        self.assertEqual(
            payload["bootstrap_method"],
            "quantlib_piecewise_log_linear_discount",
        )
        self.assertEqual(payload["calendar_code"], "Mexico")
        self.assertEqual(payload["interpolation_method"], "log_linear_discount")
        self.assertEqual(payload["compounding"], "compounded_annual")
        self.assertEqual(payload["builder_payload"]["key_node_schema"], "CurveKeyNode")
        self.assertIn("quote_source", payload["builder_payload"]["valmer_extensions"])
        self.assertIn("dirty_price", payload["builder_payload"]["valmer_extensions"])
        self.assertEqual(
            payload["builder_payload"]["instrument_rules"]["CETES"],
            {
                "instrument_type": "zero_coupon_bond",
                "helper_type": "zero_coupon_bond_helper",
                "quote_type": "clean_price",
                "quote_unit": "price_per_10",
                "source_quote_type": "dirty_price",
                "yield_type": "yield_to_maturity",
                "yield_unit": "decimal",
            },
        )
        self.assertEqual(
            payload["builder_payload"]["instrument_rules"]["M_BONOS"]["quote_type"],
            "clean_price",
        )

    def test_curve_binding_definitions_use_real_index_selectors_and_mid_side(self):
        bindings = {
            (definition.role_key, definition.index_unique_identifier): definition
            for definition in VALMER_INDEX_CURVE_BINDING_DEFINITIONS
        }

        self.assertEqual(
            bindings[
                ("projection", TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER)
            ].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("projection", TIIE_28_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("projection", TIIE_182_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[
                ("z_spread_base", TIIE_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER)
            ].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("z_spread_base", TIIE_28_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("z_spread_base", TIIE_91_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("z_spread_base", TIIE_182_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("z_spread_base", CETE_28_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("z_spread_base", CETE_182_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("z_spread_base", CETE_91_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_MXN_GOVERNMENT_BOND_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[("projection", TIIE_91_INDEX_UNIQUE_IDENTIFIER)].curve_unique_identifier,
            VALMER_TIIE_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertEqual(
            bindings[
                ("projection", USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER)
            ].curve_unique_identifier,
            VALMER_USD_SOFR_OVERNIGHT_CURVE_UNIQUE_IDENTIFIER,
        )
        self.assertNotIn(
            ("discount", USD_SOFR_OVERNIGHT_INDEX_UNIQUE_IDENTIFIER),
            bindings,
        )
        self.assertNotIn(
            "VALMER_TIIE_28",
            {definition.curve_unique_identifier for definition in VALMER_CURVE_BUILDING_DETAILS_DEFINITIONS},
        )
        self.assertNotIn(
            "VALMER_TIIE_28",
            {definition.curve_unique_identifier for definition in VALMER_INDEX_CURVE_BINDING_DEFINITIONS},
        )
        for definition in VALMER_INDEX_CURVE_BINDING_DEFINITIONS:
            self.assertEqual(definition.quote_side, "mid")

        payload = bindings[
            ("z_spread_base", CETE_28_INDEX_UNIQUE_IDENTIFIER)
        ].to_index_curve_selection_payload(
            market_data_set_uid="market-data-set-uid",
            index_uid="index-uid",
            curve_uid="curve-uid",
        )
        self.assertEqual(payload["role_key"], "z_spread_base")
        self.assertEqual(payload["quote_side"], "mid")
        self.assertEqual(payload["index_uid"], "index-uid")
        self.assertEqual(payload["curve_uid"], "curve-uid")

    def test_curve_runtime_attach_includes_valmer_details_by_default(self):
        from msm_pricing.data_nodes.curves.storage import DiscountCurvesStorage
        from msm_pricing.data_nodes.index_fixings.storage import IndexFixingsStorage
        from msm_pricing.models.curve_building_details import CurveBuildingDetailsTable
        from msm_pricing.models.market_data_bindings import (
            PricingMarketDataSetBindingTable,
            PricingMarketDataSetCurveBindingTable,
            PricingMarketDataSetTable,
        )

        with (
            patch("msm.start_engine") as start_engine,
            patch("msm_pricing.bootstrap.attach_pricing_schemas", return_value="pricing-runtime")
            as pricing_bootstrap,
        ):
            result = attach_valmer_curve_pricing_runtime(timeout=15)

        models = start_engine.call_args.kwargs["models"]
        self.assertIn(ValmerAssetDetailsTable, models)
        pricing_bootstrap.assert_called_once_with(
            models=valmer_pricing_runtime_models(),
            timeout=15,
        )
        pricing_models = pricing_bootstrap.call_args.kwargs["models"]
        self.assertIn(CurveBuildingDetailsTable, pricing_models)
        self.assertIn(PricingMarketDataSetTable, pricing_models)
        self.assertIn(PricingMarketDataSetBindingTable, pricing_models)
        self.assertIn(PricingMarketDataSetCurveBindingTable, pricing_models)
        self.assertIn(DiscountCurvesStorage, pricing_models)
        self.assertIn(IndexFixingsStorage, pricing_models)
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
