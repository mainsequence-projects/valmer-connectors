import unittest

from msm.constants import INDEX_TYPE_INTEREST_RATE

from src.instruments.curve_bootstrap import (
    MEXICAN_INDEX_CONVENTION_DEFINITIONS,
    MEXICAN_REFERENCE_INDEX_DEFINITIONS,
    TIIE_28_INDEX_UNIQUE_IDENTIFIER,
    VALMER_TIIE_28_CURVE_DEFINITION,
    mexican_reference_index_payloads,
)


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

    def test_valmer_curve_payload_links_to_tiie_index(self):
        payload = VALMER_TIIE_28_CURVE_DEFINITION.to_curve_payload(index_uid="fake-index-uid")

        self.assertEqual(payload["unique_identifier"], "VALMER_TIIE_28")
        self.assertEqual(payload["curve_type"], "discount")
        self.assertEqual(payload["source"], "valmer")
        self.assertEqual(payload["index_uid"], "fake-index-uid")


if __name__ == "__main__":
    unittest.main()
